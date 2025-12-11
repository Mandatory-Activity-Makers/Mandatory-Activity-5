package main

import (
	proto "ReplicationService/grpc"
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ReplicationServiceServer struct {
	proto.UnimplementedReplicationServiceServer

	port  string
	mutex sync.RWMutex

	// Auction state
	auctionEnd        time.Time
	isAuctionActive   bool
	highest_bid       int64
	highest_bidder_id string

	// Replication state
	peerAddresses []string
	peerClients   []proto.ReplicationServiceClient

	// Consensus state
	logicalClock   int64                          //Lamport clock
	proposals      map[string]map[int64]*Proposal // replicaID -> timestamp -> proposal
	votes          map[int64]map[string]bool      // timestamp -> replicaID -> vote
	decisions      map[int64]bool                 // timestamp -> final decision
	executionOrder []ConsensusOperation           // Global execution order
	executedUpTo   int64
}

type Proposal struct {
	Amount    int64
	BidderID  string
	Timestamp int64
	ReplicaID string
}

type ConsensusOperation struct {
	Timestamp int64
	ReplicaID string
	Amount    int64
	BidderID  string
	Accepted  bool
}

func NewReplicationServiceServer(port string, peerAddresses []string) *ReplicationServiceServer {
	return &ReplicationServiceServer{
		port:              port,
		peerAddresses:     peerAddresses,
		auctionEnd:        time.Now().Add(100 * time.Second),
		isAuctionActive:   true,
		highest_bid:       0,
		highest_bidder_id: "",

		logicalClock:   0,
		proposals:      make(map[string]map[int64]*Proposal),
		votes:          make(map[int64]map[string]bool),
		decisions:      make(map[int64]bool),
		executionOrder: make([]ConsensusOperation, 0),
		executedUpTo:   -1,
	}
}

func (s *ReplicationServiceServer) InitializeAuction(duration time.Duration) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.highest_bid = 0
	s.highest_bidder_id = ""
	s.isAuctionActive = true
	s.auctionEnd = time.Now().Add(duration)

	go s.startAuctionTimer(duration)
	log.Printf("Server [%s] Auction started! Will end at: %v", s.port, s.auctionEnd)
}

func (s *ReplicationServiceServer) startAuctionTimer(duration time.Duration) {
	timer := time.NewTimer(duration)
	<-timer.C

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.isAuctionActive = false
	s.executeAllPending() // Execute any remaining consensus operations

	log.Printf("Auction ended on server [%s]! Winner: %s with bid: %d",
		s.port, s.highest_bidder_id, s.highest_bid)
}

func (s *ReplicationServiceServer) Bid(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	clientBid := req.GetAmount()
	clientID := req.GetId()

	log.Printf("Server [%s] BID: Received bid of %d from client %s", s.port, clientBid, clientID)

	// Quick local check
	s.mutex.RLock()
	auctionActive := s.isAuctionActive
	currentHighest := s.highest_bid
	s.mutex.RUnlock()

	if !auctionActive {
		log.Printf("Server [%s] BID: Auction has ended!", s.port)
		return &proto.BidResponse{Ack: false}, nil
	}

	if clientBid <= currentHighest {
		log.Printf("Server [%s] BID: Rejected bid %d (not higher than current: %d)",
			s.port, clientBid, currentHighest)
		return &proto.BidResponse{Ack: false}, nil
	}

	// Consensus phase 1: propose
	s.mutex.Lock()
	s.logicalClock++
	proposalTimestamp := s.logicalClock

	// Create and store proposal
	proposal := &Proposal{
		Amount:    clientBid,
		BidderID:  clientID,
		Timestamp: proposalTimestamp,
		ReplicaID: s.port,
	}

	// Initialize proposal map for this replica if needed
	if s.proposals[s.port] == nil {
		s.proposals[s.port] = make(map[int64]*Proposal)
	}
	s.proposals[s.port][proposalTimestamp] = proposal

	// Initialize votes for this timestamp
	s.votes[proposalTimestamp] = make(map[string]bool)
	s.votes[proposalTimestamp][s.port] = true // Vote YES for own proposal
	s.mutex.Unlock()

	log.Printf("Server [%s] BID: Created proposal with timestamp %d", s.port, proposalTimestamp)

	// Broadcast proposal to all peers
	voteChan := make(chan *proto.ProposeResponse, len(s.peerClients))

	for i, peerClient := range s.peerClients {
		go func(peerIdx int, client proto.ReplicationServiceClient) {
			proposeReq := &proto.ProposeRequest{
				Amount:    clientBid,
				Id:        clientID,
				Timestamp: proposalTimestamp,
				ReplicaId: s.port,
			}

			proposeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := client.Propose(proposeCtx, proposeReq)
			cancel()

			if err != nil {
				log.Printf("Server [%s] BID: Peer %d proposal failed: %v", s.port, peerIdx, err)
				voteChan <- nil
				return
			}

			voteChan <- resp
		}(i, peerClient)
	}

	// Collect votes
	timeout := time.After(10 * time.Second)
	yesVotes := 1 // Start with own vote
	totalVotes := 1

	for i := 0; i < len(s.peerClients); i++ {
		select {
		case resp := <-voteChan:
			if resp != nil {
				s.mutex.Lock()
				// Update logical clock
				if resp.GetTimestamp() > s.logicalClock {
					s.logicalClock = resp.GetTimestamp()
				}
				s.logicalClock++

				// Record vote
				s.votes[proposalTimestamp][resp.GetReplicaId()] = resp.GetAccept()

				if resp.GetAccept() {
					yesVotes++
				}
				totalVotes++
				s.mutex.Unlock()
			}
		case <-timeout:
			log.Printf("Server [%s] BID: Timeout collecting votes for timestamp %d",
				s.port, proposalTimestamp)
		}
	}

	// Consensus phase 2: decide
	quorumSize := (len(s.peerAddresses) / 2) + 1
	decision := yesVotes >= quorumSize

	log.Printf("Server [%s] BID: Consensus reached: yes=%d, total=%d, quorum=%d, decision=%v",
		s.port, yesVotes, totalVotes, quorumSize, decision)

	// Broadcast decision to ALL replicas (including self)
	decisionChan := make(chan bool, len(s.peerClients)+1)

	// Apply decision locally first
	go func() {
		s.mutex.Lock()
		s.decisions[proposalTimestamp] = decision

		// Add to global execution order with deterministic ordering
		op := ConsensusOperation{
			Timestamp: proposalTimestamp,
			ReplicaID: s.port,
			Amount:    clientBid,
			BidderID:  clientID,
			Accepted:  decision,
		}
		s.executionOrder = append(s.executionOrder, op)

		// Sort execution order by (timestamp, replicaID) for deterministic ordering
		sort.Slice(s.executionOrder, func(i, j int) bool {
			if s.executionOrder[i].Timestamp != s.executionOrder[j].Timestamp {
				return s.executionOrder[i].Timestamp < s.executionOrder[j].Timestamp
			}
			return s.executionOrder[i].ReplicaID < s.executionOrder[j].ReplicaID
		})

		s.executeOrderedOperations()
		s.mutex.Unlock()

		decisionChan <- true
	}()

	// Broadcast to peers
	for i, peerClient := range s.peerClients {
		go func(peerIdx int, client proto.ReplicationServiceClient) {
			decideReq := &proto.DecideRequest{
				Timestamp: proposalTimestamp,
				Accepted:  decision,
				ReplicaId: s.port,
				Amount:    clientBid,
				Id:        clientID,
			}

			decideCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := client.Decide(decideCtx, decideReq)
			cancel()

			if err != nil {
				log.Printf("Server [%s] BID: Failed to send decision to peer %d: %v",
					s.port, peerIdx, err)
				decisionChan <- false
			} else {
				decisionChan <- true
			}
		}(i, peerClient)
	}

	// Wait for decision acknowledgments
	successfulDecisions := 0
	for i := 0; i < len(s.peerClients)+1; i++ {
		if <-decisionChan {
			successfulDecisions++
		}
	}

	log.Printf("Server [%s] BID: Decision broadcast complete: %d/%d successful",
		s.port, successfulDecisions, len(s.peerClients)+1)

	return &proto.BidResponse{Ack: decision}, nil
}

func (s *ReplicationServiceServer) Propose(ctx context.Context, req *proto.ProposeRequest) (*proto.ProposeResponse, error) {
	amount := req.GetAmount()
	bidderID := req.GetId()
	timestamp := req.GetTimestamp()
	replicaID := req.GetReplicaId()

	log.Printf("Server [%s] PROPOSE: Received proposal from %s: amount=%d, timestamp=%d",
		s.port, replicaID, amount, timestamp)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Update logical clock
	if timestamp > s.logicalClock {
		s.logicalClock = timestamp
	}
	s.logicalClock++

	// Store proposal
	if s.proposals[replicaID] == nil {
		s.proposals[replicaID] = make(map[int64]*Proposal)
	}

	s.proposals[replicaID][timestamp] = &Proposal{
		Amount:    amount,
		BidderID:  bidderID,
		Timestamp: timestamp,
		ReplicaID: replicaID,
	}

	// Initialize votes for this timestamp if needed
	if s.votes[timestamp] == nil {
		s.votes[timestamp] = make(map[string]bool)
	}

	// Decide whether to accept this proposal
	accept := false
	if s.isAuctionActive && amount > s.highest_bid {
		accept = true
	}

	// Record our vote
	s.votes[timestamp][s.port] = accept

	log.Printf("Server [%s] PROPOSE: Voting %v for proposal from %s (ts=%d)",
		s.port, accept, replicaID, timestamp)

	return &proto.ProposeResponse{
		Accept:    accept,
		Timestamp: s.logicalClock,
		ReplicaId: s.port,
	}, nil
}

func (s *ReplicationServiceServer) Decide(ctx context.Context, req *proto.DecideRequest) (*proto.DecideResponse, error) {
	timestamp := req.GetTimestamp()
	accepted := req.GetAccepted()
	replicaID := req.GetReplicaId()
	amount := req.GetAmount()
	bidderID := req.GetId()

	log.Printf("Server [%s] DECIDE: Received decision from %s for timestamp %d: accepted=%v",
		s.port, replicaID, timestamp, accepted)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Store the final decision
	s.decisions[timestamp] = accepted

	// Ensure that the proposal is stored
	if s.proposals[replicaID] == nil || s.proposals[replicaID][timestamp] == nil {
		// Store it if we don't have it
		if s.proposals[replicaID] == nil {
			s.proposals[replicaID] = make(map[int64]*Proposal)
		}
		s.proposals[replicaID][timestamp] = &Proposal{
			Amount:    amount,
			BidderID:  bidderID,
			Timestamp: timestamp,
			ReplicaID: replicaID,
		}
	}

	// Add to global execution order
	op := ConsensusOperation{
		Timestamp: timestamp,
		ReplicaID: replicaID,
		Amount:    amount,
		BidderID:  bidderID,
		Accepted:  accepted,
	}

	// Check if already in execution order
	alreadyExists := false
	for _, existingOp := range s.executionOrder {
		if existingOp.Timestamp == timestamp && existingOp.ReplicaID == replicaID {
			alreadyExists = true
			break
		}
	}

	if !alreadyExists {
		s.executionOrder = append(s.executionOrder, op)

		// Sort by (timestamp, replicaID) for deterministic ordering
		sort.Slice(s.executionOrder, func(i, j int) bool {
			if s.executionOrder[i].Timestamp != s.executionOrder[j].Timestamp {
				return s.executionOrder[i].Timestamp < s.executionOrder[j].Timestamp
			}
			return s.executionOrder[i].ReplicaID < s.executionOrder[j].ReplicaID
		})
	}

	// Execute operations
	s.executeOrderedOperations()

	log.Printf("Server [%s] DECIDE: Applied decision for timestamp %d from %s",
		s.port, timestamp, replicaID)

	return &proto.DecideResponse{Ack: true}, nil
}

func (s *ReplicationServiceServer) executeOrderedOperations() {
	// Execute in deterministic order
	for i := int64(0); i < int64(len(s.executionOrder)); i++ {
		if i <= s.executedUpTo {
			continue // Already executed
		}

		op := s.executionOrder[i]

		// Check if it has a decision for this operation
		decision, hasDecision := s.decisions[op.Timestamp]
		if !hasDecision {
			// Don't execute operations without decisions
			break
		}

		// Execute if accepted
		if decision && op.Accepted && s.isAuctionActive {
			if op.Amount > s.highest_bid {
				s.highest_bid = op.Amount
				s.highest_bidder_id = op.BidderID
				log.Printf("Server [%s] EXECUTE: Applied bid %d from %s (ts=%d, replica=%s)",
					s.port, op.Amount, op.BidderID, op.Timestamp, op.ReplicaID)
			}
		}

		s.executedUpTo = i
	}
}

func (s *ReplicationServiceServer) executeAllPending() {
	// Execute any remaining operations :)))))))
	s.executeOrderedOperations()
}

func (s *ReplicationServiceServer) Result(ctx context.Context, _ *proto.Empty) (*proto.ResultResponse, error) {
	s.mutex.Lock()
	s.executeOrderedOperations() // Ensure we're up to date
	result := s.highest_bid
	bidder := s.highest_bidder_id
	s.mutex.Unlock()

	log.Printf("Server [%s] RESULT: Returning highest bid: %d by client %s",
		s.port, result, bidder)

	return &proto.ResultResponse{
		Result:          result,
		HighestBidderId: bidder,
	}, nil
}

func (s *ReplicationServiceServer) ConnectToPeers() {
	s.peerClients = make([]proto.ReplicationServiceClient, 0)

	for _, peerAddr := range s.peerAddresses {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := grpc.DialContext(ctx, peerAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		cancel()

		if err != nil {
			log.Printf("Server [%s] SETUP: Failed to connect to peer %s: %v", s.port, peerAddr, err)
			continue
		}

		client := proto.NewReplicationServiceClient(conn)
		s.peerClients = append(s.peerClients, client)
	}

	log.Printf("Server [%s] SETUP: Connected to %d/%d peers", s.port, len(s.peerClients), len(s.peerAddresses))
}

func main() {
	port := flag.String("port", "50051", "Server port number")
	peers := flag.String("peers", "", "Comma-separated peer addresses")
	flag.Parse()

	addr := ":" + *port

	var peerAddresses []string
	if *peers != "" {
		peerAddresses = strings.Split(*peers, ",")
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Server STARTUP_ERROR: failed to listen on %s: %v", addr, err)
	}

	server := NewReplicationServiceServer(*port, peerAddresses)

	// Start Auction
	server.InitializeAuction(100 * time.Second)

	grpcServer := grpc.NewServer()
	proto.RegisterReplicationServiceServer(grpcServer, server)

	log.Printf("Server STARTUP: listening on %s", addr)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Server ERROR: %v", err)
		}
	}()

	time.Sleep(time.Second)
	server.ConnectToPeers()
	log.Printf("Server [%s] READY: Server is ready to accept requests", *port)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	log.Printf("Server [%s] SHUTDOWN: Received shutdown signal", *port)
	grpcServer.GracefulStop()
}
