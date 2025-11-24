package main

import (
	proto "ReplicationService/grpc"
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ReplicationServiceServer struct {
	proto.UnimplementedReplicationServiceServer

	port              string
	mutex             sync.Mutex
	timestamp         int64
	highest_bid       int64
	highest_bidder_id int64

	peerAddresses []string                         // e.g., ["localhost:50052", "localhost:50053"]
	peerClients   []proto.ReplicationServiceClient // Connections to other servers
}

func (s *ReplicationServiceServer) Bid(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	// Phase 1: Local validation
	s.mutex.Lock()
	if req.Amount <= s.highest_bid {
		s.mutex.Unlock()
		return &proto.BidResponse{Ack: false}, nil
	}
	s.mutex.Unlock()

	// Phase 2: Replicate to other servers
	ackCount := 1 // Count self as acknowledged

	// TODO: For each other server:
	//   - Call server.Replicate(req)
	//   - If it responds positively, increment ackCount
	//   - Handle errors (server might be down!)

	// Phase 3: Check quorum
	requiredAcks := 2 // For 3 servers with F=1 tolerance
	if ackCount < requiredAcks {
		return &proto.BidResponse{Ack: false}, nil // Didn't get enough replicas
	}

	// Phase 4: Update local state (now it's safe!)
	s.mutex.Lock()
	if req.Amount > s.highest_bid {
		s.highest_bid = req.Amount
		s.highest_bidder_id = req.Id
	}
	s.mutex.Unlock()

	return &proto.BidResponse{Ack: true}, nil
}

func (s *ReplicationServiceServer) Result(ctx context.Context, _ *proto.Empty) (*proto.ResultResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	log.Printf("Server [%s] RESULT: Returning highest bid: %d by client %d", s.port, s.highest_bid, s.highest_bidder_id)

	return &proto.ResultResponse{
		Result:          s.highest_bid,
		HighestBidderId: s.highest_bidder_id,
	}, nil
}

func (s *ReplicationServiceServer) Replicate(ctx context.Context, req *proto.ReplicateRequest) (*proto.ReplicateResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	bidAmount := req.GetAmount()
	bidderID := req.GetId()

	log.Printf("Server [%s] REPLICATE: Received replication of bid %d from client %d",
		s.port, bidAmount, bidderID)

	// Check if this bid is higher than current highest
	if bidAmount > s.highest_bid {
		// Accept and update
		s.highest_bid = bidAmount
		s.highest_bidder_id = bidderID

		log.Printf("Server [%s] REPLICATE: Accepted and updated to bid %d from client %d",
			s.port, bidAmount, bidderID)

		return &proto.ReplicateResponse{Ack: true, CurrentHighest: bidAmount}, nil
	} else {
		// Reject - not higher than current
		log.Printf("Server [%s] REPLICATE: Rejected bid %d (current highest: %d)",
			s.port, bidAmount, s.highest_bid)

		return &proto.ReplicateResponse{Ack: false, CurrentHighest: s.highest_bid}, nil
	}
}

// Call this after starting the server
func (s *ReplicationServiceServer) ConnectToPeers() {
	s.peerClients = make([]proto.ReplicationServiceClient, 0)

	for _, peerAddr := range s.peerAddresses {
		log.Printf("Server [%s] SETUP: Attempting to connect to peer %s", s.port, peerAddr)

		// Create connection with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := grpc.DialContext(ctx, peerAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		cancel()

		if err != nil {
			log.Printf("Server [%s] SETUP: Failed to connect to peer %s: %v", s.port, peerAddr, err)
			// Don't add to peerClients if connection fails
			continue
		}

		client := proto.NewReplicationServiceClient(conn)
		s.peerClients = append(s.peerClients, client)
		log.Printf("Server [%s] SETUP: Successfully connected to peer %s", s.port, peerAddr)
	}

	log.Printf("Server [%s] SETUP: Connected to %d/%d peers", s.port, len(s.peerClients), len(s.peerAddresses))
}

func main() {
	// Parse command line flags
	port := flag.String("port", "50051", "Server port number")
	peers := flag.String("peers", "", "Comma-separated peer addresses (e.g., localhost:50052,localhost:50053)")
	flag.Parse()

	addr := ":" + *port

	// Parse peer addresses
	var peerAddresses []string
	if *peers != "" {
		peerAddresses = strings.Split(*peers, ",")
	}

	// Create TCP listener on specified port
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Server STARTUP_ERROR: failed to listen on %s: %v", addr, err)
	}

	// Create server instance
	server := &ReplicationServiceServer{
		port:          *port,
		peerAddresses: peerAddresses,
	}

	// Create and register gRPC server
	grpcServer := grpc.NewServer()
	proto.RegisterReplicationServiceServer(grpcServer, server)

	log.Printf("Server STARTUP: listening on %s", addr)

	// Start gRPC server in a goroutine
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Server ERROR: %v", err)
		}
	}()

	// Give the server a moment to start before connecting to peers
	time.Sleep(time.Second)

	// Connect to peer servers
	server.ConnectToPeers()

	log.Printf("Server [%s] READY: Server is ready to accept requests", *port)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	log.Printf("Server [%s] SHUTDOWN: Received shutdown signal", *port)
	grpcServer.GracefulStop()
}
