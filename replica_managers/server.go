package main

import (
	proto "ReplicationService/grpc"
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"google.golang.org/grpc"
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

func main() {
	// Parse command line flag for port
	port := flag.String("port", "50051", "Server port number")
	flag.Parse()

	addr := ":" + *port

	// Create TCP listener on specified port
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Server STARTUP_ERROR: failed to listen on %s: %v", addr, err)
	}

	// Create server instance
	grpcServer := grpc.NewServer()

	// Register our service implementation with the gRPC server
	proto.RegisterReplicationServiceServer(grpcServer, &ReplicationServiceServer{
		port: *port,
	})

	log.Printf("Server STARTUP: listening on %s", addr)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Printf("Server [%s] SHUTDOWN: Received shutdown signal", *port)
		grpcServer.GracefulStop()
	}()

	// Run server
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Server ERROR: %v", err)
	}
}
