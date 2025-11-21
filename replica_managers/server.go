package main

import (
	proto "ReplicationService/grpc"
	"context"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
)

type ReplicationServiceServer struct {
	proto.UnimplementedReplicationServiceServer

	port              string // localhost
	mutex             sync.Mutex
	timestamp         int64 // Lamport clock
	highest_bid       int64 // current highest bid
	highest_bidder_id int64 // current highest bidder id
}

func (s *ReplicationServiceServer) Bid(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	ClientBid := req.GetAmount()
	ClientID := req.GetId()

	s.mutex.Lock()
	if ClientBid > s.highest_bid {
		s.highest_bid = ClientBid
		s.highest_bidder_id = ClientID
		return &proto.BidResponse{Ack: true}, nil
	}
	s.mutex.Unlock()

	return &proto.BidResponse{Ack: false}, nil
}

func (s *ReplicationServiceServer) Result(ctx context.Context, _ *proto.Empty) (*proto.ResultResponse, error) {
	return &proto.ResultResponse{
		Result:          s.highest_bid,
		HighestBidderId: s.highest_bidder_id,
	}, nil
}

func main() {
	addr := ":50051"

	//Create TCP listener on specified port
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Server STARTUP_ERROR: failed to listen on %s: %v", addr, err)
	}
	//Creates server instance
	grpcServer := grpc.NewServer()
	//Register our service implementation with the gRPC server
	proto.RegisterReplicationServiceServer(grpcServer, &ReplicationServiceServer{})

	log.Printf("Server STARTUP: listening on %s", addr)

	// run server
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Server ERROR: %v", err)
		}
	}()
	//Block main goroutine -keep server running :)
	select {}
}

// replicateToManagers(){}

// handleReplication
