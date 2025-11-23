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
	"time"

	"google.golang.org/grpc"
)

type ReplicationServiceServer struct {
	proto.UnimplementedReplicationServiceServer

	port  string
	mutex sync.Mutex

	auctionEnd      time.Time
	isAuctionActive bool

	highest_bid       int64
	highest_bidder_id int64
}

func NewAuctionService() *ReplicationServiceServer {
	duration := 10 * time.Second //Duration of the auction

	s := &ReplicationServiceServer{
		highest_bid:     0,
		isAuctionActive: true,
		auctionEnd:      time.Now().Add(duration),
	}

	go s.startAuctionTimer(duration)
	log.Printf("Auction started! Will end at: %v", s.auctionEnd)
	return s
}

func (s *ReplicationServiceServer) startAuctionTimer(duration time.Duration) {
	timer := time.NewTimer(duration)
	<-timer.C

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.isAuctionActive = false

	log.Printf("Auction ended after %v! Winner: %d with bid: %d",
		duration, s.highest_bidder_id, s.highest_bid)
}

func (s *ReplicationServiceServer) Bid(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {
	ClientBid := req.GetAmount()
	ClientID := req.GetId()
	log.Printf("Server [%s] BID: Received bid of %d from client %d", s.port, ClientBid, ClientID)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if ClientBid > s.highest_bid {
		s.highest_bid = ClientBid
		s.highest_bidder_id = ClientID
		log.Printf("Server [%s] BID: New highest bid: %d from client %d", s.port, ClientBid, ClientID)
		return &proto.BidResponse{Ack: true}, nil
	} else {
		log.Printf("Server [%s] BID: Bid rejected (not higher than current: %d)", s.port, s.highest_bid)
		return &proto.BidResponse{Ack: false}, nil
	}
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
	// Start Auction
	NewAuctionService()

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
