package main

import (
	proto "ReplicationService/grpc"
	"log"
	"net"

	"google.golang.org/grpc"
)

type ReplicationServiceServer struct {
	proto.UnimplementedReplicationServiceServer
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
