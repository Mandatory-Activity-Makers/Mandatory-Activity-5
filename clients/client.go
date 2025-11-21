package main

import (
	proto "ReplicationService/grpc"
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var clientID int64 = 1
	var serverAddr string = "localhost:50051"
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Not working")
	}
	defer conn.Close() //Connection is closed when main func exits

	//Create gRPC client stub from the proto file
	client := proto.NewReplicationServiceClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stdin := bufio.NewScanner(os.Stdin)
	fmt.Println("Enter 'bid' to place a bid, thereafter enter your bid amount:")

	for stdin.Scan() {
		line := stdin.Text()
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		//Handle exit when user types /leave
		if line == "bid" {
			_, err := client.Bid(ctx, &proto.BidRequest{Amount: 100, Id: 1})
			if err != nil {
				log.Printf("Client BID_RPC_ERROR: %v", err)
			}
			log.Printf("Client BID: id=%d bid 100", clientID)
			// Sleep briefly to allow leave broadcast to flow and then exit
			time.Sleep(200 * time.Millisecond)
			return
		}

		if line == "result" {
			result, err := client.Result(ctx, &proto.Empty{})
			if err != nil {
				log.Printf("Client RESULT_RPC_ERROR: %v", err)
			} else {
				log.Printf("Client RESULT: highest bid is %d by client %d", result.GetResult(), result.GetHighestBidderId())
			}
		}
	}
}
