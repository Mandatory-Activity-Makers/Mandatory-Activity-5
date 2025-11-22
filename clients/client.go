package main

import (
	proto "ReplicationService/grpc"
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var clientID int64 = 1
	serverAddr := "localhost:50051"

	// Dial with timeout so failures return quickly during development
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	conn, err := grpc.DialContext(dialCtx, serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Fatalf("failed to dial %s: %v", serverAddr, err)
	}
	defer conn.Close()

	client := proto.NewReplicationServiceClient(conn)

	stdin := bufio.NewScanner(os.Stdin)
	fmt.Println("Commands:")
	fmt.Println("  bid <amount>   - place a bid, e.g. 'bid 100'")
	fmt.Println("  result         - request highest bid")
	fmt.Println("  quit           - exit client")

	for stdin.Scan() {
		line := strings.TrimSpace(stdin.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "quit", "exit":
			fmt.Println("exiting")
			return

		case "bid":
			var amount int64
			if len(parts) >= 2 {
				v, err := strconv.ParseInt(parts[1], 10, 64)
				if err != nil {
					log.Printf("invalid amount: %v", err)
					continue
				}
				amount = v
			} else {
				fmt.Print("Enter bid amount: ")
				if !stdin.Scan() {
					continue
				}
				v, err := strconv.ParseInt(strings.TrimSpace(stdin.Text()), 10, 64)
				if err != nil {
					log.Printf("invalid amount: %v", err)
					continue
				}
				amount = v
			}

			rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 3*time.Second)
			resp, err := client.Bid(rpcCtx, &proto.BidRequest{Amount: amount, Id: clientID})
			rpcCancel()
			if err != nil {
				log.Printf("BID RPC error: %v", err)
				continue
			}
			log.Printf("Bid sent: id=%d amount=%d ack=%v", clientID, amount, resp.GetAck())

		case "result":
			rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 3*time.Second)
			resp, err := client.Result(rpcCtx, &proto.Empty{})
			rpcCancel()
			if err != nil {
				log.Printf("RESULT RPC error: %v", err)
				continue
			}
			log.Printf("RESULT: highest bid=%d by client=%d", resp.GetResult(), resp.GetHighestBidderId())

		default:
			fmt.Println("unknown command; use 'bid <amount>', 'result', or 'quit'")
		}
	}
	if err := stdin.Err(); err != nil {
		log.Printf("stdin error: %v", err)
	}
}
