package main

import (
	proto "ReplicationService/grpc"
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ClientConnection struct {
	serverAddrs  []string
	currentIdx   int
	client       proto.ReplicationServiceClient
	conn         *grpc.ClientConn
	mu           sync.RWMutex
	reconnecting bool
	clientID     string
}

func NewClientConnection(clientID string, serverAddrs []string, startRandom bool) *ClientConnection {
	startIdx := 0
	if startRandom {
		startIdx = rand.Intn(len(serverAddrs))
	}

	return &ClientConnection{
		serverAddrs: serverAddrs,
		currentIdx:  startIdx,
		clientID:    clientID,
	}
}

// Connect establishes initial connection to the first available server
func (cc *ClientConnection) Connect() error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	for i := 0; i < len(cc.serverAddrs); i++ {
		addr := cc.serverAddrs[cc.currentIdx]
		log.Printf("Attempting to connect to server at %s...", addr)

		dialCtx, dialCancel := context.WithTimeout(context.Background(), 3*time.Second)
		conn, err := grpc.DialContext(dialCtx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		dialCancel()

		if err != nil {
			log.Printf("Failed to connect to %s: %v", addr, err)
			cc.currentIdx = (cc.currentIdx + 1) % len(cc.serverAddrs)
			continue
		}

		cc.conn = conn
		cc.client = proto.NewReplicationServiceClient(conn)
		log.Printf("Successfully connected to server at %s", addr)
		return nil
	}

	return fmt.Errorf("failed to connect to any server")
}

// Reconnect tries to connect to the next available server
func (cc *ClientConnection) Reconnect() error {
	cc.mu.Lock()
	if cc.reconnecting {
		cc.mu.Unlock()
		return fmt.Errorf("already reconnecting")
	}
	cc.reconnecting = true

	// Close old connection if exists
	if cc.conn != nil {
		cc.conn.Close()
		cc.conn = nil
		cc.client = nil
	}
	cc.mu.Unlock()

	// Keep trying forever until we connect
	for {
		cc.mu.Lock()
		cc.currentIdx = (cc.currentIdx + 1) % len(cc.serverAddrs)
		addr := cc.serverAddrs[cc.currentIdx]
		cc.mu.Unlock()

		log.Printf("Reconnecting to server at %s...", addr)

		dialCtx, dialCancel := context.WithTimeout(context.Background(), 3*time.Second)
		conn, err := grpc.DialContext(dialCtx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		dialCancel()

		if err != nil {
			log.Printf("Failed to reconnect to %s: %v", addr, err)
			time.Sleep(time.Second) // Wait before trying next server
			continue
		}

		cc.mu.Lock()
		cc.conn = conn
		cc.client = proto.NewReplicationServiceClient(conn)
		cc.reconnecting = false
		cc.mu.Unlock()

		log.Printf("Successfully reconnected to server at %s", addr)
		return nil
	}
}

// GetClient returns the current client, reconnecting if necessary
func (cc *ClientConnection) GetClient() proto.ReplicationServiceClient {
	cc.mu.RLock()
	client := cc.client
	cc.mu.RUnlock()

	if client == nil {
		cc.Reconnect()
		cc.mu.RLock()
		client = cc.client
		cc.mu.RUnlock()
	}

	return client
}

// Close closes the connection
func (cc *ClientConnection) Close() {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if cc.conn != nil {
		cc.conn.Close()
	}
}

// ========== NEW METHOD: ConsensusBid ==========
// This method handles the consensus bidding process with proper timeouts
func (cc *ClientConnection) ConsensusBid(amount int64) (bool, error) {
	// Keep trying until successful or user gives up
	maxAttempts := 3

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		client := cc.GetClient()

		// CONSENSUS TAKES LONGER - use longer timeout
		rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 10*time.Second) // Increased from 3s to 10s

		log.Printf("Attempting bid of %d (attempt %d/%d)...", amount, attempt, maxAttempts)
		resp, err := client.Bid(rpcCtx, &proto.BidRequest{
			Amount: amount,
			Id:     cc.clientID,
		})

		rpcCancel()

		if err != nil {
			log.Printf("BID RPC error (attempt %d): %v", attempt, err)

			// Check if it's a timeout (consensus might be taking too long)
			if strings.Contains(err.Error(), "deadline exceeded") ||
				strings.Contains(err.Error(), "timeout") {
				log.Printf("Consensus taking too long, trying different server...")
				cc.Reconnect()
				continue
			}

			// Other errors - reconnect and retry
			cc.Reconnect()
			time.Sleep(time.Duration(attempt) * time.Second) // Exponential backoff
			continue
		}

		// Success!
		return resp.GetAck(), nil
	}

	return false, fmt.Errorf("bid failed after %d attempts", maxAttempts)
}

// ========== MAIN FUNCTION (minimal changes) ==========
func main() {
	var clientID string

	flag.StringVar(&clientID, "id", "", "Client ID (required)")
	flag.Parse()

	// List all server addresses
	serverAddrs := []string{
		"localhost:50051",
		"localhost:50052",
		"localhost:50053",
	}

	if clientID == "" {
		fmt.Fprintln(os.Stderr, "client id is required: -id <name>")
		os.Exit(2)
	}

	clientConn := NewClientConnection(clientID, serverAddrs, true)

	if err := clientConn.Connect(); err != nil {
		log.Fatalf("Failed to establish initial connection: %v", err)
	}
	defer clientConn.Close()

	stdin := bufio.NewScanner(os.Stdin)
	fmt.Println("========================================")
	fmt.Printf("Auction Client (ID: %s)\n", clientID)
	fmt.Println("========================================")
	fmt.Println("Commands:")
	fmt.Println("  bid <amount>   - place a bid (uses consensus)")
	fmt.Println("  result         - request highest bid")
	fmt.Println("  quit           - exit client")
	fmt.Println("========================================")
	fmt.Println("NOTE: Bids now use consensus protocol")
	fmt.Println("      This may take longer than before")
	fmt.Println("========================================")

	for stdin.Scan() {
		line := strings.TrimSpace(stdin.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "quit", "exit":
			fmt.Println("Exiting...")
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

			// ========== CHANGED: Use ConsensusBid instead of direct RPC ==========
			ack, err := clientConn.ConsensusBid(amount)
			if err != nil {
				log.Printf("BID failed: %v", err)
			} else {
				status := "REJECTED"
				if ack {
					status = "ACCEPTED"
				}
				log.Printf("Bid %s: amount=%d", status, amount)
			}

		// In your main() function, just modify the "result" case:
		case "result":
			var finalResp *proto.ResultResponse
			var finalErr error

			// Try up to 3 times with different servers
			for attempt := 1; attempt <= 3; attempt++ {
				client := clientConn.GetClient()
				rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 2*time.Second)
				resp, err := client.Result(rpcCtx, &proto.Empty{})
				rpcCancel()

				if err != nil {
					finalErr = err
					clientConn.Reconnect()
					continue
				}

				// If we get result=0 on first attempt, be suspicious
				if attempt == 1 && resp.GetResult() == 0 {
					log.Printf("Got result=0, trying another server to verify...")
					clientConn.Reconnect() // Try next server
					continue
				}

				finalResp = resp
				finalErr = nil
				break
			}

			if finalErr != nil {
				log.Printf("RESULT ERROR: %v", finalErr)
			} else if finalResp != nil {
				log.Printf("RESULT: highest bid=%d by client=%s",
					finalResp.GetResult(), finalResp.GetHighestBidderId())
			}
		}

		// Print prompt
		fmt.Print("\n> ")
	}

	if err := stdin.Err(); err != nil {
		log.Printf("stdin error: %v", err)
	}
}
