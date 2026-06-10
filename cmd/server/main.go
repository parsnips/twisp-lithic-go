package main

import (
	"context"
	"log"
	"net/http"
	"os"

	twisplithic "github.com/parsnips/twisp-lithic-go"
)

func main() {
	target := env("TWISP_GRPC_TARGET", "localhost:8081")
	accountID := env("TWISP_ACCOUNT_ID", "000000000000")
	addr := env("HTTP_ADDR", ":8090")

	ctx := context.Background()
	client, err := twisplithic.DialLocal(ctx, target, accountID)
	if err != nil {
		log.Fatalf("dial twisp grpc: %v", err)
	}
	defer client.Close()

	processor := twisplithic.NewProcessor(client)
	log.Printf("listening on %s; forwarding to Twisp gRPC %s as account %s", addr, target, accountID)
	if err := http.ListenAndServe(addr, twisplithic.NewHandler(processor)); err != nil {
		log.Fatal(err)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
