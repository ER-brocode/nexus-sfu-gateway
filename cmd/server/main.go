package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ER-brocode/nexus-sfu-gateway/pkg/sfu"
	"github.com/ER-brocode/nexus-sfu-gateway/pkg/signaling"
)

func main() {
	port := flag.Int("port", 8080, "Port for Nexus SFU Gateway HTTP/WebSocket server")
	flag.Parse()

	log.Println("=========================================================")
	log.Println("  NEXUS SFU GATEWAY - Zero-Latency AV Streaming Gateway  ")
	log.Println("=========================================================")

	router, err := sfu.NewSFURouter()
	if err != nil {
		log.Fatalf("Failed to initialize SFU Router: %v", err)
	}
	_ = router

	sigServer := signaling.NewServer()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", sigServer.HandleWebSocket)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"healthy","gateway":"nexus-sfu-gateway"}`))
	})

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("Nexus SFU Gateway listening on port %d...", *port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failure: %v", err)
		}
	}()

	<-stop
	log.Println("Shutting down Nexus SFU Gateway...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	log.Println("Gateway stopped gracefully.")
}
