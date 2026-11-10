package main

import (
	"context"
	"errors"
	"gominimq/api"
	"gominimq/broker"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

func main() {
	// Create broker
	b := broker.NewBroker("data")

	if err := b.Recover(); err != nil {
		log.Fatalf("Failed to recover broker: %v", err)
	}

	// Create API server
	server := api.NewServer(b)

	// Create Gin router
	router := gin.Default()

	router.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{"message": "pong"})
	})

	router.POST("/publish", server.PublishHandler)
	router.GET("/consume", server.ConsumeHandler)

	router.POST("/deliver", server.DeliverHandler)
	router.POST("/ack", server.AckHandler)
	router.POST("/nack", server.NackHandler)

	// Create HTTP server
	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	// Listen for shutdown signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)

	// Start server
	go func() {
		log.Println("Server starting on :8080")

		if err := httpServer.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Wait for Ctrl+C or SIGTERM
	<-stop

	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Stop HTTP server gracefully
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)

		if closeErr := httpServer.Close(); closeErr != nil {
			log.Printf("HTTP force-close error: %v", closeErr)
		}
	}

	// Close broker and its topic logs
	if err := b.Close(); err != nil {
		log.Printf("Broker shutdown error: %v", err)
	}

	log.Println("Application shutdown complete")
}
