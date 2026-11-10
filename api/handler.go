package api

import (
	"gominimq/broker"
	"gominimq/models"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type Server struct {
	Broker *broker.Broker
}

func NewServer(b *broker.Broker) *Server {
	return &Server{Broker: b}
}

type PublishRequest struct {
	Topic string `json:"topic"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type TopicRequest struct {
	Topic string `json:"topic"`
}

type AckRequest struct {
	Topic     string `json:"topic"`
	MessageID int64  `json:"message_id"`
}

// PublishHandler publishes a message.
func (s *Server) PublishHandler(c *gin.Context) {
	var req PublishRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON"})
		return
	}

	if req.Topic == "" || req.Key == "" || req.Value == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Topic, key and value are required",
		})
		return
	}

	msg := models.Message{
		Key:       req.Key,
		Value:     req.Value,
		Timestamp: time.Now().Unix(),
	}

	if !s.Broker.Publish(req.Topic, msg) {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "Topic not found or broker unavailable",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Message published successfully",
	})
}

// ConsumeHandler permanently removes the next ready message.
// This is the legacy consume operation without ACK.
func (s *Server) ConsumeHandler(c *gin.Context) {
	topicName := c.Query("topic")

	if topicName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Topic is required"})
		return
	}

	msg := s.Broker.Consume(topicName)

	if msg == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "No message available"})
		return
	}

	c.JSON(http.StatusOK, msg)
}

// DeliverHandler moves a message into the in-flight state.
func (s *Server) DeliverHandler(c *gin.Context) {
	var req TopicRequest

	if err := c.ShouldBindJSON(&req); err != nil || req.Topic == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Valid topic is required"})
		return
	}

	if _, exists := s.Broker.GetTopic(req.Topic); !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Topic not found"})
		return
	}

	msg := s.Broker.Deliver(req.Topic)

	if msg == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "No message available"})
		return
	}

	c.JSON(http.StatusOK, msg)
}

// AckHandler durably acknowledges a delivered message.
func (s *Server) AckHandler(c *gin.Context) {
	var req AckRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON"})
		return
	}

	if req.Topic == "" || req.MessageID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Topic and valid message_id are required",
		})
		return
	}

	if _, exists := s.Broker.GetTopic(req.Topic); !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Topic not found"})
		return
	}

	if !s.Broker.Ack(req.Topic, req.MessageID) {
		c.JSON(http.StatusConflict, gin.H{
			"error": "Message is not in-flight or ACK could not be persisted",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Message acknowledged successfully"})
}

// NackHandler returns a delivered message to the ready queue.
func (s *Server) NackHandler(c *gin.Context) {
	var req AckRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON"})
		return
	}

	if req.Topic == "" || req.MessageID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Topic and valid message_id are required",
		})
		return
	}

	if _, exists := s.Broker.GetTopic(req.Topic); !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Topic not found"})
		return
	}

	if !s.Broker.Nack(req.Topic, req.MessageID) {
		c.JSON(http.StatusConflict, gin.H{
			"error": "Message is not in-flight",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Message returned to queue"})
}
