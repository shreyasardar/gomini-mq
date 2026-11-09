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
	return &Server{
		Broker: b,
	}
}

type PublishRequest struct {
	Topic string `json:"topic"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (s *Server) PublishHandler(c *gin.Context) {
	var req PublishRequest

	// Decode incoming JSON into the request struct.
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid JSON",
		})
		return
	}

	// Validate request fields.
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

	success := s.Broker.Publish(req.Topic, msg)

	if !success {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "Topic not found or broker unavailable",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Message published successfully",
	})
}

func (s *Server) ConsumeHandler(c *gin.Context) {
	topicName := c.Query("topic")

	if topicName == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Topic is required",
		})
		return
	}

	msg := s.Broker.Consume(topicName)

	if msg == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "No message available",
		})
		return
	}

	c.JSON(http.StatusOK, msg)
}
