package api

import (
	"bytes"
	"encoding/json"
	"gominimq/broker"
	"gominimq/models"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func setupTestServer(t *testing.T) *gin.Engine {
	gin.SetMode(gin.TestMode)

	b := broker.NewBroker(t.TempDir())

	if err := b.CreateTopic("orders"); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Errorf("failed to close broker: %v", err)
		}
	})

	s := NewServer(b)

	router := gin.New()
	router.POST("/publish", s.PublishHandler)
	router.GET("/consume", s.ConsumeHandler)

	return router
}

func TestPublishHandler(t *testing.T) {
	router := setupTestServer(t)

	body := PublishRequest{
		Topic: "orders",
		Key:   "order-1",
		Value: "new order",
	}

	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(
		http.MethodPost,
		"/publish",
		bytes.NewReader(data),
	)
	req.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf(
			"expected status 200, got %d, response: %s",
			recorder.Code,
			recorder.Body.String(),
		)
	}
}

func TestPublishHandlerInvalidInput(t *testing.T) {
	router := setupTestServer(t)

	body := PublishRequest{
		Topic: "",
		Key:   "order-1",
		Value: "new order",
	}

	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(
		http.MethodPost,
		"/publish",
		bytes.NewReader(data),
	)
	req.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected status 400, got %d, response: %s",
			recorder.Code,
			recorder.Body.String(),
		)
	}
}

func TestConsumeHandler(t *testing.T) {
	router := setupTestServer(t)

	// First publish a message.
	body := PublishRequest{
		Topic: "orders",
		Key:   "order-1",
		Value: "new order",
	}

	data, _ := json.Marshal(body)

	publishReq := httptest.NewRequest(
		http.MethodPost,
		"/publish",
		bytes.NewReader(data),
	)
	publishReq.Header.Set("Content-Type", "application/json")

	publishRecorder := httptest.NewRecorder()
	router.ServeHTTP(publishRecorder, publishReq)

	if publishRecorder.Code != http.StatusOK {
		t.Fatalf("publish failed: %s", publishRecorder.Body.String())
	}

	// Now consume the message.
	consumeReq := httptest.NewRequest(
		http.MethodGet,
		"/consume?topic=orders",
		nil,
	)

	consumeRecorder := httptest.NewRecorder()
	router.ServeHTTP(consumeRecorder, consumeReq)

	if consumeRecorder.Code != http.StatusOK {
		t.Fatalf("consume failed: %s", consumeRecorder.Body.String())
	}

	var msg models.Message

	if err := json.Unmarshal(consumeRecorder.Body.Bytes(), &msg); err != nil {
		t.Fatal(err)
	}

	if msg.Key != "order-1" || msg.Value != "new order" {
		t.Errorf("unexpected message: %+v", msg)
	}
}
