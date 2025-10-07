package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"shared-lib/common"
	sharedKafka "shared-lib/kafka"

	"github.com/segmentio/kafka-go"
)

type CartEvent struct {
	EventType string `json:"event_type"`
	UserID    int    `json:"user_id"`
	ProductID int    `json:"product_id"`
	CartID    string `json:"cart_id"`
	SessionID string `json:"session_id"`
	Timestamp int64  `json:"timestamp"`
	Action    string `json:"action"`
	Quantity  int    `json:"quantity"`
}

func main() {
	topic := common.GetEnv("TOPIC", "cart-events")
	writer := sharedKafka.NewWriter([]string{"kafka:9092"}, topic)
	defer writer.Close()

	log.Println("Cart generator started ...")

	for {
		event := generateCartEvent()
		eventJSON := fmt.Sprintf(`{"type": "%s", "user_id": %d, "product_id": %d, "quantity": %d}`,
			event.EventType, event.UserID, event.ProductID, event.Quantity)

		err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   fmt.Appendf(nil, "user-%d", event.UserID),
				Value: []byte(eventJSON),
			},
		)

		if err != nil {
			log.Printf("Failed to write message: %v", err)
		} else {
			log.Printf("Sent event: %s", eventJSON)
		}

		time.Sleep(time.Duration(5+rand.Intn(15)) * time.Second)
	}
}

func generateCartEvent() CartEvent {
	eventTypes := []string{"add_to_cart", "remove_from_cart", "update_quantity"}

	return CartEvent{
		EventType: eventTypes[rand.Intn(len(eventTypes))],
		UserID:    rand.Intn(1000) + 1,
		ProductID: rand.Intn(100) + 1,
		CartID:    fmt.Sprintf("cart-%d", rand.Intn(10000)),
		SessionID: fmt.Sprintf("session-%d", rand.Intn(10000)),
		Timestamp: time.Now().Unix(),
		Quantity:  rand.Intn(5) + 1,
		Action:    eventTypes[rand.Intn(len(eventTypes))],
	}
}
