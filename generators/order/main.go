package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"shared-lib/common"
	sharedKafka "shared-lib/kafka"

	"github.com/segmentio/kafka-go"
)

type OrderEvent struct {
	EventType string  `json:"event_type"`
	UserID    int     `json:"user_id"`
	OrderID   string  `json:"order_id"`
	SessionID string  `json:"session_id"`
	Timestamp int64   `json:"timestamp"`
	Amount    float64 `json:"amount"`
	Status    string  `json:"status"`
}

func main() {
	topic := common.GetEnv("TOPIC", "order-events")
	writer := sharedKafka.NewWriter([]string{"kafka:9092"}, topic)
	defer writer.Close()

	log.Println("Order generator started ...")

	for {
		event := generateOrderEvent()
		eventJSON, _ := json.Marshal(event)

		err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   fmt.Appendf(nil, "user-%d", event.UserID),
				Value: eventJSON,
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

func generateOrderEvent() OrderEvent {
	statuses := []string{"created", "completed", "cancelled"}

	return OrderEvent{
		EventType: "order_event",
		UserID:    rand.Intn(1000) + 1,
		OrderID:   fmt.Sprintf("order-%d-%d", time.Now().Unix(), rand.Intn(1000)),
		SessionID: fmt.Sprintf("session-%d", rand.Intn(10000)),
		Timestamp: time.Now().Unix(),
		Amount:    float64(rand.Intn(10000)+100) / 100.0,
		Status:    statuses[rand.Intn(len(statuses))],
	}
}
