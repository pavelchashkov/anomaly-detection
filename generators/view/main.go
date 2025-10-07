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

type ViewEvent struct {
	EventType string    `json:"event_type"`
	UserID    int       `json:"user_id"`
	ProductID int       `json:"product_id,omitempty"`
	SessionID string    `json:"session_id"`
	Timestamp time.Time `json:"timestamp"`
	Page      string    `json:"page,omitempty"`
}

func main() {
	topic := common.GetEnv("TOPIC", "page-views")
	writer := sharedKafka.NewWriter([]string{"kafka:9092"}, topic)
	defer writer.Close()

	defer writer.Close()

	log.Println("View generator started ...")

	for {
		event := generateViewEvent()
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
			log.Printf("Sent cart event: %s", eventJSON)
		}

		time.Sleep(time.Duration(1+rand.Intn(4)) * time.Second)
	}
}

func generateViewEvent() ViewEvent {
	pages := []string{"home", "catalog", "product", "category"}
	eventTypes := []string{"page_view", "product_click"}

	return ViewEvent{
		EventType: eventTypes[rand.Intn(len(eventTypes))],
		UserID:    rand.Intn(1000) + 1,
		ProductID: rand.Intn(100) + 1,
		SessionID: fmt.Sprintf("session-%d", rand.Intn(10000)),
		Timestamp: time.Now(),
		Page:      pages[rand.Intn(len(pages))],
	}
}
