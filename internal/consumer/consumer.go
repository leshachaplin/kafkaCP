package consumer

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"time"
)

type Consumer struct {
	r *kafka.Reader
}

func New(topic, groupId string) *Consumer {
	return &Consumer{r: kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     topic,
		GroupID:   groupId,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	}),
	}
}

func Close(c Consumer) error {
	return c.r.Close()
}

func (c *Consumer) Consume(ctx context.Context, consumerRole *Consumer, isWatcher *bool) {
	t2 := time.NewTicker(time.Second * 3)
	tick := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t2.C:
			{
				fmt.Println("read message consumer which on start is false")
				ctx, _ := context.WithTimeout(context.Background(), time.Second)
				m, err := c.r.ReadMessage(ctx)

				if err != nil {
					log.Errorf("consumer doesnt work", err)
					continue
				}
				fmt.Println(string(m.Value))
			}
		case <-tick.C:
			{
				ctx, _ := context.WithTimeout(context.Background(), time.Second)
				m, err := consumerRole.r.ReadMessage(ctx)
				if err != nil {
					log.Errorf("consumer doesnt work", err)
				}
				_ = m
				fmt.Println(string(m.Value))
				if err == nil {
					*isWatcher = !*isWatcher
					fmt.Println("Swicth role consumer which on start is false")
					t2.Stop()
					tick.Stop()
					return
				}
			}
		}
	}
}
