package producer

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"time"
)

type Producer struct {
	con *kafka.Conn
}

func New(con *kafka.Conn) *Producer {
	return &Producer{
		con: con,
	}
}

func (p *Producer) Produce(ctx context.Context, connRole *Producer, isWatcher *bool) {
	t := time.NewTicker(time.Second * 3)
	t2 := time.NewTicker(time.Second * 10)

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			{
				fmt.Println("producer which on start true ")
				i, err := p.con.WriteMessages(kafka.Message{
					Value: []byte(time.Now().String()),
				})
				fmt.Print("send message")
				if err != nil {
					log.Errorf("consumer doesnt work code ", i, err)
				}
			}
		case <-t2.C:
			*isWatcher = !*isWatcher
			fmt.Println("Swicth producer which on start is true")
			i, err := connRole.con.WriteMessages(kafka.Message{
				Value: []byte("switch"),
			})
			time.Sleep(time.Second * 5)
			if err != nil {
				log.Errorf("consumer doesnt work code ", i, err)
			}
			t.Stop()
			t2.Stop()
			return
		}
	}
}
