package cmd

import (
	"context"
	"fmt"
	"github.com/leshachaplin/kafkaCP/internal/consumer"
	"github.com/leshachaplin/kafkaCP/internal/producer"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
)

var (
	isWatcher bool
)

var rootCmd = &cobra.Command{
	Use:   "",
	Short: "",
	Long:  `.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(isWatcher)
		s := make(chan os.Signal)
		signal.Notify(s, os.Interrupt)
		done, cnsl := context.WithCancel(context.Background())
		connTime, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "time", 0)
		if err != nil {
			log.Fatalf("deadline not set")
		}

		connRole, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "role", 0)
		if err != nil {
			log.Fatalf("deadline not set")
		}
		defer connTime.Close()
		defer connRole.Close()

		produceTime := producer.New(connTime)
		produceRole := producer.New(connRole)

		//consT := kafka.NewReader(kafka.ReaderConfig{
		//	Brokers:   []string{"localhost:9092"},
		//	Topic:     "time1",
		//	GroupID:   fmt.Sprintf("%v", isWatcher),
		//	Partition: 0,
		//	MinBytes:  10e3, // 10KB
		//	MaxBytes:  10e6, // 10MB
		//})
		//defer consT.Close()
		//
		//consR := kafka.NewReader(kafka.ReaderConfig{
		//	Brokers:   []string{"localhost:9092"},
		//	Topic:     "role1",
		//	GroupID:   fmt.Sprintf("%v", isWatcher),
		//	Partition: 0,
		//	MinBytes:  10e3, // 10KB
		//	MaxBytes:  10e6, // 10MB
		//})

		//defer consR.Close()
		consumerTime := consumer.New("time", fmt.Sprintf("%v", isWatcher))
		consumerRole := consumer.New("role", fmt.Sprintf("%v", isWatcher))
		defer consumer.Close(*consumerTime)
		defer consumer.Close(*consumerRole)

		go func() {

			for {
				work(done, produceTime, produceRole, consumerTime, consumerRole)
			}
		}()

		<-s
		close(s)
		cnsl()
	},
}

func init() {
	rootCmd.PersistentFlags().BoolVar(&isWatcher, "watcher", false, "User role for app")
}

func Execute() error {
	return rootCmd.Execute()
}

func work(done context.Context, prodT *producer.Producer, prodR *producer.Producer, consT *consumer.Consumer, consR *consumer.Consumer) {
	if !isWatcher {
		prodT.Produce(done, prodR, &isWatcher)
	} else {
		consT.Consume(done, consR, &isWatcher)
	}
}
