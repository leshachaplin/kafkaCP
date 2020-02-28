package cmd

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"time"
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
		conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "time1", 0)
		if err != nil {
			log.Fatalf("deadline not set")
		}

		conn2, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "role1", 0)
		if err != nil {
			log.Fatalf("deadline not set")
		}
		defer conn.Close()
		defer conn2.Close()


		consT := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{"localhost:9092"},
			Topic:     "time1",
			GroupID:   fmt.Sprintf("%v", isWatcher),
			Partition: 0,
			MinBytes:  10e3, // 10KB
			MaxBytes:  10e6, // 10MB
		})
		defer consT.Close()

		consR := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{"localhost:9092"},
			Topic:     "role1",
			GroupID:   fmt.Sprintf("%v", isWatcher),
			Partition: 0,
			MinBytes:  10e3, // 10KB
			MaxBytes:  10e6, // 10MB
		})

		defer consR.Close()


		go func() {

			for {
				work(done, conn, conn2, consT, consR)
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

func work(done context.Context, conn *kafka.Conn, conn2 *kafka.Conn, consT *kafka.Reader, consR *kafka.Reader) {
	if !isWatcher {
		t := time.NewTicker(time.Second * 3)
		t2 := time.NewTicker(time.Second * 10)

		for {
			select {
			case <-t.C:
				{
					i, err := conn.WriteMessages(kafka.Message{
						Value: []byte(time.Now().String()),
					})
					if err != nil {
						log.Errorf("consumer doesnt work code ",i, err)
					}
				}
			case <-t2.C:
				isWatcher = !isWatcher
				fmt.Println("Swicth")
				i, err := conn2.WriteMessages(kafka.Message{
					Value: []byte("switch"),
				})
				time.Sleep(time.Second*5)
				if err != nil {
					log.Errorf("consumer doesnt work code ",i, err)
				}
				t.Stop()
				t2.Stop()
				return
			}
		}
	} else {
		t2 := time.NewTicker(time.Second *3)
		tick := time.NewTicker(time.Second *5)
		for {
			select {
			case <-t2.C:
				{
					ctx,_:= context.WithTimeout(context.Background(),time.Second)
					m, err := consT.ReadMessage(ctx)

					if err != nil {
						log.Errorf("consumer doesnt work", err)
						continue
					}
					fmt.Println(string(m.Value))
				}
			case <-tick.C:
				{
					ctx,_:= context.WithTimeout(context.Background(),time.Second)
					m, err := consR.ReadMessage(ctx)
					if err != nil {
						log.Errorf("consumer doesnt work", err)
					}
					_ =m
					/*fmt.Println(string(m.Value))
					if err == nil {
						isWatcher = !isWatcher
						fmt.Println("Swicth 2")
						t.Stop()
						tick.Stop()
						return
					}*/
				}
			}
		}
	}
}
