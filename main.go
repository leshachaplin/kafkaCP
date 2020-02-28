package main

import (
	"github.com/leshachaplin/kafkaCP/cmd"
	"log"
)

func main() {
	err := cmd.Execute()
	if err != nil {
		log.Fatal(err)
	}
}
