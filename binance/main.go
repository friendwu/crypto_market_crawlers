package main

import (
	"github.com/charmbracelet/log"
)

func main() {
	config := initConfig()

	producerConsumer := NewProducerConsumer(config, binanceInitCallback, binanceProducerCallback, binanceConsumerCallback)
	producerConsumer.Run()

	log.Info("all jobs finished")
}
