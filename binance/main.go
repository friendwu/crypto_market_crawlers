package main

import (
	"github.com/charmbracelet/log"
)

func main() {
	config := BinanceConfig{}
	initConfig(&config)

	log.Infof("configs: %#v", config)

	producerConsumer := NewProducerConsumer(&config, binanceInitCallback, binanceProducerCallback, binanceConsumerCallback)
	producerConsumer.Run()

	log.Info("all jobs finished")
}
