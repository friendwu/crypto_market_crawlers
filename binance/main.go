package main

import (
	"flag"

	"github.com/charmbracelet/log"
)

func main() {
	spider := flag.String("spider", "", "spider name")
	concurrencyLevel := flag.Int("concurrency", 0, "concurrency level")
	configFile := flag.String("config", "", "config file")
	flag.Parse()

	var config interface{}

	switch *spider {
	case "gateio":
		config = &GateioConfig{}
	case "binance":
		config = &BinanceConfig{}
	default:
		log.Fatalf("invalid spider name")
	}

	initConfig(*configFile, config)

	log.Infof("configs: %#v", config)

	var producerConsumer *ProducerConsumer

	//TODO callback into a struct.
	switch *spider {
	case "gateio":
		producerConsumer = NewProducerConsumer(*concurrencyLevel, config, gateioInitCallback, gateioProducerCallback, gateioConsumerCallback)

	case "binance":
		producerConsumer = NewProducerConsumer(*concurrencyLevel, config, binanceInitCallback, binanceProducerCallback, binanceConsumerCallback)
	default:
		log.Fatalf("invalid spider name")
	}

	producerConsumer.Run()

	log.Info("all jobs finished")
}
