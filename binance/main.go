package main

import (
	"flag"

	"github.com/charmbracelet/log"
)

func main() {
	spiderName := flag.String("spider", "", "spider name")
	concurrencyLevel := flag.Int("concurrency", 0, "concurrency level")
	configFile := flag.String("config", "", "config file")
	flag.Parse()

	spider := NewSpider(*spiderName, *configFile)
	producerConsumer := NewProducerConsumer(*concurrencyLevel, spider)

	producerConsumer.Run()

	log.Info("all jobs finished")
}
