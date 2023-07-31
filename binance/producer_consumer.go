package main

import (
	"sync"

	"github.com/charmbracelet/log"
)

type ProducerConsumer struct {
	concurrencyLevel int
	spider           Spider

	jobCh chan interface{}
	wg    sync.WaitGroup
}

func NewProducerConsumer(concurrencyLevel int, spider Spider) *ProducerConsumer {
	res := &ProducerConsumer{
		concurrencyLevel: concurrencyLevel,
		spider:           spider,
		jobCh:            make(chan interface{}, concurrencyLevel),
	}

	res.wg.Add(concurrencyLevel + 1)

	return res
}

func (pc *ProducerConsumer) Run() {
	go pc.producer()

	for i := 0; i < pc.concurrencyLevel; i++ {
		go pc.consumer()
	}

	pc.wg.Wait()
}

func (pc *ProducerConsumer) producer() {
	defer pc.wg.Done()

	log.Info("start to execute producer")
	pc.spider.ProducerCallback(pc.jobCh)

	close(pc.jobCh)
}

func (pc *ProducerConsumer) consumer() {
	defer pc.wg.Done()

	pc.spider.ConsumerCallback(pc.jobCh)
}
