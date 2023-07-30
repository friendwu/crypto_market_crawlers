package main

import (
	"sync"

	"github.com/charmbracelet/log"
)

type AFunc func(context interface{}, jobCh chan interface{})
type IFunc func(context interface{}) interface{}

type ProducerConsumer struct {
	concurrencyLevel     int
	producerCallbackFunc AFunc
	consumerCallbackFunc AFunc
	initCallbackFunc     IFunc
	initCallbackContext  interface{}

	jobCh chan interface{}
	wg    sync.WaitGroup
}

func NewProducerConsumer(config *BinanceConfig,
	initCallbackFunc IFunc,
	producerCallbackFunc AFunc,
	consumerCallbackFunc AFunc) *ProducerConsumer {
	res := &ProducerConsumer{
		concurrencyLevel:     config.ConcurrencyLevel,
		initCallbackContext:  initCallbackFunc(config),
		producerCallbackFunc: producerCallbackFunc,
		consumerCallbackFunc: consumerCallbackFunc,
		jobCh:                make(chan interface{}, config.ConcurrencyLevel),
	}

	res.wg.Add(config.ConcurrencyLevel + 1)

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
	pc.producerCallbackFunc(pc.initCallbackContext, pc.jobCh)

	close(pc.jobCh)
}

func (pc *ProducerConsumer) consumer() {
	defer pc.wg.Done()

	pc.consumerCallbackFunc(pc.initCallbackContext, pc.jobCh)
}
