package main

type Spider interface {
	ProducerCallback(jobCh chan interface{})
	ConsumerCallback(jobCh chan interface{})
}

func NewSpider(name string, configFile string) Spider {
	switch name {
	case "gateio":
		return NewGateioSpider(configFile)
	case "binance":
		return NewBinanceSpider(configFile)
	case "gecko":
		return NewGeckoSpider(configFile)
	default:
		return nil
	}
}
