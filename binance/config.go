package main

import (
	"os"

	"github.com/charmbracelet/log"
	"github.com/spf13/viper"
)

type BinanceConfig struct {
	DataRoot         string `yaml:"dataRoot"`
	ConcurrencyLevel int    `yaml:"concurrencyLevel"`
	Biz              string `yaml:"biz"`
	Metric           string `yaml:"metric"`
	Interval         string `yaml:"interval"`
	Granularity      string `yaml:"granularity"`
}

func initConfig(config interface{}) {
	if len(os.Args) != 2 {
		log.Fatalf("Please provide a config file")
	}

	configFile := os.Args[1]
	viper.SetConfigFile(configFile)

	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("failed to read config file %v", err)
	}

	err = viper.Unmarshal(config)
	if err != nil {
		log.Fatalf("failed to unmarshal config file %v", err)
	}
}
