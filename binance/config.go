package main

import (
	"github.com/charmbracelet/log"
	"github.com/spf13/viper"
)

func initConfig(configFile string, config interface{}) {
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
