package main

import (
	"encoding/json"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/charmbracelet/log"
	"github.com/philippgille/gokv"

	coingecko "github.com/superoo7/go-gecko/v3"
	coingecko_types "github.com/superoo7/go-gecko/v3/types"
)

var (
	TASK_STATUS_DONE = "done"
	TASK_STATUS_DOING = "doing"
	TASK_STATUS_KEY = "task_status"
)

type checkpoint struct {
	LeftDate  time.Time
	RightDate time.Time
}

type taskStatus struct {
	Batch int 
	startTime time.Time	
	endTime   time.Time

	Status string // "doing", "done"
}

type GeckoConfig struct {
	DSN string `yaml:"dsn"`
	DataRoot     string `yaml:"data_root"`
}

type GeckoSpider struct {
	cgClient     *coingecko.Client
	statusKvStore      gokv.Store
	batchKvStore      gokv.Store
	db           *gorm.DB
	dataPath     string
	coinListPath string	
}

type GeckoMarket struct {
	gorm.Model

	GeckoId      string    `gorm:"column:gecko_id;primaryKey"`
	Symbol       string    `gorm:"column:symbol"`
	Name         string    `gorm:"column:name"`
	Timestamp    int64     `gorm:"column:timestamp;primaryKey"`
	DateTime     time.Time `gorm:"column:datetime"`
	PriceUsd     float64   `gorm:"column:price_usd"`
	MarketCapUsd float64   `gorm:"column:market_cap_usd"`
	Volume24hUsd float64   `gorm:"column:volume_24h_usd"`
}

func NewGeckoSpider(configFile string) *GeckoSpider {
	var config GeckoConfig
	initConfig(configFile, &config)

	db, err := gorm.Open(postgres.Open(config.DSN), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect database: %#v", err)
	}

	proxyUrl, _ := url.Parse("http://127.0.0.1:8443")
	cgClient := coingecko.NewClient(&http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: http.ProxyURL(proxyUrl)}})
	kvstorePath := filepath.Join(config.DataRoot, "gokv")
	dataPath := filepath.Join(config.DataRoot, "coin_history")
	coinListPath := filepath.Join(config.DataRoot, "coins.json")

	statusKvStore := NewKvstore(kvstorePath)
	ts := &taskStatus{}
	found, err := statusKvStore.Get(TASK_STATUS_KEY, ts)
	if err!= nil {
		log.Fatalf("failed to get checkpoint in kvstore: %#v", err)
	}

	if !found {
		ts.Batch = 0
		ts.Status = TASK_STATUS_DOING
		ts.startTime = time.Now()
	} 
	
	if ts.Status == TASK_STATUS_DONE {
		ts.Batch = ts.Batch + 1
		ts.Status = TASK_STATUS_DOING
		ts.startTime = time.Now()
		ts.endTime = time.Time{}
	}

	err = statusKvStore.Set(TASK_STATUS_KEY, ts)
	if err!= nil {
		log.Fatalf("failed to set checkpoint in kvstore: %#v", err)
	}

	batchKvStore := NewKvstore(filepath.Join(kvstorePath, "batches", strconv.Itoa(ts.Batch)))
	
	return &GeckoSpider{
		db:       db,
		cgClient: cgClient,
		statusKvStore: statusKvStore,
		batchKvStore:  batchKvStore,
		dataPath: dataPath,
		coinListPath: coinListPath,
	}
}

func (s *GeckoSpider) ProducerCallback(jobCh chan interface{}) {
	coins, err := s.cgClient.CoinsList()
	if err != nil {
		log.Fatalf("failed to get coin list in producer, %v", err)
	}

	r, _ := json.Marshal(coins)
	if err = os.WriteFile(s.coinListPath, r, 0644); err != nil {
		log.Error("failed to write coin list, %v", err)
	}

	for _, coin := range *coins {
		path := filepath.Join(s.dataPath, coin.ID)
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			log.Errorf("failed to create directory, %s, %v", path, err)
		}

		jobCh <- coin
	}

	ts := &taskStatus{}
	found, err := s.statusKvStore.Get(TASK_STATUS_KEY, ts)
	if err != nil || !found {
		log.Fatalf("failed to get task status, found: %v, %v", found, err)
		return 
	}

	ts.Status = TASK_STATUS_DONE
	ts.endTime = time.Now()
	s.statusKvStore.Set(TASK_STATUS_KEY, ts)

	log.Info("all coins sent")
}

func (s *GeckoSpider) ConsumerCallback(jobCh chan interface{}) {
	proxyUrl, _ := url.Parse("http://127.0.0.1:8443")
	// cgClient := coingecko.NewClient(&http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: http.ProxyURL(proxyUrl), DisableKeepAlives: true}})
	cgClient := coingecko.NewClient(&http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: http.ProxyURL(proxyUrl)}})

	for coin := range jobCh {
		s.syncCoinHistory(coin.(coingecko_types.CoinsListItem), cgClient)
	}
}

func (s *GeckoSpider) syncCoinHistory(coin coingecko_types.CoinsListItem, cgClient *coingecko.Client) {
	cp := &checkpoint{}
	found, err := s.batchKvStore.Get(coin.ID, cp)
	if err != nil {
		log.Errorf("failed to get checkpoint of %v, %v", coin, err)
		return
	}

	if found && !cp.LeftDate.IsZero() && !cp.RightDate.IsZero() {
		return //already synced
	}

	retryDuration := 61 * time.Second

	for {
		coinMarkets, err := cgClient.CoinsIDMarketChart(coin.ID, "usd", "max")

		if err != nil {
			if err1, ok := err.(net.Error); ok && err1.Timeout() {
				log.Warnf("timeout %v", err1)
			} else {
				if strings.Contains(err.Error(), "429") {
					log.Warnf("too many requests, sleep %v, %v, %v", retryDuration, coin, err)
				} else {
					log.Errorf("failed to get coin history of %v, %v", coin, err)
				}
			}

			time.Sleep(retryDuration + time.Duration(rand.Intn(10))*time.Second)

			continue
		}

		// r, _ := json.Marshal(coinMarkets)
		// path := filepath.Join(dataPath, coin+".json")

		// err = os.WriteFile(path, r, 0644)
		// if err != nil {
		// 	log.Errorf("failed to write coin history of %v, %v", coin, err)
		// 	return
		// }

		for i := 0; i < len(*(*coinMarkets).Prices); i++ {
			item := GeckoMarket{
				GeckoId:      coin.ID,
				Symbol:       coin.Symbol,
				Name:         coin.Name,
				Timestamp:    int64((*(*coinMarkets).Prices)[i][0]),
				DateTime:     time.Unix(int64((*(*coinMarkets).Prices)[i][0]/1000), 0),
				PriceUsd:     float64((*(*coinMarkets).Prices)[i][1]),
				MarketCapUsd: float64((*(*coinMarkets).MarketCaps)[i][1]),
				Volume24hUsd: float64((*(*coinMarkets).TotalVolumes)[i][1]),
			}

			// Upsert
			result := s.db.Table("blockchain_overview.gecko_markets").Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "gecko_id"}, {Name: "timestamp"}},                            // Use the "id" column to determine if a record exists
				DoUpdates: clause.AssignmentColumns([]string{"price_usd", "market_cap_usd", "volume_24h_usd"}), // If a record exists, update the "name" and "age_x" fields
			}).Create(&item)

			if result.Error != nil {
				log.Fatalf("Failed to upsert %v, %v", coin, result.Error)
			}
		}

		cp.LeftDate = time.Unix(int64((*(*coinMarkets).Prices)[0][0]/1000), 0)
		cp.RightDate = time.Unix(int64((*(*coinMarkets).Prices)[len(*(*coinMarkets).Prices)-1][0]/1000), 0)

		err = s.batchKvStore.Set(coin.ID, cp)

		if err != nil {
			log.Errorf("failed to update checkpoint of %v, %v", coin, err)
			return
		}

		log.Infof("synced coin history of %v", coin)

		break
	}
}
