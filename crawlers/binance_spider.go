package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/exp/slices"

	"github.com/charmbracelet/log"
	"github.com/philippgille/gokv"
)

var (
	CrawlDirectionForward  = "forward"
	CrawlDirectionBackward = "backward"
)

type BinanceCheckpoint struct {
	LeftOpenDate   time.Time
	RightCloseDate time.Time
	LeftProbed     bool
}

type BinanceConfig struct {
	DataRoot    string `yaml:"dataRoot"`
	Biz         string `yaml:"biz"`
	Metric      string `yaml:"metric"`
	Interval    string `yaml:"interval"`
	Granularity string `yaml:"granularity"`
}

type BinanceSpider struct {
	kvStore  gokv.Store
	dataPath string

	biz         string //spot / futures_um / futures_cm / ...
	metric      string //klines / metrics / fundingRate / ...
	interval    string //daily or monthly
	granularity string //5m / 1h / 1d / 1w / 1M
}

var (
	BIZ_SPOT       = "spot"
	BIZ_FUTURES_UM = "futures_um"
	BIZ_FUTURES_CM = "futures_cm"

	METRIC_KLINES       = "klines"
	METRIC_METRICS      = "metrics"
	METRIC_FUNDING_RATE = "fundingRate"

	INTERVAL_MONTHLY = "monthly"
	INTERVAL_DAILY   = "daily"

	BIZS      = []string{BIZ_SPOT, BIZ_FUTURES_CM, BIZ_FUTURES_UM}
	METRICS   = []string{METRIC_KLINES, METRIC_METRICS, METRIC_FUNDING_RATE}
	INTERVALS = []string{INTERVAL_MONTHLY, INTERVAL_DAILY}
	//GRANULARITYS = []string{"1m", "5m", "1h", "4h"}

	metric2ProductIdMap = map[string]int{
		METRIC_KLINES:       1,
		METRIC_METRICS:      6,
		METRIC_FUNDING_RATE: 10,
	}
)

// Document: https://www.binance.com/en/landing/data
func NewBinanceSpider(configFile string) Spider {
	var config BinanceConfig
	initConfig(configFile, &config)

	if !slices.Contains(BIZS, config.Biz) || !slices.Contains(METRICS, config.Metric) {
		log.Fatalf("invalid config")
	}

	if config.Metric == METRIC_FUNDING_RATE && config.Interval != INTERVAL_MONTHLY {
		log.Warnf("force interval to monthly")

		config.Interval = INTERVAL_MONTHLY
	}

	dataPath := filepath.Join(config.DataRoot, config.Biz+"_"+config.Metric, config.Interval, config.Granularity)
	kvstorePath := filepath.Join(dataPath, "gokv")

	if err := os.MkdirAll(dataPath, os.ModePerm); err != nil {
		log.Fatalf("failed to create directory %s, %v", dataPath, err)
	}

	return &BinanceSpider{
		kvStore:     NewKvstore(kvstorePath),
		biz:         config.Biz,
		metric:      config.Metric,
		granularity: config.Granularity,
		interval:    config.Interval,
		dataPath:    dataPath,
	}
}

func (s *BinanceSpider) ProducerCallback(jobCh chan interface{}) {
	url := "https://www.binance.com/bapi/bigdata/v1/public/bigdata/finance/exchange/listDownloadOptions"

	type Payload struct {
		Biz       string `json:"bizType"`
		ProductID int    `json:"productId"`
	}

	type DownloadOptionsResp struct {
		Code string `json:"code"`
		Data struct {
			GranularityList []interface{} `json:"granularityList"`
			Intervals       []string      `json:"intervals"`
			MaxEndDay       string        `json:"maxEndDay"`
			MinStartDay     string        `json:"minStartDay"`
			SymbolList      []string      `json:"symbolList"`
		} `json:"data"`
		Message       *string `json:"message"`
		MessageDetail *string `json:"messageDetail"`
		Success       bool    `json:"success"`
	}

	data := Payload{
		Biz:       strings.ToUpper(s.biz),
		ProductID: metric2ProductIdMap[s.metric],
	}
	payloadBytes, err := json.Marshal(data)
	if err != nil {
		log.Error("Error preparing request body:", err)
		return
	}

	log.Infof("url, %s payload: %s", url, payloadBytes)

	req, err := http.NewRequest("POST", url, bytes.NewReader(payloadBytes))
	if err != nil {
		log.Error("Error creating request:", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Error("failed to do request", err)

		return
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("failed to read response body", err)

		return
	}

	options := DownloadOptionsResp{}
	json.Unmarshal(body, &options)

	log.Infof("options: %#v", options)

	for _, pair := range options.Data.SymbolList {
		// var cp BinanceCheckpoint
		// found, err := s.kvStore.Get(pair, &cp)
		// if err != nil {
		// 	log.Errorf("failed to get checkpoint %v", err)
		// 	continue
		// }

		// if found && cp.LeftProbed {
		// 	continue
		// }

		jobCh <- pair

		log.Infof("send job %s", pair)
	}

	log.Errorf("All jobs sent")
}

func (s *BinanceSpider) ConsumerCallback(jobCh chan interface{}) {
	for job := range jobCh {
		pair := job.(string)
		s.consumeJob(pair, CrawlDirectionBackward)
		s.consumeJob(pair, CrawlDirectionForward)
	}
}

func (s *BinanceSpider) EndCallback() {
	log.Info("binance spider ended")
}

func (s *BinanceSpider) consumeJob(job string, direction string) {
	client := &http.Client{Timeout: 30 * time.Second}

	log.Infof("start to execute job %s:%s", job, direction)

	pair := job
	var cp BinanceCheckpoint
	found, err := s.kvStore.Get(pair, &cp)
	if err != nil {
		log.Errorf("failed to get checkpoint %v", err)
		return
	}

	if !found {
		if direction == CrawlDirectionForward {
			log.Fatalf("you should crawle backward first")
			return
		}

		cp.LeftOpenDate = time.Date(2023, 06, 30, 0, 0, 0, 0, time.UTC)
		cp.RightCloseDate = cp.LeftOpenDate
		cp.LeftProbed = false
	}

	if direction == CrawlDirectionBackward && cp.LeftProbed {
		return
	}

	for {
		var m time.Time
		if direction == CrawlDirectionBackward {
			m = cp.LeftOpenDate
		} else {
			m = cp.RightCloseDate
		}

		var date string
		switch s.interval {
		case INTERVAL_MONTHLY:
			date = m.Format("2006-01")
		case INTERVAL_DAILY:
			date = m.Format("2006-01-02")
		}

		var fileName string
		//fundingRate / klines / metrics.
		//https://data.binance.vision/data/spot/daily/klines/1INCHBTC/5m/1INCHBTC-5m-2023-07-27.zip
		//https://data.binance.vision/data/futures/um/daily/metrics/APTUSDT/APTUSDT-metrics-2023-07-28.zip
		//https://data.binance.vision/data/futures/um/monthly/fundingRate/ATOMUSDT/ATOMUSDT-fundingRate-2023-06.zip
		//https://data.binance.vision/data/futures/um/monthly/metrics/XVGUSDT/XVGUSDT-metrics-2023-06.zip
		switch s.metric {
		case METRIC_KLINES:
			fileName = fmt.Sprintf("%s-%s-%s.zip", pair, s.granularity, date)
		default:
			fileName = fmt.Sprintf("%s-%s-%s.zip", pair, s.metric, date)
		}
		path := filepath.Join(s.dataPath, fileName)

		//hardcode url component futures_um --> futures/um
		biz := s.biz
		if s.biz == BIZ_FUTURES_UM {
			biz = "futures/um"
		}

		//https://data.binance.vision/data/futures/um/daily/klines/ARBUSDT/5m/ARBUSDT-5m-2023-07-28.zip
		var url string
		switch s.metric {
		case METRIC_KLINES:
			url = fmt.Sprintf("https://data.binance.vision/data/%s/%s/%s/%s/%s/%s", biz, s.interval, s.metric, pair, s.granularity, fileName)
		default:
			url = fmt.Sprintf("https://data.binance.vision/data/%s/%s/%s/%s/%s", biz, s.interval, s.metric, pair, fileName)
		}

		notFound, err := DownloadFile(client, url, path)
		if err != nil {
			log.Errorf("failed to download file %s %v", url, err)

			//re-download
			time.Sleep(5 * time.Second)
			continue
		}

		if notFound {
			log.Warnf("file %s not found", url)

			if direction == CrawlDirectionBackward {
				cp.LeftProbed = true
				s.kvStore.Set(pair, cp)
			}

			break
		}

		log.Infof("downloaded file %s", url)

		if direction == CrawlDirectionBackward {
			switch s.interval {
			case INTERVAL_MONTHLY:
				cp.LeftOpenDate = m.AddDate(0, -1, 0)
			case INTERVAL_DAILY:
				cp.LeftOpenDate = m.AddDate(0, 0, -1)
			}
		} else {
			switch s.interval {
			case INTERVAL_MONTHLY:
				cp.RightCloseDate = m.AddDate(0, 1, 0)
			case INTERVAL_DAILY:
				cp.RightCloseDate = m.AddDate(0, 0, 1)
			}
		}

		s.kvStore.Set(pair, cp)
	}
}
