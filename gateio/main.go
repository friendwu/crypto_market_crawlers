package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/log"
	"github.com/philippgille/gokv"
	"github.com/philippgille/gokv/file"
	"github.com/spf13/viper"
)

// //////////////////////////////////////////////////////////////////////////////////////////////////
// var dataRoot string = "/root/data/gateio/"
// var concurrencyLevel int = 50
// var biz string = "spot"
// var typ = "candlesticks_5m"
// var dataPath = filepath.Join(dataRoot, biz+"_"+typ)
// var kvstorePath string = filepath.Join(dataPath, "gokv")

var (
	dataRoot         string
	concurrencyLevel int
	biz              string
	typ              string
	dataPath         string
	kvstorePath      string
)

////////////////////////////////////////////////////////////////////////////////////////////////////

var kvStore gokv.Store

type checkpoint struct {
	LeftOpenDate   time.Time
	RightCloseDate time.Time
	LeftProbed     bool
}

func initKvstore(path string) {
	options := file.DefaultOptions
	options.Directory = path
	var err error
	kvStore, err = file.NewStore(options)
	if err != nil {
		log.Errorf("failed to create kvstore %v", err)
	}
}

func initLog() {
	// handler := slog.NewTextHandler(os.Stderr, nil)
	// log = slog.New(handler)
}

func initConfig() {
	// viper.SetConfigName("config")
	// viper.AddConfigPath(".")
	// viper.SetConfigType("yaml")

	// Check that a command line argument was provided
	if len(os.Args) != 2 {
		log.Fatalf("Please provide a config file")
	}

	configFile := os.Args[1]

	// Use the provided config file
	viper.SetConfigFile(configFile)

	viper.SetDefault("concurrencyLevel", 50)
	viper.SetDefault("biz", "spot")
	viper.SetDefault("type", "candlesticks_5m")
	viper.SetDefault("dataRoot", "/root/data/gateio/")

	err := viper.ReadInConfig()
	if err != nil {
		log.Errorf("failed to read config file %v", err)
	}

	concurrencyLevel = viper.GetInt("concurrencyLevel")
	biz = viper.GetString("biz")
	typ = viper.GetString("type")
	dataRoot = viper.GetString("dataRoot")
	dataPath = filepath.Join(dataRoot, biz+"_"+typ)
	kvstorePath = filepath.Join(dataPath, "gokv")

	log.Infof("configs: %d, %s, %s, %s, %s", concurrencyLevel, biz, typ, dataRoot, kvstorePath)
}

func producer(wg *sync.WaitGroup, jobCh chan string) {
	defer wg.Done()

	log.Info("start to execute producer")
	produceJobs(jobCh)
}

func consumer(wg *sync.WaitGroup, jobCh chan string) {
	defer wg.Done()

	for pair := range jobCh {
		//log.Errorf("start to new execute job %s", pair)
		consumeJob(pair)
	}
}

func runProducerConsumer() {
	var wg sync.WaitGroup
	jobCh := make(chan string, concurrencyLevel)

	wg.Add(concurrencyLevel + 1)

	go producer(&wg, jobCh)

	for i := 0; i < concurrencyLevel; i++ {
		go consumer(&wg, jobCh)
	}

	wg.Wait()
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type gateioPair struct {
	NameEn string `json:"name_en"`
	NameCn string `json:"name_cn"`
	Pair   string `json:"pair"`
}

type gateioPairs struct {
	Spot      []gateioPair `json:"spot"`
	Contract  []gateioPair `json:"contract"`
	Etf       []gateioPair `json:"etf"`
	Lend      []gateioPair `json:"lend"`
	Borrow    []gateioPair `json:"borrow"`
	Mortgage  []gateioPair `json:"mortgage"`
	Liquidity []gateioPair `json:"liquidity"`
}

func produceJobs(jobCh chan string) {
	url := "https://www.gate.io/json_svr/query?u=23"
	method := "POST"

	payload := strings.NewReader("type=get_all_market_pairs")

	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		return
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	var pairs gateioPairs
	json.Unmarshal(body, &pairs)

	//log.Warnf("response body: %d, %d, %d\n", resp.StatusCode, len(body), len(pairs.Spot))

	var p []gateioPair
	if biz == "spot" {
		p = pairs.Spot
	} else if biz == "futures_usdt" {
		p = pairs.Contract
	}

	for _, pair := range p {
		var cp checkpoint
		found, err := kvStore.Get(pair.Pair, &cp)
		if err != nil {
			log.Errorf("failed to get checkpoint %v", err)
			continue
		}

		if found && cp.LeftProbed {
			continue
		}

		jobCh <- pair.Pair

		log.Infof("send job %s", pair.Pair)
	}

	log.Errorf("All jobs sent")

	close(jobCh)
}

func downloadFile(client *http.Client, url, path string) (notFound bool, err error) {
	notFound = false
	log.Infof("start to download file %s", url)

	resp, err := client.Get(url)
	if err != nil {
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		notFound = true

		return
	}

	out, err := os.Create(path)

	if err != nil {
		return
	}
	defer out.Close()

	// Write data to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return
	}

	//log.Infof("File %s, %s downloaded.", path, url)

	return
}

func consumeJob(job string) {
	client := &http.Client{Timeout: 30 * time.Second}

	log.Infof("start to execute job %s", job)

	pair := job
	var cp checkpoint
	found, err := kvStore.Get(pair, &cp)
	if err != nil {
		log.Errorf("failed to get checkpoint %v", err)
		return
	}

	if !found {
		cp.LeftOpenDate = time.Date(2023, 06, 30, 0, 0, 0, 0, time.UTC)
		cp.RightCloseDate = cp.LeftOpenDate
		cp.LeftProbed = false
	}

	if cp.LeftProbed {
		// TODO then go forward.
		return
	}

	for {
		m := cp.LeftOpenDate
		date := m.Format("200601")

		//url := "https://download.gatedata.org/spot/candlesticks_1m/202304/LUNA_ETH-202304.csv.gz"
		url := fmt.Sprintf("https://download.gatedata.org/%s/%s/%s/%s-%s.csv.gz", biz, typ, date, pair, date)
		path := filepath.Join(dataPath, fmt.Sprintf("%s-%s.csv.gz", pair, date))

		notFound, err := downloadFile(client, url, path)
		if err != nil {
			log.Errorf("failed to download file %s %v", url, err)

			//re-download
			time.Sleep(5 * time.Second)
			continue
		}

		if notFound {
			cp.LeftProbed = true
			kvStore.Set(pair, cp)

			break
		}

		log.Infof("downloaded file %s", url)

		cp.LeftOpenDate = m.AddDate(0, -1, 0)
		kvStore.Set(pair, cp)
	}
}

func Init() {
	initConfig()
	initLog()
	initKvstore(kvstorePath)

	if err := os.MkdirAll(dataPath, os.ModePerm); err != nil {
		log.Errorf("failed to create directory %s, %v", dataRoot, err)
	}
	// defer kvStore.Close()
}

func main() {
	//Document: https://www.gate.tv/zh/developer/historical_quotes
	Init()

	runProducerConsumer()

	log.Info("all jobs finished")
}
