package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"net/http"
	"strings"

	"github.com/charmbracelet/log"
	"github.com/philippgille/gokv"
	//"golang.org/x/exp/slices"
)

type GateioConfig struct {
	DataRoot         string `yaml:"dataRoot"`
	ConcurrencyLevel int    `yaml:"concurrencyLevel"`
	Biz              string `yaml:"biz"`
	Typ              string `yaml:"type"`
}

type GateioCheckpoint struct {
	LeftOpenDate   time.Time
	RightCloseDate time.Time
	LeftProbed     bool
}

type GateioContext struct {
	kvStore  gokv.Store
	dataPath string
	biz      string
	typ      string
}

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

func gateioInitCallback(cfg interface{}) interface{} {
	config := cfg.(*GateioConfig)

	if config.DataRoot == "" || config.ConcurrencyLevel == 0 || config.Biz == "" || config.Typ == "" {
		log.Fatalf("invalid config")
	}

	// if !slices.Contains(BIZS, config.Biz) || !slices.Contains(TYPES, config.Typ) {
	// 	log.Fatalf("invalid config")
	// }

	dataPath := filepath.Join(config.DataRoot, config.Biz+"_"+config.Typ)
	kvstorePath := filepath.Join(dataPath, "gokv")

	if err := os.MkdirAll(dataPath, os.ModePerm); err != nil {
		log.Fatalf("failed to create directory %s, %v", dataPath, err)
	}

	return &GateioContext{
		kvStore:  NewKvstore(kvstorePath),
		biz:      config.Biz,
		typ:      config.Typ,
		dataPath: dataPath,
	}
}

func gateioProducerCallback(c interface{}, jobCh chan interface{}) {
	context := c.(*GateioContext)

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
	if context.biz == "spot" {
		p = pairs.Spot
	} else if context.biz == "futures_usdt" {
		p = pairs.Contract
	}

	for _, pair := range p {
		var cp GateioCheckpoint
		found, err := context.kvStore.Get(pair.Pair, &cp)
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

	return
}

func gateioConsumerCallback(context interface{}, jobCh chan interface{}) {
	c := context.(*GateioContext)
	for job := range jobCh {
		gateioConsumeJob(c, job.(string))
	}
}

func gateioConsumeJob(context *GateioContext, pair string) {
	client := &http.Client{Timeout: 30 * time.Second}

	log.Infof("start to execute job %s", pair)

	var cp BinanceCheckpoint
	found, err := context.kvStore.Get(pair, &cp)
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
		url := fmt.Sprintf("https://download.gatedata.org/%s/%s/%s/%s-%s.csv.gz", context.biz, context.typ, date, pair, date)
		path := filepath.Join(context.dataPath, fmt.Sprintf("%s-%s.csv.gz", pair, date))

		notFound, err := DownloadFile(client, url, path)
		if err != nil {
			log.Errorf("failed to download file %s %v", url, err)

			//re-download
			time.Sleep(5 * time.Second)
			continue
		}

		if notFound {
			cp.LeftProbed = true
			context.kvStore.Set(pair, cp)

			break
		}

		log.Infof("downloaded file %s", url)

		cp.LeftOpenDate = m.AddDate(0, -1, 0)
		context.kvStore.Set(pair, cp)
	}
}
