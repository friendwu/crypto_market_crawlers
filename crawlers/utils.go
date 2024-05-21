package main

import (
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/charmbracelet/log"
	"github.com/philippgille/gokv"
	"github.com/philippgille/gokv/file"
)

func DownloadFile(client *http.Client, url, path string) (notFound bool, err error) {
	notFound = false

	resp, err := client.Get(url)
	if err != nil {
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		notFound = true

		return
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		log.Fatalf("failed to create directory %s, %v", dir, err)
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

	return
}

func NewKvstore(path string) gokv.Store {
	options := file.DefaultOptions
	options.Directory = path
	kvStore, err := file.NewStore(options)
	if err != nil {
		log.Errorf("failed to create kvstore %v", err)
		return nil
	}

	return kvStore
}
