package main

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/larrabee/ftp"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"io"
	"os"
	"strings"
	"sync"
	"time"
	"gopkg.in/Graylog2/go-gelf.v2/gelf"
	"runtime"
)

var log = logrus.New()

func main() {
	configPath := os.Args[1]
	config, err := getConfig(configPath)
	if err != nil {
		log.Fatalf("Config opening failed with err: %s", err)
	}

	switch strings.ToUpper(config.GetString("logging.level")) {
	case "DEBUG":
		log.SetLevel(logrus.DebugLevel)
	case "INFO":
		log.SetLevel(logrus.InfoLevel)
	case "WARN":
		log.SetLevel(logrus.WarnLevel)
	case "ERROR":
		log.SetLevel(logrus.ErrorLevel)
	case "FATAL":
		log.SetLevel(logrus.FatalLevel)
	default:
		log.SetLevel(logrus.InfoLevel)
	}
	gelfOutputQueue := make(chan gelf.Message, config.GetInt("gelf.workers")*2)
	for i := 0; i < config.GetInt("gelf.workers"); i++ {
		go GelfOutputWorker(gelfOutputQueue, config)
	}

	for site := range config.GetStringMapString("sites") {
		go LogFilesWatcher(site, config, gelfOutputQueue)
	}

	for {
		runtime.Gosched()
	}
}

func LogFilesWatcher(site string, config *viper.Viper, output chan<- gelf.Message) {
	for {
		time.Sleep(config.GetDuration("ftp.sleep_interval") * time.Second)

		redisConn := getRedisConnection(config)

		ftpConn, err := getFTPConnection(site, config)
		if err != nil {
			log.Errorf("FTP Connect error: %s", err)
			continue
		}

		workersCnt := config.GetInt(fmt.Sprintf("sites.%s.workers", site))
		processQueue := make(chan FtpEntry, workersCnt)
		wg := sync.WaitGroup{}
		for i := 0; i < workersCnt; i++ {
			wg.Add(1)
			go ProcessFilesWorker(processQueue, output, site, config, &wg)
		}

		files, err := listFTPRecursive("", ftpConn)
		if err != nil {
			log.Errorf("FTP Listing error: %s", err)
			continue
		}

		for _, file := range files {
			if file.Type == ftp.EntryTypeFile {
				_, err := redisConn.Get(site + file.Path).Result()
				if (err != nil) && (err != redis.Nil) {
					log.Errorf("Redis request failed with error: %s", err)
					continue
				}
				if err == redis.Nil {
					log.Debugf("Processing file: '%s' on site: '%s'", file.Path, site)
					processQueue <- file
				}
			}
		}
		close(processQueue)
		wg.Wait()
	}
}


func ProcessFilesWorker(input <-chan FtpEntry, output chan<- gelf.Message, site string, config *viper.Viper, wg *sync.WaitGroup) {
	Start:
	ftpConn, err := getFTPConnection(site, config)
	if err != nil {
		log.Errorf("FTP Connect error: %s", err)
		goto Start
	}
	redisConn := getRedisConnection(config)

	for entry := range input {
		resp, err := ftpConn.Retr(entry.Path)
		if err != nil {
			log.Errorf("File %s retr request failed with error: %s", entry.Path, err)
			continue
		}

		gzReader, err := gzip.NewReader(resp)
		if err != nil {
			log.Errorf("Gzip file opening failed with error: %s", err)
			resp.Close()
			continue
		}

		csvReader := csv.NewReader(gzReader)
		csvReader.Comma = '\t'
		csvReader.Comment = '#'
		csvReader.LazyQuotes = true
		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				gzReader.Close()
				resp.Close()
				break
			}
			if err != nil {
				log.Errorf("CSV parsing failed with error: %s", err)
				continue
			}
			message, err := formatMessage(record, site, config)
			if err != nil {
				log.Warnf("Cannot parse message, error: %s", err)
				continue
			}
			output<- message
		}
		gzReader.Close()
		resp.Close()


		err = redisConn.Set(site+entry.Path, "done", time.Second*86400*180).Err()
		if err != nil {
			log.Errorf("Setting redis key failed with error: %s", err)
			continue
		} else {
			log.Infof("File '%s' on site '%s' parsed successful", entry.Path, site)
		}
	}
	wg.Done()
}


func GelfOutputWorker(input <-chan gelf.Message, config *viper.Viper) {
	var gelfWriter gelf.Writer
	var err error
	switch strings.ToUpper(config.GetString("gelf.proto")) {
	case "TCP":
		gelfWriter, err = gelf.NewTCPWriter(config.GetString("gelf.conn"))
	default:
		gelfWriter, err = gelf.NewUDPWriter(config.GetString("gelf.conn"))
	}
	if err != nil {
		return
	}
	for message := range input {
		log.Debugf("%+v\n", message)
		err := gelfWriter.WriteMessage(&message)
		if err != nil {
			log.Errorf("Gelf message sending failed with error: %s", err)
		}
	}
}
