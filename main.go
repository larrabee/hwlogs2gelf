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

func getConfig(configPath string) (*viper.Viper, error) {
	var v = viper.New()
	v.SetConfigType("yaml")
	file, err1 := os.Open(configPath)
	defer file.Close()
	if err1 != nil {
		return nil, err1
	}
	err2 := v.ReadConfig(file)
	return v, err2
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
		//processQueue <- FtpEntry{Path: "/cds/2018/07/01/cds_20180701-000000-11544413003dc1.log.gz", Type: ftp.EntryTypeFile}

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

type FtpEntry struct {
	Path string
	Type ftp.EntryType
}

func getFTPConnection(site string, config *viper.Viper) (*ftp.ServerConn, error) {
	ftpConn, err := ftp.DialTimeout(config.GetString("ftp.conn"), time.Second*config.GetDuration("ftp.timeout"))
	if err != nil {
		return ftpConn, err
	}

	ftpLogin := config.GetString("ftp.username") + ":" + site
	err = ftpConn.Login(ftpLogin, config.GetString("ftp.password"))
	if err != nil {
		return ftpConn, err
	}
	return ftpConn, err
}

func getRedisConnection(config *viper.Viper) *redis.Client {
	redisConn := redis.NewClient(&redis.Options{
		Addr:     config.GetString("redis.conn"),
		Password: config.GetString("redis.password"),
		DB:       config.GetInt("redis.db"),
	})
	return redisConn
}

func listFTPRecursive(path string, conn *ftp.ServerConn) ([]FtpEntry, error) {
	entries, err := conn.List(path)
	entriesRef := make([]FtpEntry, len(entries))
	if err != nil {
		return entriesRef, err
	}
	for i, entry := range entries {
		entriesRef[i].Path = path + "/" + entry.Name
		entriesRef[i].Type = entry.Type
	}

	for _, entry := range entriesRef {
		if entry.Type == ftp.EntryTypeFolder {
			nestedEntries, err := listFTPRecursive(entry.Path, conn)
			if err != nil {
				return nestedEntries, err
			}
			entriesRef = append(entriesRef, nestedEntries...)
		}
	}
	return entriesRef, nil
}

func ProcessFilesWorker(input <-chan FtpEntry, output chan<- gelf.Message, site string, config *viper.Viper, wg *sync.WaitGroup) {
	ftpConn, err := getFTPConnection(site, config)
	if err != nil {
		log.Errorf("FTP Connect error: %s", err)
		wg.Done()
		return
	}
	redisConn := getRedisConnection(config)

Main:
	for entry := range input {
		resp, err := ftpConn.Retr(entry.Path)
		if err != nil {
			log.Errorf("File %s retr request failed with error: %s", entry.Path, err)
			resp.Close()
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
		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				gzReader.Close()
				resp.Close()
				break
			}
			if err != nil {
				log.Errorf("CSV parsing failed with error: %s", err)
				gzReader.Close()
				resp.Close()
				continue Main
			}
			message, err := formatMessage(record, site, config)
			if err != nil {
				log.Warnf("Cannot parse message, error: %s", err)
				continue
			}
			output<- message
		}


		err = redisConn.Set(site+entry.Path, "done", 0).Err()
		if err != nil {
			log.Errorf("Setting redis key failed with error: %s", err)
			continue
		} else {
			log.Infof("File '%s' on site '%s' parsed successful", entry.Path, site)
		}
	}
	wg.Done()
}

func formatMessage(record []string, site string, config *viper.Viper) (gelfMessage gelf.Message, err error) {
	pTime, err := time.Parse(time.RFC3339, fmt.Sprintf("%sT%sZ", record[0], record[1]))
	if err != nil {
		return
	}
	gelfMessage.Version = "1.1"
	gelfMessage.Level = 1
	gelfMessage.Host = record[10]
	gelfMessage.TimeUnix = float64(pTime.Unix())
	gelfMessage.Short = record[14]
	gelfMessage.Extra = map[string]interface{}{
		"method": record[2],
		"remote": record[3],
		"scheme": record[4],
		"referer": record[5],
		"agent": record[6],
		"size": record[7],
		"req_size": record[8],
		"res_size": record[9],
		"request_time": record[11],
		"code": record[12],
		"parameters": record[13],
		"path_without_parameters": record[14],
		"tag": config.GetString("gelf.tag"),
		"site_id": site,
		"site_name": config.GetString(fmt.Sprintf("sites.%s.name", site)),
	}

	return gelfMessage, nil
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
		err := gelfWriter.WriteMessage(&message)
		if err != nil {
			log.Errorf("Gelf message sending failed with error: %s", err)
		}
	}
}
