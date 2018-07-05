package main

import (
	"github.com/spf13/viper"
	"os"
	"time"
	"github.com/go-redis/redis"
	"github.com/larrabee/ftp"
	"gopkg.in/Graylog2/go-gelf.v2/gelf"
	"fmt"
)


type FtpEntry struct {
	Path string
	Type ftp.EntryType
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