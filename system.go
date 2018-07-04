package main

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
)

func newTempFile(dir, prefix, suffix string) (*os.File, error) {
Start:
	randBytes := make([]byte, 64)
	rand.Read(randBytes)
	path := filepath.Join(dir, prefix+hex.EncodeToString(randBytes)+suffix)
	if _, err := os.Stat(path); os.IsNotExist(err) == false {
		goto Start
	}
	file, err := os.Create(path)
	return file, err
}
