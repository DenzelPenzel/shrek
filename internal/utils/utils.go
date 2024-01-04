package utils

import (
	"crypto/rand"
	"math/big"
	"net"
	"os"
	"path/filepath"
)

func PathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true // path exists
	}
	if os.IsNotExist(err) {
		return false // path does not exist
	}
	// handle other errors, e.g., permission denied
	return false
}

func FindDirectory(currentDir, target string) string {
	for {
		if _, err := os.Stat(filepath.Join(currentDir, target)); err == nil {
			return currentDir
		}
		parentDir := filepath.Dir(currentDir)
		if parentDir == currentDir {
			break
		}
		currentDir = parentDir
	}

	return currentDir
}

func RandomString(n int) string {
	var alphanumerics = []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	s := make([]rune, n)
	for i := range s {
		randomIndex, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphanumerics))))
		if err != nil {
			return ""
		}
		s[i] = alphanumerics[randomIndex.Int64()]
	}
	return string(s)
}

func GetTCPAddr(addr string) (*net.TCPAddr, error) {
	res, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func TempDir(name string) string {
	path, err := os.MkdirTemp("", name)
	if err != nil {
		panic("failed to create temp dir")
	}
	return path
}
