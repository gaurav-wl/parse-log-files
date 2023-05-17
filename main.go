package main

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
)

func fetchUniqueIPs(logChunk []byte, wg *sync.WaitGroup, ips *sync.Map) {
	defer wg.Done()
	ipRegex := regexp.MustCompile(`host=([^\s]+)`)

	allMatched := ipRegex.FindAllString(string(logChunk), -1)
	for _, match := range allMatched {
		ip := strings.Trim(match, "host=")
		if _, ok := ips.Load(ip); !ok {
			ips.Store(ip, true)
		}
	}
}

func printUniqueIpsFromLogFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error open log file %v", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Println("error closing file")
		}
	}()

	// filePartitionChunk: 256 KB
	const filePartitionChunk = 256 << 10

	Ips := new(sync.Map)

	wg := new(sync.WaitGroup)
	for {
		fileBuffer := make([]byte, filePartitionChunk)
		bytesRead, err := file.Read(fileBuffer)
		if err != nil {
			break
		}
		wg.Add(1)
		go fetchUniqueIPs(fileBuffer[:bytesRead], wg, Ips)
	}

	wg.Wait()

	// Convert set to slice
	Ips.Range(func(key, value interface{}) bool {
		fmt.Println(key)
		return true // return false to stop the iteration
	})

	return nil
}

func main() {
	err := printUniqueIpsFromLogFile("logs.json")
	if err != nil {
		log.Fatal(err)
	}
}
