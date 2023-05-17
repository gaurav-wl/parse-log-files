package main

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
)

type Job struct {
	Chunk []byte
	Size  int
}

func fetchUniqueIPs(logChunk []byte, ips *sync.Map) {
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

	// filePartitionChunk: 128 KB
	const filePartitionChunk = 128 << 10

	Ips := new(sync.Map)
	wg := new(sync.WaitGroup)
	jobs := make(chan Job, 10)

	numWorkers := 4 // use worker as per your system capacity
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go processJob(jobs, wg, Ips)
	}

	go func() {
		defer close(jobs)
		for {
			fileBuffer := make([]byte, filePartitionChunk)
			bytesRead, err := file.Read(fileBuffer)
			if err != nil {
				break
			}
			jobs <- Job{Chunk: fileBuffer, Size: bytesRead}
		}
	}()

	wg.Wait()

	// Convert set to slice
	Ips.Range(func(key, value interface{}) bool {
		fmt.Println(key)
		return true
	})

	return nil
}

func processJob(jobs <-chan Job, wg *sync.WaitGroup, ips *sync.Map) {
	defer wg.Done()
	for job := range jobs {
		fetchUniqueIPs(job.Chunk[:job.Size], ips)
	}
}

func main() {
	err := printUniqueIpsFromLogFile("logs.json")
	if err != nil {
		log.Fatal(err)
	}
}
