package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

const totalFile = 3000
const contentLength = 5000

var tmpPath = "random-files"

type FileInfo struct {
	Index       int
	FileName    string
	WorkerIndex int
	Err         error
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	log.Println("start")
	start := time.Now()

	generateFiles()

	duration := time.Since(start)
	log.Println("done in", duration.Seconds(), "seconds")

}

func randomString(length int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	result := make([]rune, length)
	for i := range result {
		result[i] = letters[rand.Intn(len(letters))]
	}
	return string(result)
}

func generateFiles() {
	os.RemoveAll(tmpPath)
	os.MkdirAll(tmpPath, os.ModePerm)

	// pipeline 1: job distribution
	// Fan-out
	chanFileIndex := generateFileIndexes()

	// pipeline 2: the main logic (creating files)
	workers := runtime.NumCPU()
	// Fan-out Fan-in
	chanFileResult := createFiles(chanFileIndex, workers)

	// track and print output
	counterTotal := 0
	counterSuccess := 0

	for fileResult := range chanFileResult {
		if fileResult.Err != nil {
			log.Printf("error creating file %s. stack trace: %s", fileResult.FileName, fileResult.Err)
		} else {
			counterSuccess++
		}

		counterTotal++
	}

	log.Printf("%d/%d of total files created", counterSuccess, counterTotal)
}

func generateFileIndexes() <-chan FileInfo {
	chOut := make(chan FileInfo)

	go func() {
		for i := 0; i < totalFile; i++ {
			chOut <- FileInfo{
				Index:    i,
				FileName: fmt.Sprintf("file-%d.txt", i),
			}
		}
		close(chOut)
	}()

	return chOut
}

func createFiles(chIn <-chan FileInfo, numberOfWorkers int) <-chan FileInfo {
	chOut := make(chan FileInfo)

	// wait group to control the workers
	wg := new(sync.WaitGroup)

	// allocate N of workers
	wg.Add(numberOfWorkers)

	go func() {
		// dispatch N workers
		for worker := 0; worker < numberOfWorkers; worker++ {
			go func(worker int) {
				// listen to `chIn` channel for incoming jobs
				for job := range chIn {
					filePath := filepath.Join(tmpPath, job.FileName)
					content := randomString(contentLength)
					err := ioutil.WriteFile(filePath, []byte(content), os.ModePerm)
					log.Println("worker", worker, "working on", job.FileName, "file generation")
					// construct the job's result, and send it to `chOut`
					chOut <- FileInfo{
						FileName:    job.FileName,
						WorkerIndex: worker,
						Err:         err,
					}
				}

				// if `chIn` is closed, and the remaining jobs are finished,
				// only then we mark the worker as complete.
				wg.Done()

			}(worker)

		}
	}()

	// wait until `chIn` closed and then all workers are done,
	// because right after that - we need to close the `chOut` channel.
	go func() {
		wg.Wait()
		close(chOut)
	}()

	return chOut
}
