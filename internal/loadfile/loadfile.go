package loadfile

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"openalex/internal/mode"
	"os"
	"path/filepath"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
)

var (
	basicPath   = "/home/ni/data/openAlex/data"
	processCout = 30
	DocSumCount int64
)

type Base interface {
	ProjectName() string
}

type SourceGeneric interface {
	mode.WorkSource
}

type MongoGeneric interface {
	mode.WorkMongo
}

type Work struct {
}

func (c *Work) ProjectName() string {
	return "works"
}

func iteratePath[T Base](c T) (retPathStrs []string) {
	rootPath := filepath.Join(basicPath, c.ProjectName())
	filepath.WalkDir(rootPath, func(path string, d fs.DirEntry, err error) error {
		if strings.HasSuffix(path, ".gz") {
			retPathStrs = append(retPathStrs, path)
		}
		return err
	})
	// log.Println(retPathStrs)
	fmt.Printf("files count: %v\n", len(retPathStrs))
	return
}

func worker[T Base, S SourceGeneric](c T, paths <-chan string, results chan<- S, wg *sync.WaitGroup) {
	for filePath := range paths {
		f, err := os.Open(filePath)
		if err != nil {
			log.Fatal(err)
		}
		gr, err := gzip.NewReader(f)
		if err != nil {
			log.Fatal(err)
		}

		scanner := bufio.NewReader(gr)
		i := 0
		for {
			i += 1
			line, err := scanner.ReadString('\n')

			if err == io.EOF {
				break
			}
			var item S
			err = json.Unmarshal([]byte(line), &item)
			if err != nil {
				log.Println(err, filePath, string(line))
			}
			results <- item
		}

		gr.Close()
		f.Close()
		// log.Println("path done:", filePath, " ", i)
	}
	wg.Done()
}

func extracting[T Base, S SourceGeneric](c T, baseChan chan S) {

	// 99999 is enough size
	pathChan := make(chan string, 99999)
	// baseChan := make(chan S, 10000)

	// producter
	for _, path := range iteratePath(c) {
		pathChan <- path
	}
	close(pathChan)

	// worker
	wg := sync.WaitGroup{}
	for w := 0; w < processCout; w++ {
		wg.Add(1)
		go worker(c, pathChan, baseChan, &wg)
	}

	// consumer
	// go func() {
	// 	for item := range baseChan {
	// 		log.Println(item)
	// 	}
	// }()

	wg.Wait()
	close(baseChan)

}

func ExtractingWork() (baseChan chan mode.WorkSource) {
	baseChan = make(chan mode.WorkSource, processCout*10)
	wk := &Work{}
	// Test(wk)
	go extracting[*Work, mode.WorkSource](wk, baseChan)
	return
}

func Test[T Base](c T) {
	// log.Println(c.ProjectName())
	iteratePath(c)
	// c.iteratePath()
}
