package loadfile

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
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
	processCout = 1
	MergeIDs    = NewMergeIDS()
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
	return
}

func worker[T Base, S SourceGeneric](c T, paths <-chan string, results chan<- S, wg *sync.WaitGroup, piplines []func(S)) {
	for filePath := range paths {
		f, err := os.Open(filePath)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		gr, err := gzip.NewReader(f)
		if err != nil {
			log.Fatal(err)
		}
		defer gr.Close()

		scanner := bufio.NewScanner(gr)
		for scanner.Scan() {
			var item S
			json.Unmarshal(scanner.Bytes(), &item)

			for _, fc := range piplines {
				fc(item)
			}
			results <- item
		}
		// break
	}
	wg.Done()
}

func extracting[T Base, S SourceGeneric](c T, baseChan chan S, piplines []func(S)) {

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
	wg.Add(processCout)
	for w := 0; w < processCout; w++ {
		go worker(c, pathChan, baseChan, &wg, piplines)
	}

	// consumer
	go func() {
		for item := range baseChan {
			log.Println(item)
		}
	}()

	wg.Wait()
	close(baseChan)

}

func ExtractingWork(piplines []func(mode.WorkSource)) (baseChan chan mode.WorkSource) {
	baseChan = make(chan mode.WorkSource, processCout*10)
	wk := &Work{}
	// Test(wk)
	go extracting[*Work, mode.WorkSource](wk, baseChan, piplines)
	return
}

func Test[T Base](c T) {
	// log.Println(c.ProjectName())
	iteratePath(c)
	// c.iteratePath()
}
