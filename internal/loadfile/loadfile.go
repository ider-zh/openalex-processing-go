package loadfile

// 多线程读文件，但是只能同时读一天的文件，
// 从新到旧的读取文件，已经读取的文件 ID， 最后一次性保存的 Set, 如果已经在 Set 中了， 就不再保存，实现读写分离
// 一天内的文件多线程同时读取，不同天的文件重新到旧的读取

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
	"sort"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
)

var (
	basicPath   = "/home/ni/data/openalex-snapshot/data"
	processCout = 10
	DocSumCount int64
)

type Base interface {
	ProjectName() string
}

type SourceGeneric interface {
	mode.WorkSource | mode.BaseSource
}

type MongoGeneric interface {
	mode.WorkMongo
}

type Work struct {
}

type byModTime []os.FileInfo

func (fis byModTime) Len() int {
	return len(fis)
}

func (fis byModTime) Swap(i, j int) {
	fis[i], fis[j] = fis[j], fis[i]
}

func (fis byModTime) Less(i, j int) bool {
	// return fis[i].ModTime().Before(fis[j].ModTime())
	return fis[i].Name() > fis[j].Name()
}

func (c *Work) ProjectName() string {
	return "works"
}

func iteratePath[T Base](c T) (retPathStrs []string) {
	rootPath := filepath.Join(basicPath, c.ProjectName())

	// sort by name
	f, _ := os.Open(rootPath)
	fis, _ := f.Readdir(-1)
	f.Close()
	sort.Sort(byModTime(fis))

	fileCount := 0
	for _, fi := range fis {
		filepath.WalkDir(filepath.Join(rootPath, fi.Name()), func(path string, d fs.DirEntry, err error) error {
			if strings.HasSuffix(path, ".gz") {
				fileCount += 1
				retPathStrs = append(retPathStrs, path)
			}
			return err
		})
	}
	fmt.Printf("files count: %v\n", fileCount)

	return
}

func worker[T Base, S SourceGeneric](c T, paths <-chan string, results chan<- *S, wg *sync.WaitGroup) {
	for filePath := range paths {
		dirName := filepath.Dir(filePath)
		// only keep date
		dirName = dirName[len(dirName)-10:]
		log.Infof("start extracting file: %v \tparent:%v\n", filePath, dirName)
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

			results <- &item
		}

		gr.Close()
		f.Close()
		// log.Println("path done:", filePath, " ", i)
	}
	wg.Done()
}

func extracting[T Base, S SourceGeneric](c T, baseChan chan *S) {

	// 10000 is enough size
	pathChan := make(chan string, 1000)
	// baseChan := make(chan S, 10000)
	// producter
	for _, pathItem := range iteratePath(c) {
		pathChan <- pathItem

	}
	close(pathChan)
	// worker
	wg := sync.WaitGroup{}
	for w := 0; w < processCout; w++ {
		wg.Add(1)
		go worker(c, pathChan, baseChan, &wg)
	}

	wg.Wait()
	// consumer
	// go func() {
	// 	for item := range baseChan {
	// 		log.Println(item)
	// 	}
	// }()
	close(baseChan)

}

func ExtractingWork() (baseChan chan *mode.WorkSource) {
	baseChan = make(chan *mode.WorkSource, processCout*10)
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
