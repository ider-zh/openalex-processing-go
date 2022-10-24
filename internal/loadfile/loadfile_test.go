package loadfile_test

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"testing"

	// . "openalex/internal/loadfile"
	"openalex/internal/mode"

	log "github.com/sirupsen/logrus"
)

func TestLogic(t *testing.T) {
	filePath := "/home/ni/data/openAlex/data/works/updated_date=2022-08-04/part_002.gz"
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	gr, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}

	reader := bufio.NewReader(gr)

	i := 0
	for {
		i += 1
		line, err := reader.ReadString('\n')

		if err == io.EOF {
			break
		}
		var item mode.WorkSource
		err = json.Unmarshal([]byte(line), &item)
		if err != nil {
			log.Println(err, filePath, string(line))
		}

	}

	gr.Close()
	f.Close()
	log.Println("path done:", filePath, " ", i)
}

// func TestWorks(t *testing.T) {
// 	// F1 := "a1"
// 	// F2 := "a2"
// 	// var testpipLine1 = func(s mode.WorkSource) {
// 	// 	log.Println(F1, s.ID)
// 	// }

// 	// var testpipLine2 = func(s mode.WorkSource) {
// 	// 	log.Println(F2, s.ID)
// 	// }
// 	// ffc := []func(mode.WorkSource){testpipLine1, testpipLine2}
// 	// for row := range ExtractingWork() {
// 	// 	log.Info(row.ID)
// 	// }
// 	pathChan := make(chan string, 1)
// 	// baseChan := make(chan S, 10000)

// 	pathChan <- "/home/ni/data/openAlex/data/works/updated_date=2022-08-04/part_002.gz"
// 	baseChan := make(chan mode.WorkSource, 10)
// 	wg := sync.WaitGroup{}
// 	wg.Add(1)
// 	wk := &Work{}
// 	go func() {
// 		i := 0
// 		for _ = range baseChan {
// 			i++
// 		}
// 		t.Log("base count:", i)
// 	}()
// 	Worker(wk, pathChan, baseChan, &wg)
// 	wg.Wait()
// }

// func TestMergeIDS(t *testing.T) {
// 	NewMergeIDS()
// 	// mid := NewMergeIDS()
// 	// t.Log(mid.IDMAP)
// }
