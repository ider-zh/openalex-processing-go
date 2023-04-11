/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"openalex/internal/graph"
	"openalex/internal/mode"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	mapset "github.com/deckarep/golang-set/v2"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// entropyCmd represents the entropy command
var entropyCmd = &cobra.Command{
	Use:   "entropy",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("entropy called")
		process_entropy()
	},
}

func init() {
	rootCmd.AddCommand(entropyCmd)
}

func entropy_test() {
	mainPath, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}
	log.Info(mainPath)
}

func process_entropy() {
	type dumpDegreeEntropy struct {
		InE         []float64
		OutE        []float64
		UndirectedE []float64
	}

	type dumpStructEntropy struct {
		InE              []float64
		OutE             []float64
		UndirectedE      []float64
		InSE             []float64
		OutSE            []float64
		UndirectedSE     []float64
		InLength         []int
		OutLength        []int
		UndirectedLength []int
	}

	ResultPath := "/tmp/entropy"

	WorkPool := make(map[int64]*mode.WorkMongo)
	{
		Client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://knogen:knogen@127.0.0.1:27017"))
		ctx := context.Background()
		err = Client.Connect(ctx)
		if err != nil {
			log.Panic("mongo connect fail")
		}

		collection := Client.Database("openalex").Collection("works")
		filter := bson.M{
			"$or": []bson.M{
				bson.M{"in": bson.M{"$exists": true}},
				bson.M{"out": bson.M{"$exists": true}},
			},
		}
		cursor, err := collection.Find(ctx, filter, options.Find().SetProjection(bson.M{"_id": 1, "in": 1, "year": 1, "out": 1}))
		if err != nil {
			log.Fatal(err)
		}
		log.Info("start load db")

		for cursor.Next(ctx) {
			var item mode.WorkMongo
			if err = cursor.Decode(&item); err != nil {
				log.Fatal(err)
			}
			if len(item.In) == 0 && len(item.Out) == 0 {
				continue
			}
			WorkPool[item.ID] = &item
		}
		log.Info("finish load db")
		log.Info("WorkPool size:", len(WorkPool))
	}

	folderPath := "/home/ni/data/openAlex/ref_field_nobook_notypenull_all/"

	// var filepaths []string

	filePaths := make(chan string, 1000)
	{
		// iterate over all files in the folder
		err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
			// check if the current path is a file
			if !info.IsDir() {
				// get the file name
				// fileName := info.Name()
				// filepaths = append(filepaths, path)
				filePaths <- path

				// fileName := filepath.Base(path)
				// fileNameWithoutExt := fileName[:len(fileName)-len(filepath.Ext(fileName))]
				// newFileName := strings.ReplaceAll(strings.ToLower(fileNameWithoutExt), " ", "_")
				// fmt.Println(newFileName)
			}
			return nil
		})
		if err != nil {
			log.Println(err)
		}
	}
	log.Info("Start to calculate disruption")
	close(filePaths)
	get_entropy_subject := func(filePath string) (subject_year [][]int64) {

		file, err := os.Open(filePath)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		// Create a new CSV reader
		reader := csv.NewReader(file)
		reader.Comma = '\t'
		// Set the FieldsPerRecord field to a large value
		reader.LazyQuotes = true
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal(err)
			}

			ID, err := strconv.ParseInt(strings.TrimPrefix(record[0], "W"), 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			year, err := strconv.ParseInt(record[1], 10, 32)
			if err != nil {
				log.Fatal(err)
			}
			subject_year = append(subject_year, []int64{ID, year})
		}
		return subject_year
	}
	ensureDir := func(folderPath string) {
		if _, err := os.Stat(folderPath); os.IsNotExist(err) {
			err := os.MkdirAll(folderPath, 0755)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	wg := sync.WaitGroup{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			for path := range filePaths {
				var dumpMiniDegreeEntropyObject = dumpDegreeEntropy{}
				var dumpMiniStructEntropyObject = dumpStructEntropy{}

				fileName := filepath.Base(path)
				fileNameWithoutExt := fileName[:len(fileName)-len(filepath.Ext(fileName))]
				newFileName := strings.ReplaceAll(strings.ToLower(fileNameWithoutExt), " ", "_")
				log.Println("start", newFileName)

				IDYearList := get_entropy_subject(path)
				for year := 1900; year <= 2022; year++ {
					log.Info("start:", fileName, year)
					IDSet := mapset.NewSet[int64]()
					for _, row := range IDYearList {
						if row[1] <= int64(year) {
							IDSet.Add(row[0])
						}
					}

					edgeChan := make(chan graph.Edge, 1000)
					var graphNodeRetChan = make(chan []*graph.NodeLink)
					go graph.EdgesToGraphByChan(edgeChan, graphNodeRetChan)
					for ID := range IDSet.Iter() {
						if node, ok := WorkPool[ID]; ok {
							for _, outID := range node.Out {
								if IDSet.Contains(outID) {
									edgeChan <- graph.Edge{S: ID, D: outID}
								}
							}
						}
					}
					close(edgeChan)
					graphNodeDetail := <-graphNodeRetChan

					gp := graph.GraphProcess{Node: graphNodeDetail}
					retA := gp.GetDegreeEntropy()

					dumpMiniDegreeEntropyObject.InE = append(dumpMiniDegreeEntropyObject.InE, retA.InE)
					dumpMiniDegreeEntropyObject.OutE = append(dumpMiniDegreeEntropyObject.OutE, retA.OutE)
					dumpMiniDegreeEntropyObject.UndirectedE = append(dumpMiniDegreeEntropyObject.UndirectedE, retA.UndirectedE)

					retB := gp.GetStructEntropy()
					dumpMiniStructEntropyObject.InE = append(dumpMiniStructEntropyObject.InE, retB.InE)
					dumpMiniStructEntropyObject.OutE = append(dumpMiniStructEntropyObject.OutE, retB.OutE)
					dumpMiniStructEntropyObject.UndirectedE = append(dumpMiniStructEntropyObject.UndirectedE, retB.UndirectedE)
					dumpMiniStructEntropyObject.InSE = append(dumpMiniStructEntropyObject.InSE, retB.InSE)
					dumpMiniStructEntropyObject.OutSE = append(dumpMiniStructEntropyObject.OutSE, retB.OutSE)
					dumpMiniStructEntropyObject.UndirectedSE = append(dumpMiniStructEntropyObject.UndirectedSE, retB.UndirectedSE)
					dumpMiniStructEntropyObject.InLength = append(dumpMiniStructEntropyObject.InLength, retB.InLength)
					dumpMiniStructEntropyObject.OutLength = append(dumpMiniStructEntropyObject.OutLength, retB.OutLength)
					dumpMiniStructEntropyObject.UndirectedLength = append(dumpMiniStructEntropyObject.UndirectedLength, retB.UndirectedLength)
				}

				degreeEntropyFolderPath := fmt.Sprintf("%v/degreeEntropy", ResultPath)
				ensureDir(degreeEntropyFolderPath)
				file, _ := json.MarshalIndent(dumpMiniDegreeEntropyObject, "", " ")
				_ = ioutil.WriteFile(fmt.Sprintf("%v/%v.json", degreeEntropyFolderPath, newFileName), file, 0644)

				structEntropyFolderPath := fmt.Sprintf("%v/structEntropy/", ResultPath)
				ensureDir(structEntropyFolderPath)
				file, _ = json.MarshalIndent(dumpMiniStructEntropyObject, "", " ")
				_ = ioutil.WriteFile(fmt.Sprintf("%v/%v.json", structEntropyFolderPath, newFileName), file, 0644)
				log.Println("finish", newFileName)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	log.Info("over")

}
