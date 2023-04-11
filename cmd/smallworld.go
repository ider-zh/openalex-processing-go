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
	"openalex/internal/grpc/graphtool"
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
	"google.golang.org/grpc"
)

// smallworldCmd represents the smallworld command
var smallworldCmd = &cobra.Command{
	Use:   "smallworld",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("smallworld called")
		process_smallworld()
	},
}

func init() {
	rootCmd.AddCommand(smallworldCmd)
}

func smallworld_test() {
	mainPath, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}
	log.Info(mainPath)
}

func process_smallworld() {
	GRPC_URI := "192.168.50.3:50052"

	type smallWorldStats struct {
		EdgeCount     int64
		NodeCount     int64
		PathCount     int64
		DistanceCount int64
		ASD           float64
		CC            float64
		Frequency     [][]int64
	}

	ResultPath := "/tmp/smallworld"

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
	get_openalex_subject := func(filePath string) (subject_year [][]int64) {

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

	fileExists := func(filename string) bool {
		_, err := os.Stat(filename)
		if os.IsNotExist(err) {
			return false
		}
		return true
	}

	rpcProcess := func(in *graphtool.Graph) *graphtool.GraphStats {
		conn, err := grpc.Dial(GRPC_URI, grpc.WithInsecure())
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close()
		client := graphtool.NewDirectGraphDistanceServiceClient(conn)
		// .NewDirectGraphDistanceServiceClient(c.conn)
		// 通过编译rpc.pb.go得到的Hello服务来发送数据类型为String的数据
		// edges := []*pb.Edge{{SourceID: 1, TargetID: 2}, {SourceID: 2, TargetID: 3}}
		// for i := 0; i < 10000; i++ {
		// 	edges = append(edges, &pb.Edge{SourceID: int64(i), TargetID: int64(i + 1)})
		// }
		// stream, err := client.DirectGraphDistance(context.Background(), &pb.EdgeArray{Edges: edges}, maxSizeOption)
		result, err := client.DirectGraphDistance(context.Background(), in, grpc.MaxCallRecvMsgSize(1024*1024*1024))
		if err != nil {
			log.Fatal(err)
		}
		return result
	}

	statsFolderPath := fmt.Sprintf("%v/stats", ResultPath)
	ensureDir(statsFolderPath)

	wg := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {

			for path := range filePaths {
				var swStats = smallWorldStats{}

				fileName := filepath.Base(path)
				fileNameWithoutExt := fileName[:len(fileName)-len(filepath.Ext(fileName))]
				newFileName := strings.ReplaceAll(strings.ToLower(fileNameWithoutExt), " ", "_")
				log.Println("start", newFileName)

				IDYearList := get_openalex_subject(path)
				for year := 1900; year <= 2022; year++ {

					smallWorldJsonFile := fmt.Sprintf("%v/%v_%v.json", statsFolderPath, newFileName, year)
					// The existing file indicates that the small world has already calculated, skipping this calculation
					if fileExists(smallWorldJsonFile) {
						continue
					}
					log.Info("start:", fileName, year)

					IDSet := mapset.NewSet[int64]()
					for _, row := range IDYearList {
						if row[1] <= int64(year) {
							IDSet.Add(row[0])
						}
					}
					if IDSet.Cardinality() > 500000 {
						log.Info("subject is large than 500000, over", newFileName)
						break
					}
					edgeGraph := graphtool.Graph{}
					for ID := range IDSet.Iter() {
						if node, ok := WorkPool[ID]; ok {
							for _, outID := range node.Out {
								if IDSet.Contains(outID) {
									edgeGraph.Edges = append(edgeGraph.Edges, &graphtool.Edge{SourceID: ID, TargetID: outID})
								}
							}
						}
					}
					result := rpcProcess(&edgeGraph)

					swStats.EdgeCount = result.EdgeCount
					swStats.NodeCount = result.NodeCount
					swStats.ASD = result.ASD
					swStats.CC = result.CC
					swStats.DistanceCount = result.DistanceCount
					swStats.PathCount = result.PathCount

					for _, row := range result.Frequency {
						swStats.Frequency = append(swStats.Frequency, row.Values)
					}
					file, _ := json.MarshalIndent(swStats, "", " ")
					_ = ioutil.WriteFile(smallWorldJsonFile, file, 0644)
				}
				log.Println("finish", newFileName)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	log.Info("over")

}
