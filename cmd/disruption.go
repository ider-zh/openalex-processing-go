/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"fmt"
	"openalex/internal/mode"
	"sync"

	"github.com/emirpasic/gods/sets/hashset"
	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// disruptionCmd represents the disruption command
var disruptionCmd = &cobra.Command{
	Use:   "disruption",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("disruption called")
		process_disruption()
	},
}

func init() {
	rootCmd.AddCommand(disruptionCmd)
}

func process_disruption() {
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

	WorkPool := make(map[int64]*mode.WorkMongo)
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

	log.Info("Start to calculate disruption")

	IDChan := make(chan int64, 1000)
	ResultChan := make(chan mode.DisruptionType, 1000)

	// result
	resultWg := sync.WaitGroup{}
	resultWg.Add(1)
	go func() {
		Client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://knogen:knogen@127.0.0.1:27017"))
		ctx := context.Background()
		err = Client.Connect(ctx)
		if err != nil {
			log.Panic("mongo connect fail")
		}
		collection := Client.Database("openalex").Collection("works_disruption")

		opts := options.BulkWrite().SetOrdered(false)
		models := []mongo.WriteModel{}
		for item := range ResultChan {
			models = append(models, mongo.NewInsertOneModel().SetDocument(item))
			if len(models) > 40000 {
				log.Println("insert to mongo", len(models))
				_, err := collection.BulkWrite(ctx, models, opts)
				if err != nil {
					log.Println("bulk upsert fail", err)
				}
				models = []mongo.WriteModel{}
			}
		}

		log.Info("last insert to mongo")
		if len(models) > 0 {
			_, err := collection.BulkWrite(ctx, models, opts)
			if err != nil {
				log.Println("bulk upsert fail", err)
			}
		}
		resultWg.Done()
	}()

	sw := sync.WaitGroup{}
	process_count := 20
	sw.Add(process_count)
	for _i := 0; _i < process_count; _i++ {
		go func() {
			for ID := range IDChan {
				item := WorkPool[ID]
				if len(item.Out) == 0 {
					ret := mode.DisruptionType{
						ID:   ID,
						I:    len(item.In),
						J:    0,
						K:    0,
						D:    1,
						Year: item.Year,
					}
					ResultChan <- ret
					continue
				}

				linksInSet := hashset.New()
				outNodeLinksInSet := hashset.New()
				for _, InID := range item.In {
					linksInSet.Add(InID)
				}

				for _, outID := range item.Out {
					if outNode, ok := WorkPool[outID]; ok {
						for _, InID := range outNode.In {
							outNodeLinksInSet.Add(InID)
						}
					}
				}
				j := linksInSet.Intersection(outNodeLinksInSet).Size()
				i := len(item.In) - j
				k := outNodeLinksInSet.Size() - j - 1
				ret := mode.DisruptionType{
					ID:   ID,
					I:    i,
					J:    j,
					K:    k,
					D:    float64(i-j) / float64(i+j+k),
					Year: item.Year,
				}
				ResultChan <- ret
			}
			sw.Done()
		}()
	}

	for ID := range WorkPool {
		IDChan <- ID
	}
	close(IDChan)

	sw.Wait()
	close(ResultChan)
	resultWg.Wait()
	log.Info("over")

}
