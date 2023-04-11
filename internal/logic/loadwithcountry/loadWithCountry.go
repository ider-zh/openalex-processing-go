package loadwithcountry

import (
	"bufio"
	"context"
	"encoding/json"
	"openalex/internal/loadfile"
	"openalex/internal/mode"
	"openalex/internal/worktype"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/monitor1379/yagods/sets/hashset"
	log "github.com/sirupsen/logrus"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// dump reference to mongo, add paper country
type conceptItem struct {
	ID    string `json:"id"`
	Level int32  `json:"level"`
}

func Main() {
	// country iso name map
	COUNTRY_MAP := make(map[string]string)
	filePath := "/home/ni/data/mag2020/sx_country_target_zh.txt"
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		strs := strings.Split(scanner.Text(), "\t")
		if len(strs) == 4 {

			codeName := strings.TrimSpace(strs[0])
			countryName1 := strings.TrimSpace(strs[1])
			countryName2 := strings.TrimSpace(strs[2])
			COUNTRY_MAP[countryName1] = codeName
			COUNTRY_MAP[countryName2] = codeName

		} else {
			log.Printf("len(strs): %v %v\n", len(strs), strs)
		}
	}
	for k, v := range COUNTRY_MAP {
		log.Printf("ORG_COUNTRY_MAP: %v  :  %v\n", k, v)
		break
	}
	log.Printf("ORG_COUNTRY_MAP: %v\n", len(COUNTRY_MAP))

	// load autor fountry map
	ORG_COUNTRY_MAP := make(map[string]string)
	filePath = "/home/ni/data/mag2020/noOrgId_country_openAlex_zh.txt"
	file, err = os.Open(filePath)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	scanner = bufio.NewScanner(file)
	for scanner.Scan() {
		strs := strings.Split(scanner.Text(), "\t")
		if len(strs) > 1 {
			var countrysList []string
			err = json.Unmarshal([]byte(strs[len(strs)-1]), &countrysList)
			if err != nil {
				log.Fatalln(err)
			}
			if len(countrysList) == 1 && countrysList[0] != "" {
				if isoNmae, ok := COUNTRY_MAP[countrysList[0]]; ok {
					orgStr := strings.TrimSpace(strings.Join(strs[:len(strs)-1], " "))
					ORG_COUNTRY_MAP[orgStr] = isoNmae
				} else {
					log.Printf("countrysList[0]: no iso name [%v]\n", countrysList)
				}
			}
		} else {
			log.Printf("len(strs): %v %v\n", len(strs), strs)
		}
	}
	for k, v := range ORG_COUNTRY_MAP {
		log.Printf("ORG_COUNTRY_MAP: %v  :  %v\n", k, v)
		break
	}
	log.Printf("ORG_COUNTRY_MAP: %v\n", len(ORG_COUNTRY_MAP))

	// load mergeIDS

	MergeIDs := loadfile.NewMergeIDS()
	WorksSet := MergeIDs.IDMAP["works"]

	// load work types
	WorkTypeConvert := worktype.NewWorkType()
	defer WorkTypeConvert.Dump()

	// load works

	chanOut := make(chan *mode.WorkMongo, 1000)

	var handlePipeLine = func(s *mode.WorkSource) {
		IDs := strings.Split(s.ID, "/")
		ID, err := strconv.ParseInt(IDs[len(IDs)-1][1:], 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		if WorksSet.Contains(ID) {
			return
		}
		countryCodeSet := hashset.New[string]()
		for _, obj := range s.AuthorShips {
		FindCountry:
			for _, ins := range obj.Institutions {
				if ins.CountryCode != "" {
					countryCodeSet.Add(ins.CountryCode)
					break FindCountry
				}

				if subCountryCode, ok := ORG_COUNTRY_MAP[ins.Name]; ok {
					countryCodeSet.Add(subCountryCode)
					break FindCountry
				}
			}
		}

		countryCode := countryCodeSet.Values()
		dataOut := mode.WorkMongo{
			ID:      ID,
			Year:    s.Year,
			TypeStr: strings.TrimSpace(s.Type),
		}
		if len(countryCode) > 0 {
			dataOut.Country = countryCode
		}

		for _, item := range s.Concepts {
			// format ID
			IDs := strings.Split(item.ID, "/")
			IDInt64, err := strconv.ParseInt(IDs[len(IDs)-1][1:], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			dataOut.Concept = append(dataOut.Concept, mode.WorkMongoConcepts{ID: IDInt64, Level: item.Level})
		}

		idOuts := []int64{}
		for _, idString := range s.Ref {
			IDs := strings.Split(idString, "/")
			IDR, err := strconv.ParseInt(IDs[len(IDs)-1][1:], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			idOuts = append(idOuts, IDR)
		}
		if len(idOuts) > 0 {
			dataOut.Out = idOuts
		}

		chanOut <- &dataOut
	}

	// handle ret data

	baseChan := loadfile.ExtractingWork()

	// work
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			for data := range baseChan {
				handlePipeLine(data)
			}
			wg.Done()
		}()
	}

	// summer
	wgLast := sync.WaitGroup{}
	wgLast.Add(1)
	go func() {
		// handle linsin
		inMap := make(map[int64][]int64)
		allMap := make(map[int64]*mode.WorkMongo)

		allHandleOut := 0
		for item := range chanOut {
			allHandleOut += 1
			allMap[item.ID] = item

			// buid in id
			for _, outID := range item.Out {
				inMap[outID] = append(inMap[outID], item.ID)
			}
		}
		log.Println("all allHandleOut count:", allHandleOut)

		Client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://knogen:knogen@127.0.0.1:27017"))
		// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		ctx := context.Background()
		// defer cancel()
		err = Client.Connect(ctx)
		if err != nil {
			log.Panic("mongo connect fatil")
		}

		collection := Client.Database("openalex").Collection("works")
		indexModels := []mongo.IndexModel{
			{
				Keys: bson.D{{Key: "year", Value: 1}}, Options: options.Index().SetBackground(true),
			},
			{
				Keys: bson.D{{Key: "type", Value: 1}}, Options: options.Index().SetBackground(true),
			},
			{
				Keys: bson.D{{Key: "country", Value: 1}}, Options: options.Index().SetBackground(true),
			},
			{
				Keys: bson.D{{Key: "year", Value: 1}, {Key: "country", Value: 1}}, Options: options.Index().SetBackground(true),
			},
			{
				Keys: bson.D{{Key: "year", Value: 1}, {Key: "type", Value: 1}, {Key: "country", Value: 1}}, Options: options.Index().SetBackground(true),
			},
		}
		collection.Indexes().CreateMany(ctx, indexModels)

		opts := options.BulkWrite().SetOrdered(false)
		models := []mongo.WriteModel{}

		i := 0
		for k := range allMap {
			if In, ok := inMap[allMap[k].ID]; ok {
				allMap[k].In = In
			}
			// work type convert
			allMap[k].Type = WorkTypeConvert.GetTypeID(allMap[k].TypeStr)

			i += 1
			models = append(models, mongo.NewInsertOneModel().SetDocument(allMap[k]))
			if i%50000 == 0 {
				log.Println("insert to mongo")
				_, err := collection.BulkWrite(ctx, models, opts)
				if err != nil {
					log.Println("bulk upsert fail", err)
				}
				models = []mongo.WriteModel{}
			}
		}
		if len(models) > 0 {
			_, err := collection.BulkWrite(ctx, models, opts)
			log.Println("last insert to mongo")
			if err != nil {
				log.Println("bulk upsert fail", err)
			}
		}
		// dump work type
		wgLast.Done()
	}()
	wg.Wait()
	close(chanOut)
	wgLast.Wait()

}
