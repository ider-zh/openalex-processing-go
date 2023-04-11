package googledistance

import (
	"context"
	"fmt"
	"math"
	"openalex/internal/mode"
	"sort"
	"sync"

	"github.com/emirpasic/gods/sets/hashset"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// google当年的距离：A=国家A的linksout B=国家B的linksout J=A和B的交集 Total=当年（新发布的）的所有论文数+当年（新发布的）论文的linksout数，distance=[log(max)-log(J)]/[log(Total)-log(min)]
// Total只取当年的论文数的话，可能导致国家的linksout数量大于当年的论文数，最终导致距离为负数。Total算上linksout数的话，就是取的当年论文的linksout引用网络算的一个距离。
// 国家间的距离，
// 国家-学科间的距离
type totalCountN struct {
	Total    int32
	NoOrphan int32
}

var (
	yearStart                 = 1920
	yearEnd                   = 2023
	yearRange                 = yearEnd - yearStart + 1
	totalCountNByYear         = make([]totalCountN, yearRange)
	countrySet                = hashset.New("US", "CN", "GB", "DE", "JP", "FR", "IN", "CA", "BR", "IT", "AU", "ES", "KR", "RU", "NL", "ID", "PL", "IR", "SE", "CH", "TR", "TW", "BE", "MX", "IL", "DK", "AT", "FI", "ZA", "PT")
	conceptTopSet             = []int64{205649164, 127413603, 144133560, 33923547, 121332964, 15744967, 41008148, 95457728, 185592680, 192562407, 17744445, 127313418, 138885662, 144024400, 39432304, 142362112, 71924100, 86803240, 162324750}
	countryLinksOutMap        = make(map[string][]*hashset.Set)
	countryConceptLinksOutMap = make(map[string]map[int64][]*hashset.Set)
)

func init() {
	// init A,B
	for _, nameI := range countrySet.Values() {
		name := nameI.(string)
		countryLinksOutMap[name] = make([]*hashset.Set, yearRange)
		for i := range countryLinksOutMap[name] {
			countryLinksOutMap[name][i] = hashset.New()
		}
	}

	for _, nameI := range countrySet.Values() {
		name := nameI.(string)
		countryConceptLinksOutMap[name] = make(map[int64][]*hashset.Set)

		for _, conceptID := range conceptTopSet {
			countryConceptLinksOutMap[name][conceptID] = make([]*hashset.Set, yearRange)

			for i := range countryConceptLinksOutMap[name][conceptID] {
				countryConceptLinksOutMap[name][conceptID][i] = hashset.New()
			}
		}
	}
}

func getMongoCollection(collectionName string) (Client *mongo.Client, collection *mongo.Collection, ctx context.Context) {

	Client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://knogen:knogen@127.0.0.1:27017"))
	ctx = context.Background()
	err = Client.Connect(ctx)
	if err != nil {
		log.Panic("mongo connect fatil")
	}
	collection = Client.Database("openalex").Collection(collectionName)
	return
}

// total 取2个mode, 包含孤立节点的，和去除孤立节点的
// 年内所有国家作为一次
func getWorkData() (ret []*mode.WorkMongo) {
	Client, collection, ctx := getMongoCollection("works")

	// get book IDs
	bookIDSet := hashset.New()
	cursor, err := collection.Find(ctx, bson.M{"t": bson.M{"$in": bson.A{25, 23, 21, 17, 5, 1}}}, options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		log.Fatal(err)
	}
	for cursor.Next(ctx) {
		var item mode.WorkMongo
		if err = cursor.Decode(&item); err != nil {
			log.Fatal(err)
		}
		bookIDSet.Add(item.ID)
	}

	cursor, err = collection.Find(ctx, bson.M{"t": bson.M{"$nin": bson.A{25, 23, 21, 17, 5, 1}}})
	if err != nil {
		log.Fatal(err)
	}
	testI := 0
	for cursor.Next(ctx) {
		testI += 1
		if testI%10000000 == 0 {
			log.Infoln("get data count:", testI)
			// break
		}
		var item mode.WorkMongo
		if err = cursor.Decode(&item); err != nil {
			log.Fatal(err)
		}
		// filter not book
		if len(item.Out) > 0 {
			newOut := []int64{}
			for _, ID := range item.Out {
				if !bookIDSet.Contains(ID) {
					newOut = append(newOut, ID)
				}
			}
			item.Out = newOut
		}
		if len(item.In) > 0 {
			newIn := []int64{}
			for _, ID := range item.In {
				if !bookIDSet.Contains(ID) {
					newIn = append(newIn, ID)
				}
			}
			item.In = newIn
		}

		ret = append(ret, &item)

	}
	Client.Disconnect(ctx)
	return
}

// 遍历一次数据，生成 total CountN, 需要去重
func processABSet(inputSource []*mode.WorkMongo, wg *sync.WaitGroup) {
	log.Infof("start to process AB")
	for _, item := range inputSource {
		index := item.Year - int32(yearStart)
		if index >= int32(yearRange) || index < 0 {
			// if item.Year != 0 {
			// 	log.Warnln("out of year range:", item.Year, item.ID)
			// }
			continue
		}

		for _, name := range item.Country {
			if byTearSet, ok := countryLinksOutMap[name]; ok {
				for _, outID := range item.Out {
					byTearSet[index].Add(outID)
				}
			}
		}
	}
	log.Infof("process AB Done")
	wg.Done()
}

// 遍历一次数据，生成 total CountN, 需要去重
func processConceptABSet(inputSource []*mode.WorkMongo, wg *sync.WaitGroup) {
	log.Infof("start to concept process AB")
	for _, item := range inputSource {
		index := item.Year - int32(yearStart)
		if index >= int32(yearRange) || index < 0 {
			// if item.Year != 0 {
			// 	log.Warnln("out of year range:", item.Year, item.ID)
			// }
			continue
		}

		for _, name := range item.Country {
			if byCoceptSet, ok := countryConceptLinksOutMap[name]; ok {
				for _, obj := range item.Concept {
					if obj.Level != 0 {
						continue
					}
					for _, outID := range item.Out {
						if byYearSet, ok := byCoceptSet[obj.ID]; ok {
							byYearSet[index].Add(outID)
						} else {
							log.Warn("cocept level is 0,but not in map:", obj.ID)
						}
					}
				}
			}
		}
	}
	log.Infof("process concept AB Done")
	wg.Done()
}

// 遍历一次数据，生成 total CountN, 需要去重
func processTotalCountN(inputSource []*mode.WorkMongo, wg *sync.WaitGroup) {
	log.Infof("start to process Total N")
	type totalNHaseSey struct {
		Total    *hashset.Set
		NoOrphan *hashset.Set
	}
	totalByYearSet := make([]totalNHaseSey, yearRange)
	// init
	for i := range totalByYearSet {
		totalByYearSet[i].Total = hashset.New()
		totalByYearSet[i].NoOrphan = hashset.New()
	}

	for _, item := range inputSource {
		index := item.Year - int32(yearStart)
		if index >= int32(yearRange) || index < 0 {
			// if item.Year != 0 {
			// 	log.Warnln("out of year range:", item.Year, item.ID)
			// }
			continue
		}

		totalByYearSet[index].Total.Add(item.ID)
		for _, outID := range item.Out {
			totalByYearSet[index].Total.Add(outID)
		}

		if len(item.Out) > 0 && len(item.In) > 0 {
			totalByYearSet[index].NoOrphan.Add(item.ID)
			for _, outID := range item.Out {
				totalByYearSet[index].NoOrphan.Add(outID)
			}
		}
	}
	for i := range totalByYearSet {
		totalCountNByYear[i].NoOrphan = int32(totalByYearSet[i].NoOrphan.Size())
		totalCountNByYear[i].Total = int32(totalByYearSet[i].Total.Size())
	}
	log.Infof("process Total Count Done")
	wg.Done()
}

type GDMongo struct {
	ID        string    `bson:"_id"`
	StartYear int       `bson:"start_year"`
	EndtYear  int       `bson:"end_year"`
	A         string    `bson:"a"`
	B         string    `bson:"b"`
	DNoOrphan []float64 `bson:"d_noorphan"`
	DTotal    []float64 `bson:"d_total"`
}

type GDConceptWorkItem struct {
	ID        string  `bson:"_id"`
	Year      int     `bson:"year"`
	A         string  `bson:"a"`
	B         string  `bson:"b"`
	Ac        int64   `bson:"ac"`
	Bc        int64   `bson:"bc"`
	DNoOrphan float64 `bson:"d_noorphan"`
	DTotal    float64 `bson:"d_total"`
}

type GDConceptMongo struct {
	ID        string    `bson:"_id"`
	StartYear int       `bson:"start_year"`
	EndtYear  int       `bson:"end_year"`
	A         string    `bson:"a"`
	B         string    `bson:"b"`
	Ac        int64     `bson:"ac"`
	Bc        int64     `bson:"bc"`
	DNoOrphan []float64 `bson:"d_noorphan"`
	DTotal    []float64 `bson:"d_total"`
}

func processCocentDistance(wg *sync.WaitGroup) {
	log.Info("start summer process concept distance")
	//last cal data
	countryNames := []string{}
	for _, nameI := range countrySet.Values() {
		countryNames = append(countryNames, nameI.(string))
	}
	sort.Strings(countryNames)

	// process in
	chanin := make(chan *GDConceptWorkItem, 10000)
	chanout := make(chan *GDConceptWorkItem, 100000)
	go func() {
		for yearIndex := range totalCountNByYear {
			for i := 0; i < len(countryNames); i++ {
				for ci := range conceptTopSet {
					for j := i; j < len(countryNames); j++ {
						for cj := range conceptTopSet {
							chanin <- &GDConceptWorkItem{
								ID:   fmt.Sprintf("%v_%v_%v_%v_%v", countryNames[i], countryNames[j], conceptTopSet[ci], conceptTopSet[cj], yearIndex+yearStart),
								A:    countryNames[i],
								B:    countryNames[j],
								Ac:   conceptTopSet[ci],
								Bc:   conceptTopSet[cj],
								Year: yearIndex + yearStart,
							}
						}
					}
				}
			}
		}
		close(chanin)
	}()

	// 30 process handle
	wg1 := sync.WaitGroup{}
	for i := 0; i < 30; i++ {
		wg1.Add(1)
		go func() {
			for obj := range chanin {
				nameA := obj.A
				nameB := obj.B
				AValue := float64(countryConceptLinksOutMap[nameA][obj.Ac][obj.Year-yearStart].Size())
				BValue := float64(countryConceptLinksOutMap[nameB][obj.Bc][obj.Year-yearStart].Size())
				maxAB := math.Max(math.Log10(AValue), math.Log10(BValue))
				minAB := math.Min(math.Log10(AValue), math.Log10(BValue))
				JValue := float64(countryConceptLinksOutMap[nameA][obj.Ac][obj.Year-yearStart].Intersection(countryConceptLinksOutMap[nameB][obj.Bc][obj.Year-yearStart]).Size())
				J := math.Log10(JValue)
				NNoOrphan := math.Log10(float64(totalCountNByYear[obj.Year-yearStart].NoOrphan))
				NTotal := math.Log10(float64(totalCountNByYear[obj.Year-yearStart].Total))

				obj.DNoOrphan = (maxAB - J) / (NNoOrphan - minAB)
				obj.DTotal = (maxAB - J) / (NTotal - minAB)

				chanout <- obj
			}
			wg1.Done()
		}()
	}

	wg2 := sync.WaitGroup{}
	wg2.Add(1)
	go func() {
		retMongo := make(map[string]*GDConceptMongo)

		for obj := range chanout {
			key := fmt.Sprintf("%v_%v_%v_%v", obj.A, obj.B, obj.Ac, obj.Bc)
			if _, ok := retMongo[key]; ok {
				retMongo[key].DNoOrphan[obj.Year-yearStart] = obj.DNoOrphan
				retMongo[key].DTotal[obj.Year-yearStart] = obj.DTotal
			} else {
				retMongo[key] = &GDConceptMongo{
					ID:        key,
					StartYear: yearStart,
					EndtYear:  yearEnd,
					A:         obj.A,
					B:         obj.B,
					Ac:        obj.Ac,
					Bc:        obj.Bc,
					DNoOrphan: make([]float64, yearRange),
					DTotal:    make([]float64, yearRange),
				}
				retMongo[key].DNoOrphan[obj.Year-yearStart] = obj.DNoOrphan
				retMongo[key].DTotal[obj.Year-yearStart] = obj.DTotal
			}

		}
		Client, collection, ctx := getMongoCollection("country_google_distance_concept")
		opts := options.BulkWrite().SetOrdered(false)
		models := []mongo.WriteModel{}
		for _, item := range retMongo {
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

		Client.Disconnect(ctx)
		wg2.Done()
	}()

	wg1.Wait()
	close(chanout)
	wg2.Wait()
	wg.Done()

	log.Info("finish summer process concept distance")
}

func Main() {
	workData := getWorkData()
	log.Infof("work get success, count %v \n", len(workData))
	wg1, wg2, wg3 := &sync.WaitGroup{}, &sync.WaitGroup{}, &sync.WaitGroup{}
	// wg1, wg2 := &sync.WaitGroup{}, &sync.WaitGroup{}
	wg1.Add(1)
	wg2.Add(1)
	wg3.Add(1)
	go processTotalCountN(workData, wg1)
	go processABSet(workData, wg2)
	go processConceptABSet(workData, wg3)
	wg1.Wait()
	wg2.Wait()
	wg3.Wait()

	wg1.Add(1)
	go func() {
		log.Info("start summer process distance")
		//last cal data
		countryNames := []string{}
		for _, nameI := range countrySet.Values() {
			countryNames = append(countryNames, nameI.(string))
		}
		sort.Strings(countryNames)
		retCountryMongoCache := make(map[string]*GDMongo)

		for yearIndex := range totalCountNByYear {
			for i := 0; i < len(countryNames); i++ {
				nameA := countryNames[i]
				AValue := float64(countryLinksOutMap[nameA][yearIndex].Size())
				for j := i; j < len(countryNames); j++ {
					nameB := countryNames[j]
					BValue := float64(countryLinksOutMap[nameB][yearIndex].Size())
					maxAB := math.Max(math.Log10(AValue), math.Log10(BValue))
					minAB := math.Min(math.Log10(AValue), math.Log10(BValue))
					JValue := float64(countryLinksOutMap[nameA][yearIndex].Intersection(countryLinksOutMap[nameB][yearIndex]).Size())
					J := math.Log10(JValue)
					NNoOrphan := math.Log10(float64(totalCountNByYear[yearIndex].NoOrphan))
					NTotal := math.Log10(float64(totalCountNByYear[yearIndex].Total))

					retNoOrphanValue := (maxAB - J) / (NNoOrphan - minAB)
					retTotalValue := (maxAB - J) / (NTotal - minAB)

					key := fmt.Sprintf("%v_%v", nameA, nameB)

					if _, ok := retCountryMongoCache[key]; ok {
						retCountryMongoCache[key].DNoOrphan[yearIndex] = retNoOrphanValue
						retCountryMongoCache[key].DTotal[yearIndex] = retTotalValue
					} else {
						retCountryMongoCache[key] = &GDMongo{
							ID:        fmt.Sprintf("%v_%v_%v", nameA, nameB, yearIndex+yearStart),
							StartYear: yearStart,
							EndtYear:  yearEnd,
							A:         nameA,
							B:         nameB,
							DNoOrphan: make([]float64, yearRange),
							DTotal:    make([]float64, yearRange),
						}
						retCountryMongoCache[key].DNoOrphan[yearIndex] = retNoOrphanValue
						retCountryMongoCache[key].DTotal[yearIndex] = retTotalValue
					}
				}
			}
		}
		models := []mongo.WriteModel{}
		for _, item := range retCountryMongoCache {
			models = append(models, mongo.NewInsertOneModel().SetDocument(item))
		}
		log.Info("models count:", len(models))

		Client, collection, ctx := getMongoCollection("country_google_distance")
		opts := options.BulkWrite().SetOrdered(false)
		_, err := collection.BulkWrite(ctx, models, opts)
		log.Println("last insert to mongo")
		if err != nil {
			log.Println("bulk upsert fail", err)
		}
		Client.Disconnect(ctx)
		wg1.Done()
	}()

	wg2.Add(1)
	go processCocentDistance(wg2)

	wg1.Wait()
	wg2.Wait()

	log.Println("over")
}
