package worktype

import (
	"context"

	log "github.com/sirupsen/logrus"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type WorkType struct {
	TypeMap map[string]int32
	NextID  int32
}

type WorkTypeItem struct {
	ID   int32  `bson:"_id"`
	Name string `bson:"name"`
}

func getMongoCollection() (Client *mongo.Client, collection *mongo.Collection, ctx context.Context) {

	Client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://knogen:knogen@127.0.0.1:27017"))
	ctx = context.Background()
	err = Client.Connect(ctx)
	if err != nil {
		log.Panic("mongo connect fatil")
	}
	collection = Client.Database("openalex").Collection("work_type")
	return
}

func NewWorkType() (retItem *WorkType) {
	Client, collection, ctx := getMongoCollection()
	cursor, err := collection.Find(ctx, bson.D{})
	if err != nil {
		panic(err)
	}
	retItem = &WorkType{
		TypeMap: make(map[string]int32),
		NextID:  0,
	}
	for cursor.Next(ctx) {
		var item WorkTypeItem
		if err = cursor.Decode(&item); err != nil {
			log.Fatal(err)
		}
		retItem.TypeMap[item.Name] = item.ID
		retItem.NextID += 1
	}
	Client.Disconnect(ctx)
	log.Infoln("work type count:", retItem.NextID)
	return
}

func (c *WorkType) Dump() {
	Client, collection, ctx := getMongoCollection()
	opts := options.BulkWrite().SetOrdered(false)
	models := []mongo.WriteModel{}
	for k, value := range c.TypeMap {
		models = append(models, mongo.NewInsertOneModel().SetDocument(WorkTypeItem{value, k}))
	}
	_, err := collection.BulkWrite(ctx, models, opts)
	if err != nil {
		log.Println("bulk upsert fail", err)
	}
	Client.Disconnect(ctx)
}

func (c *WorkType) GetTypeID(name string) int32 {
	if ID, ok := c.TypeMap[name]; ok {
		return ID
	}
	c.TypeMap[name] = c.NextID
	c.NextID += 1
	return c.TypeMap[name]
}
