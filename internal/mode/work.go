package mode

type BaseSource struct {
	ID string `json:"id"`
}

type WorkSource struct {
	ID string `json:"id"`
	// DOI  string `json:"doi"`
	Year        int32  `json:"publication_year"`
	Type        string `json:"type"`
	UpdatedDate string `json:"updated_date"`
	// authorships
	AuthorShips []struct {
		Author struct {
			ID interface{} `json:"id"` //may be string or int
		} `json:"author"`
		Institutions []struct {
			ID          string `json:"id"`
			Name        string `json:"display_name"`
			CountryCode string `json:"country_code"`
		} `json:"institutions"`
	} `json:"authorships"`

	// Concepts
	Concepts []struct {
		ID    string `json:"id"`
		Level int32  `json:"level"`
	} `json:"concepts"`

	Ref []string `json:"referenced_works"`
}

type WorkMongoConcepts struct {
	ID    int64 `json:"id"`
	Level int32 `json:"level"`
}
type WorkMongo struct {
	ID      int64               `bson:"_id"`
	Year    int32               `bson:"year"`
	Type    int32               `bson:"type"`
	TypeStr string              `bson:"-"` //conver to type enum
	Concept []WorkMongoConcepts `bson:"concept,omitempty"`
	In      []int64             `bson:"in,omitempty"`
	Out     []int64             `bson:"out,omitempty"`
	Country []string            `bson:"country,omitempty"`
}

type DisruptionType struct {
	ID   int64   `bson:"_id"`
	I    int     `bson:"i"`
	J    int     `bson:"j"`
	K    int     `bson:"k"`
	D    float64 `bson:"d"`
	Year int32   `bson:"year"`
}
