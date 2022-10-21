package mode

type WorkSource struct {
	ID string `json:"id"`
	// DOI  string `json:"doi"`
	Year int32  `json:"publication_year"`
	Type string `json:"type"`
	// authorships
	AuthorShips []struct {
		Author struct {
			ID string `json:"id"`
		} `json:"author"`
		Institutions []struct {
			ID          string `json:"id"`
			Name        string `json:"display_name"`
			CountryCode string `json:"country_code"`
		} `json:"institutions"`
	} `json:"AuthorShips"`

	Ref []string `json:"referenced_works"`
}

type WorkMongo struct {
	ID      int64   `bson:"_id"`
	Year    int32   `bson:"year"`
	In      []int64 `bson:"in"`
	Out     []int64 `bson:"out"`
	Country string  `bson:"country,omitempty"`
}
