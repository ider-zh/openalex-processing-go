package loadfile

import (
	"compress/gzip"
	"encoding/csv"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	log "github.com/sirupsen/logrus"

	"github.com/monitor1379/yagods/sets/hashset"
)

var (
	projects = []string{"authors", "institutions", "venues", "works"}
)

type MergeIDS struct {
	IDMAP map[string]*hashset.Set[int64]
}

func NewMergeIDS() (ret *MergeIDS) {
	MergeName := "merged_ids"
	rootPath := filepath.Join(basicPath, MergeName)
	// init IDMAP
	ret = &MergeIDS{make(map[string]*hashset.Set[int64])}
	for _, projectName := range projects {
		ret.IDMAP[projectName] = hashset.New[int64]()

		subjectPath := filepath.Join(rootPath, projectName)
		filepath.WalkDir(subjectPath, func(path string, d fs.DirEntry, err error) error {
			if strings.HasSuffix(path, ".gz") {
				readGzipCsv(path, ret.IDMAP[projectName])
			}
			return err
		})
	}
	for key, set := range ret.IDMAP {
		log.Infof("mergeId statistic: %s: %d", key, set.Size())
	}
	return
}

func readGzipCsv(filePath string, idSet *hashset.Set[int64]) {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	cr := csv.NewReader(gr)

	// get id index
	rec, err := cr.Read()
	if err != nil {
		log.Fatal(err)
	}
	var idIndex int
	for i, v := range rec {
		if v == "id" {
			idIndex = i
		}
	}

	for {
		rec, err := cr.Read()
		if rec == nil {
			return
		}
		if err != nil {
			log.Fatal(err)
		}
		idString := rec[idIndex]
		if !unicode.IsLetter(rune(idString[0])) {
			log.Warnln("id not start  with letter", idString)
		} else {
			ID, err := strconv.ParseInt(idString[1:], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			idSet.Add(ID)
		}
	}
}
