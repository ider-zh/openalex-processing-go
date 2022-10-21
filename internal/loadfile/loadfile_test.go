package loadfile_test

import (
	"testing"

	. "openalex/internal/loadfile"
	"openalex/internal/mode"

	log "github.com/sirupsen/logrus"
)

func TestWorks(t *testing.T) {
	F1 := "a1"
	F2 := "a2"
	var testpipLine1 = func(s mode.WorkSource) {
		log.Println(F1, s.ID)
	}

	var testpipLine2 = func(s mode.WorkSource) {
		log.Println(F2, s.ID)
	}
	ffc := []func(mode.WorkSource){testpipLine1, testpipLine2}
	for row := range ExtractingWork(ffc) {
		log.Info(row.ID)
	}
}

// func TestMergeIDS(t *testing.T) {
// 	NewMergeIDS()
// 	// mid := NewMergeIDS()
// 	// t.Log(mid.IDMAP)
// }
