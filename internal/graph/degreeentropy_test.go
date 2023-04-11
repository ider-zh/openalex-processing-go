package graph

import (
	"testing"
)

func TestGetDegreeEntropy(t *testing.T) {
	var edges = []Edge{
		{0, 1}, {1, 2}, {2, 1}, {3, 4},
	}

	graph := EdgesToGraph(edges)
	gp := GraphProcess{graph}
	obj := gp.GetDegreeEntropy()
	t.Log(obj)
	if !IsEqual(obj.InE, 0.9182958340544893) {
		t.Error("inE error")
	}
	if !IsEqual(obj.OutE, 0) {
		t.Error("inE error")
	}
	if !IsEqual(obj.UndirectedE, 0.7219280948873623) {
		t.Error("inE error")
	}
	// if !IsEqual(obj.InSE, 0.6460148371100) {
	// 	t.Error("inE error")
	// }
	// if !IsEqual(obj.OutSE, 0.86135311614678) {
	// 	t.Error("inE error")
	// }
	// if !IsEqual(obj.UndirectedSE, 0.7816315860094917) {
	// 	t.Error("inE error")
	// }
}
