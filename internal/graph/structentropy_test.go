package graph

import (
	"testing"
)

func TestGetStructEntropy(t *testing.T) {
	var edges = []Edge{
		{0, 1}, {1, 2}, {2, 1}, {3, 4},
	}

	graph := EdgesToGraph(edges)
	gp := GraphProcess{graph}
	obj := gp.GetStructEntropy()
	if !IsEqual(obj.InE, 1.5) {
		t.Error("inE error")
	}
	if !IsEqual(obj.OutE, 2) {
		t.Error("inE error")
	}
	if !IsEqual(obj.UndirectedE, 2.25162916738782) {
		t.Error("inE error")
	}
	if !IsEqual(obj.InSE, 0.6460148371100) {
		t.Error("inE error")
	}
	if !IsEqual(obj.OutSE, 0.86135311614678) {
		t.Error("inE error")
	}
	if !IsEqual(obj.UndirectedSE, 0.7816315860094917) {
		t.Error("inE error")
	}
}
