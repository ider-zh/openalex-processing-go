package graph

import mapset "github.com/deckarep/golang-set/v2"

type NodeLink struct {
	ID     int64
	InIDs  mapset.Set[int64]
	OutIDs mapset.Set[int64]
}

type Edge struct {
	S int64
	D int64
}

type GraphProcess struct {
	Node []*NodeLink
}
