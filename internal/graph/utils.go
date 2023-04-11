package graph

import (
	"math"

	mapset "github.com/deckarep/golang-set/v2"
)

const MIN = 0.000001

// MIN 为用户自定义的比较精度
func IsEqual(a, b float64) bool {
	return math.Abs(a-b) <= MIN
}

func EdgesToGraph(edge []Edge) (ret []*NodeLink) {
	cacheDict := make(map[int64]*NodeLink)
	for _, edge := range edge {
		if _, ok := cacheDict[edge.S]; ok {
			cacheDict[edge.S].OutIDs.Add(edge.D)
		} else {
			cacheDict[edge.S] = &NodeLink{
				ID:     edge.S,
				InIDs:  mapset.NewSet[int64](),
				OutIDs: mapset.NewSet(edge.D),
			}
		}
		if _, ok := cacheDict[edge.D]; ok {
			cacheDict[edge.D].InIDs.Add(edge.S)
		} else {
			cacheDict[edge.D] = &NodeLink{
				ID:     edge.D,
				InIDs:  mapset.NewSet(edge.S),
				OutIDs: mapset.NewSet[int64](),
			}
		}
	}
	for k := range cacheDict {
		ret = append(ret, cacheDict[k])
	}
	return
}

func EdgesToGraphByChan(edgeChan chan Edge, graphNodeRetChan chan []*NodeLink) {
	cacheDict := make(map[int64]*NodeLink)
	for edge := range edgeChan {
		if _, ok := cacheDict[edge.S]; ok {
			cacheDict[edge.S].OutIDs.Add(edge.D)
		} else {
			cacheDict[edge.S] = &NodeLink{
				ID:     edge.S,
				InIDs:  mapset.NewSet[int64](),
				OutIDs: mapset.NewSet(edge.D),
			}
		}
		if _, ok := cacheDict[edge.D]; ok {
			cacheDict[edge.D].InIDs.Add(edge.S)
		} else {
			cacheDict[edge.D] = &NodeLink{
				ID:     edge.D,
				InIDs:  mapset.NewSet(edge.S),
				OutIDs: mapset.NewSet[int64](),
			}
		}
	}
	var graphNodeDetailCache []*NodeLink
	for k := range cacheDict {
		graphNodeDetailCache = append(graphNodeDetailCache, cacheDict[k])
	}
	graphNodeRetChan <- graphNodeDetailCache
	close(graphNodeRetChan)
}
