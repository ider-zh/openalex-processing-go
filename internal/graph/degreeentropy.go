package graph

import "math"

type degreeEntropyStats struct {
	InE         float64
	OutE        float64
	UndirectedE float64
}

func getDegreeFromMap(degreeMap map[int]int) (ret []int) {
	for k := range degreeMap {
		ret = append(ret, degreeMap[k])
	}
	return
}

func calDegreeEntropy(degrees []int) float64 {
	sum_degree := sumNumbers(degrees)

	cache_array := make([]float64, len(degrees))
	for i := range degrees {
		cache_array[i] = float64(degrees[i]) / float64(sum_degree)
	}

	for i := range cache_array {
		cache_array[i] = cache_array[i] * math.Log2(1/cache_array[i])
	}
	var E = sumNumbers(cache_array)
	// f = np_in/np_in.sum()
	// a = (- f * np.log2(f)).sum()
	return E
}

func (c *GraphProcess) GetDegreeEntropy() degreeEntropyStats {
	var in_degreeMap = make(map[int]int)
	var out_degreeMap = make(map[int]int)
	var undirected_degreeMap = make(map[int]int)

	for _, node := range c.Node {
		InIDsCount := node.InIDs.Cardinality()
		if InIDsCount > 0 {
			in_degreeMap[InIDsCount] += 1
		}

		OutIDsCount := node.OutIDs.Cardinality()
		if OutIDsCount > 0 {
			out_degreeMap[OutIDsCount] += 1
		}

		undirected_degree_count := node.InIDs.Union(node.OutIDs).Cardinality()
		if undirected_degree_count > 0 {
			undirected_degreeMap[undirected_degree_count] += 1
		}
	}
	in_degree := getDegreeFromMap(in_degreeMap)
	out_degree := getDegreeFromMap(out_degreeMap)
	undirected_degree := getDegreeFromMap(undirected_degreeMap)

	in_E := calDegreeEntropy(in_degree)
	out_E := calDegreeEntropy(out_degree)
	undirected_E := calDegreeEntropy(undirected_degree)

	return degreeEntropyStats{
		InE:         in_E,
		OutE:        out_E,
		UndirectedE: undirected_E,
	}
}
