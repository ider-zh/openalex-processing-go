package graph

import "math"

func sumNumbers[T int | float64](array []T) T {
	var result T = 0
	for _, v := range array {
		result += v
	}
	return result
}

func calStructEntropy(degrees []int) float64 {
	sum_degree := sumNumbers(degrees)

	cache_array := make([]float64, len(degrees))
	for i := range degrees {
		cache_array[i] = float64(degrees[i]) / float64(sum_degree)
	}

	for i := range cache_array {
		cache_array[i] = -cache_array[i] * math.Log2(cache_array[i])
	}
	var E = sumNumbers(cache_array)
	// f = np_in/np_in.sum()
	// a = (- f * np.log2(f)).sum()
	return E
}

type structEntropyStats struct {
	InE              float64
	OutE             float64
	UndirectedE      float64
	InSE             float64
	OutSE            float64
	UndirectedSE     float64
	InLength         int
	OutLength        int
	UndirectedLength int
}

func (c *GraphProcess) GetStructEntropy() structEntropyStats {
	var in_degree []int
	var out_degree []int
	var undirected_degree []int

	for _, node := range c.Node {
		InIDsCount := node.InIDs.Cardinality()
		if InIDsCount > 0 {
			in_degree = append(in_degree, InIDsCount)
		}

		OutIDsCount := node.OutIDs.Cardinality()
		if OutIDsCount > 0 {
			out_degree = append(out_degree, OutIDsCount)
		}

		undirected_degree_count := node.InIDs.Union(node.OutIDs).Cardinality()
		if undirected_degree_count > 0 {
			undirected_degree = append(undirected_degree, undirected_degree_count)
		}
	}

	in_E := calStructEntropy(in_degree)
	out_E := calStructEntropy(out_degree)
	undirected_E := calStructEntropy(undirected_degree)

	undirected_E_min := math.Log2(4*float64(len(undirected_degree)-1)) / 2
	undirected_E_max := math.Log2(float64(len(undirected_degree)))

	return structEntropyStats{
		InE:              in_E,
		OutE:             out_E,
		UndirectedE:      undirected_E,
		InSE:             in_E / math.Log2(float64(len(undirected_degree))),
		OutSE:            out_E / math.Log2(float64(len(undirected_degree))),
		UndirectedSE:     (undirected_E - undirected_E_min) / (undirected_E_max - undirected_E_min),
		InLength:         len(in_degree),
		OutLength:        len(out_degree),
		UndirectedLength: len(undirected_degree),
	}

}

// retin = a /  math.log2(len(np_all))
// retout = b /  math.log2(len(np_all))
// retall = (c - all_E_min) / (math.log2(len(np_all)) - all_E_min)

// return retin,retout,retall,a,b,c,len(np_in),len(np_out),len(np_all)
