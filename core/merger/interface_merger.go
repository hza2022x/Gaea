package merger

// AggregateFuncMerger is the merger of aggregate function
type AggregateFuncMerger interface {
	// MergeTo 合并结果集, from为待合并行, to为结果聚合行
	MergeTo(from, to ResultRow) error
}

// ResultRow is one Row in Result
type ResultRow []interface{}
