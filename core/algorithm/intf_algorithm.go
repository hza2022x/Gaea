package algorithm

// Shard shard接口
type Shard interface {
	FindForKey(key interface{}) (int, error)
}

// RangeShard 一个范围的分片,例如[start,end)
type RangeShard interface {
	Shard
	EqualStart(key interface{}, index int) bool
}
