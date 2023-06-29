package router

import algorithm2 "github.com/XiaoMi/Gaea/core/algorithm"

type Rule interface {
	GetDB() string
	GetTable() string
	GetShardingColumn() string
	IsLinkedRule() bool
	GetShard() algorithm2.Shard
	FindTableIndex(key interface{}) (int, error)
	GetSlice(i int) string // i is slice index
	GetSliceIndexFromTableIndex(i int) int
	GetSlices() []string
	GetSubTableIndexes() []int
	GetFirstTableIndex() int
	GetLastTableIndex() int
	GetType() string
	GetDatabaseNameByTableIndex(index int) (string, error)
}

type MycatRule interface {
	Rule
	GetDatabases() []string
	GetTableIndexByDatabaseName(phyDB string) (int, bool)
}
