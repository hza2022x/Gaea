package router

type ShardRouter interface {
	GetShardRule(db, table string) (Rule, bool)
	GetRule(db, table string) Rule
	GetDefaultRule() Rule
}
