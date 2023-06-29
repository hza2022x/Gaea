package plan

import (
	"github.com/XiaoMi/Gaea/mysql"
	"github.com/XiaoMi/Gaea/util"
)

// Executor TODO: move to package executor
type Executor interface {

	// 执行分片或非分片单条SQL
	ExecuteSQL(ctx *util.RequestContext, slice, db, sql string) (*mysql.Result, error)

	// 执行分片SQL
	ExecuteSQLs(*util.RequestContext, map[string]map[string][]string) ([]*mysql.Result, error)

	// 用于执行INSERT时设置last insert id
	SetLastInsertID(uint64)

	GetLastInsertID() uint64
}
