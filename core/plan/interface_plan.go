package plan

import (
	"github.com/XiaoMi/Gaea/mysql"
	"github.com/XiaoMi/Gaea/util"
)

// Plan is a interface for select/insert etc.
type Plan interface {
	ExecuteIn(*util.RequestContext, Executor) (*mysql.Result, error)

	// only for cache
	Size() int
}
