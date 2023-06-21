package router

import (
	"github.com/XiaoMi/Gaea/common/constant"
	"github.com/XiaoMi/Gaea/parser"
	"strings"
)

func CanHandleWithoutPlan(stmtType int) bool {
	return stmtType == parser.StmtShow ||
		stmtType == parser.StmtSet ||
		stmtType == parser.StmtBegin ||
		stmtType == parser.StmtCommit ||
		stmtType == parser.StmtRollback ||
		stmtType == parser.StmtSavepoint ||
		stmtType == parser.StmtUse
}

func CanExecuteFromSlave(sql string) bool {
	if parser.Preview(sql) != parser.StmtSelect {
		return false
	}

	_, comments := parser.SplitMarginComments(sql)
	hint := strings.ToLower(strings.TrimSpace(comments.Leading))
	result := strings.ToLower(hint) == constant.MasterHint
	return !result
}
