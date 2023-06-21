package router

import "github.com/XiaoMi/Gaea/parser"

func CanHandleWithoutPlan(stmtType int) bool {
	return stmtType == parser.StmtShow ||
		stmtType == parser.StmtSet ||
		stmtType == parser.StmtBegin ||
		stmtType == parser.StmtCommit ||
		stmtType == parser.StmtRollback ||
		stmtType == parser.StmtSavepoint ||
		stmtType == parser.StmtUse
}
