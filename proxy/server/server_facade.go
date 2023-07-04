package server

import (
	"fmt"
	"github.com/XiaoMi/Gaea/core/executor"
	"github.com/XiaoMi/Gaea/mysql"
	"github.com/XiaoMi/Gaea/util/log"
)

// ExecuteCommand execute command
func ExecuteCommand(cmd byte, data []byte, se *executor.SessionExecutor) Response {
	switch cmd {
	case mysql.ComQuit:
		se.HandleRollback(nil)
		// https://dev.mysql.com/doc/internals/en/com-quit.html
		// either a connection close or a OK_Packet, OK_Packet will cause client RST sometimes, but doesn't affect sql execute
		return CreateNoopResponse()
	case mysql.ComQuery: // data type: string[EOF]
		//DML统一为Query
		sql := string(data)
		// handle phase
		r, err := se.HandleDMLQuery(sql)
		if err != nil {
			return CreateErrorResponse(se.Status, err)
		}
		return CreateResultResponse(se.Status, r)
	case mysql.ComPing:
		return CreateOKResponse(se.Status)
	case mysql.ComInitDB:
		db := string(data)
		// handle phase
		err := se.HandleUseDB(db)
		if err != nil {
			return CreateErrorResponse(se.Status, err)
		}
		return CreateOKResponse(se.Status)
	case mysql.ComFieldList:
		fs, err := se.HandleFieldList(data)
		if err != nil {
			return CreateErrorResponse(se.Status, err)
		}
		return CreateFieldListResponse(se.Status, fs)
	case mysql.ComStmtPrepare:
		sql := string(data)
		stmt, err := se.HandleStmtPrepare(sql)
		if err != nil {
			return CreateErrorResponse(se.Status, err)
		}
		return CreatePrepareResponse(se.Status, stmt)
	case mysql.ComStmtExecute:
		values := make([]byte, len(data))
		copy(values, data)
		r, err := se.HandleStmtExecute(values)
		if err != nil {
			return CreateErrorResponse(se.Status, err)
		}
		return CreateResultResponse(se.Status, r)
	case mysql.ComStmtClose: // no response
		if err := se.HandleStmtClose(data); err != nil {
			return CreateErrorResponse(se.Status, err)
		}
		return CreateNoopResponse()
	case mysql.ComStmtSendLongData: // no response
		values := make([]byte, len(data))
		copy(values, data)
		if err := se.HandleStmtSendLongData(values); err != nil {
			return CreateErrorResponse(se.Status, err)
		}
		return CreateNoopResponse()
	case mysql.ComStmtReset:
		if err := se.HandleStmtReset(data); err != nil {
			return CreateErrorResponse(se.Status, err)
		}
		return CreateOKResponse(se.Status)
	case mysql.ComSetOption:
		return CreateEOFResponse(se.Status)
	default:
		msg := fmt.Sprintf("command %d not supported now", cmd)
		log.Warn("dispatch command failed, error: %s", msg)
		return CreateErrorResponse(se.Status, mysql.NewError(mysql.ErrUnknown, msg))
	}
}
