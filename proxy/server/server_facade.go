package server

import (
	"fmt"
	"github.com/XiaoMi/Gaea/log"
	"github.com/XiaoMi/Gaea/mysql"
)

// ExecuteCommand execute command
func ExecuteCommand(cmd byte, data []byte, se *SessionExecutor) Response {
	switch cmd {
	case mysql.ComQuit:
		se.handleRollback(nil)
		// https://dev.mysql.com/doc/internals/en/com-quit.html
		// either a connection close or a OK_Packet, OK_Packet will cause client RST sometimes, but doesn't affect sql execute
		return CreateNoopResponse()
	case mysql.ComQuery: // data type: string[EOF]
		sql := string(data)
		// handle phase
		r, err := se.handleQuery(sql)
		if err != nil {
			return CreateErrorResponse(se.status, err)
		}
		return CreateResultResponse(se.status, r)
	case mysql.ComPing:
		return CreateOKResponse(se.status)
	case mysql.ComInitDB:
		db := string(data)
		// handle phase
		err := se.handleUseDB(db)
		if err != nil {
			return CreateErrorResponse(se.status, err)
		}
		return CreateOKResponse(se.status)
	case mysql.ComFieldList:
		fs, err := se.handleFieldList(data)
		if err != nil {
			return CreateErrorResponse(se.status, err)
		}
		return CreateFieldListResponse(se.status, fs)
	case mysql.ComStmtPrepare:
		sql := string(data)
		stmt, err := se.handleStmtPrepare(sql)
		if err != nil {
			return CreateErrorResponse(se.status, err)
		}
		return CreatePrepareResponse(se.status, stmt)
	case mysql.ComStmtExecute:
		values := make([]byte, len(data))
		copy(values, data)
		r, err := se.handleStmtExecute(values)
		if err != nil {
			return CreateErrorResponse(se.status, err)
		}
		return CreateResultResponse(se.status, r)
	case mysql.ComStmtClose: // no response
		if err := se.handleStmtClose(data); err != nil {
			return CreateErrorResponse(se.status, err)
		}
		return CreateNoopResponse()
	case mysql.ComStmtSendLongData: // no response
		values := make([]byte, len(data))
		copy(values, data)
		if err := se.handleStmtSendLongData(values); err != nil {
			return CreateErrorResponse(se.status, err)
		}
		return CreateNoopResponse()
	case mysql.ComStmtReset:
		if err := se.handleStmtReset(data); err != nil {
			return CreateErrorResponse(se.status, err)
		}
		return CreateOKResponse(se.status)
	case mysql.ComSetOption:
		return CreateEOFResponse(se.status)
	default:
		msg := fmt.Sprintf("command %d not supported now", cmd)
		log.Warn("dispatch command failed, error: %s", msg)
		return CreateErrorResponse(se.status, mysql.NewError(mysql.ErrUnknown, msg))
	}
}
