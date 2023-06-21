package server

import (
	"github.com/XiaoMi/Gaea/core/executor"
	"github.com/XiaoMi/Gaea/mysql"
)

// Response response info
type Response struct {
	RespType int
	Status   uint16
	Data     interface{}
}

const (
	// RespOK means OK message
	RespOK = iota
	// RespResult means Result message
	RespResult
	// RespError means error message
	RespError
	// RespFieldList means field list message
	RespFieldList
	// RespPrepare prepare response message
	RespPrepare
	// RespEOF means EOF message
	RespEOF
	// RespNoop means empty message
	RespNoop
)

// CreateOKResponse create ok response
func CreateOKResponse(status uint16) Response {
	return Response{
		RespType: RespOK,
		Status:   status,
	}
}

// CreateResultResponse create result response
func CreateResultResponse(status uint16, result *mysql.Result) Response {
	return Response{
		RespType: RespResult,
		Status:   status,
		Data:     result,
	}
}

// CreateErrorResponse create error response
func CreateErrorResponse(status uint16, err error) Response {
	return Response{
		RespType: RespError,
		Status:   status,
		Data:     err,
	}
}

// CreateFieldListResponse create field list response
func CreateFieldListResponse(status uint16, fl []*mysql.Field) Response {
	return Response{
		RespType: RespFieldList,
		Status:   status,
		Data:     fl,
	}
}

// CreatePrepareResponse create prepare response
func CreatePrepareResponse(status uint16, stmt *executor.Stmt) Response {
	return Response{
		RespType: RespPrepare,
		Status:   status,
		Data:     stmt,
	}
}

// CreateEOFResponse create eof response
func CreateEOFResponse(status uint16) Response {
	return Response{
		RespType: RespEOF,
		Status:   status,
	}
}

// CreateNoopResponse no op response, for ComStmtClose
func CreateNoopResponse() Response {
	return Response{
		RespType: RespNoop,
	}
}
