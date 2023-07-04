package client

import (
	"database/sql"
	"github.com/XiaoMi/Gaea/mysql"
)

// Adapt 将mysql.Result适配成 sql.Rows
func Adapt(result *mysql.Result, err error) (*sql.Rows, error) {
	// TODO
	return nil, nil
}
