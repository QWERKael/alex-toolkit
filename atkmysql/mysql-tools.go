package atkmysql

import (
	"alex-toolkit/atkbase"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLConnInfo struct {
	Host, Port, Database, User, Password string
	MySQLConn                            *sql.DB
}

type MySQLResults struct {
	Source  string
	Names   []string
	Count   int
	Results [][]string
}

//根据mutliaddr或者host、port返回连接
func Connect(ci atkbase.ConnInfo) (mcis []MySQLConnInfo, err error) {
	//端口默认为6379
	port := "3306"
	if ci.Port != "" {
		port = ci.Port
	}
	//如果host存在，则用host:port覆盖mutliaddr
	if ci.Host != "" {
		ci.MutliAddr = ci.Host + ":" + port
	} else if ci.MutliAddr == "" {
		fmt.Println("请输入连接地址")
		return nil, errors.New("没有连接地址")
	}
	//连接多个地址
	if ci.MutliAddr != "" {
		database := "test"
		if ci.Database != "" {
			database = ci.Database
		}
		if err != nil {
			return nil, err
		}
		password := ci.Password
		ma := strings.Split(ci.MutliAddr, ",")
		for _, address := range ma {
			conn, err := sql.Open("mysql", ci.User+":"+password+"@tcp("+address+")/"+database)
			if err != nil {
				return nil, err
			}
			addr := strings.Split(address, ":")
			mci := MySQLConnInfo{string(addr[0]), string(addr[1]), ci.Database, ci.User, ci.Password, conn}
			mcis = append(mcis, mci)
		}
		return mcis, err
	}
	return nil, errors.New("无法获取到链接地址")
}

//返回单连接的scan结果
func Query(mci MySQLConnInfo, args ...interface{}) (queryResults MySQLResults, err error) {
	defer mci.MySQLConn.Close()

	stmt := args[0].(string)
	// stmt := "select * from entity_convert_info limit 10"

	// results := make([][]string, 0)

	rows, err := mci.MySQLConn.Query(stmt)
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()
	// 获取列名列表
	queryResults.Source = mci.Host + ":" + mci.Port
	queryResults.Names, err = rows.Columns()
	queryResults.Count = len(queryResults.Names)
	if err != nil {
		log.Fatal(err)
	}
	// cp = make([]sql.RawBytes, len(columnNames))
	for rows.Next() {
		// 初始化 column pointers
		cp := make([]interface{}, queryResults.Count)
		for i := 0; i < queryResults.Count; i++ {
			cp[i] = new(sql.RawBytes)
		}
		// 使用 Scan 为cp赋值
		err := rows.Scan(cp...)
		if err != nil {
			log.Fatal(err)
		}

		rowResults := make([]string, 0)
		for i := 0; i < queryResults.Count; i++ {
			if rb, ok := cp[i].(*sql.RawBytes); ok {
				rowResults = append(rowResults, string(*rb))
				// fmt.Println(columnNames[i], string(*rb))
				*rb = nil // reset pointer to discard current value to avoid a bug
			} else {
				return queryResults, fmt.Errorf("Cannot convert index %d column %s to type *sql.RawBytes", i, queryResults.Names[i])
			}
		}

		queryResults.Results = append(queryResults.Results, rowResults)
	}

	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	return queryResults, err
}

//多连接执行模板
func MutliExec(mcis []MySQLConnInfo,
	f func(mci MySQLConnInfo, fargs ...interface{}) (queryResults MySQLResults, err error),
	args ...interface{}) (mutliScanResults []MySQLResults, err error) {
	chs := make(chan MySQLResults, len(mcis))
	for i, mci := range mcis {
		go func(i int, mci MySQLConnInfo) (err error) {
			var mr MySQLResults
			mr, err = f(mci, args...)
			chs <- mr
			return
		}(i, mci)
		if err != nil {
			return nil, err
		}
	}
	for i := 0; i < len(mcis); i++ {
		r := <-chs
		mutliScanResults = append(mutliScanResults, r)
	}
	return
}
