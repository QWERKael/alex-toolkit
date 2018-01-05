package atkredis

import (
	"github.com/garyburd/redigo/redis"
	"alex-toolkit/atkbase"
	"strconv"
	"strings"
	"errors"
	"fmt"
)

type RedisConnInfo struct {
	Host, Port, Database, User, Password string
	RedisConn                            redis.Conn
}

//根据mutliaddr或者host、port返回连接
func Connect(ci atkbase.ConnInfo) (rcis []RedisConnInfo, err error) {
	//端口默认为6379
	port := "6379"
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
		database := 0
		if ci.Database != "" {
			database, err = strconv.Atoi(ci.Database)
		}
		if err != nil {
			return nil, err
		}
		password := ci.Password
		ma := strings.Split(ci.MutliAddr, ",")
		for _, address := range ma {
			conn, err := redis.Dial("tcp", address, redis.DialDatabase(database), redis.DialPassword(password))
			if err != nil {
				return nil, err
			}
			addr := strings.Split(address, ":")
			rci := RedisConnInfo{string(addr[0]), string(addr[1]), ci.Database, ci.User, ci.Password, conn}
			rcis = append(rcis, rci)
		}
		return rcis, err
	}
	return nil, errors.New("无法获取到链接地址")
}

//返回单连接的scan结果
func Scan(rci RedisConnInfo, args ... interface{}) (scanResults []string, err error) {
	match := args[0]
	count := args[1]
	var (
		cursor int64
		items  []string
	)
	defer rci.RedisConn.Close()

	results := make([]string, 0)

	for {
		values, err := redis.Values(rci.RedisConn.Do("SCAN", cursor, "match", match, "count", count))
		if err != nil {
			return nil, err
		}

		values, err = redis.Scan(values, &cursor, &items)
		if err != nil {
			return nil, err
		}
		results = append(results, items...)

		if cursor == 0 {
			break
		}
	}
	return results, err
}

//返回单连接的del结果
func Del(rci RedisConnInfo, args ... interface{}) (delResults []string, err error) {
	match := args[0]
	count := args[1]
	var (
		cursor int64
		items  []string
	)
	defer rci.RedisConn.Close()

	//results := make([]string, 0)

	for {
		values, err := redis.Values(rci.RedisConn.Do("SCAN", cursor, "match", match, "count", count))
		if err != nil {
			return nil, err
		}

		values, err = redis.Scan(values, &cursor, &items)
		if err != nil {
			return nil, err
		}
		//results = append(results, items...)
		for _, item := range items {
			values, err := rci.RedisConn.Do("DEL", item)
			//fmt.Println(reflect.TypeOf(values))
			if values == int64(1) {
				fmt.Println("redis "+rci.Host+":"+rci.Port+" delete ", item)
			}
			if err != nil {
				fmt.Println(err)
				return nil, err
			}
			//values, err = redis.Scan(values, &cursor, &items)
			//results = append(results, items...)
		}

		if cursor == 0 {
			break
		}
	}
	return nil, err
}

//返回单个连接的客户端列表
func ClientList(rci RedisConnInfo, _ ... interface{}) (Results []string, err error) {
	defer rci.RedisConn.Close()
	values, err := redis.String(rci.RedisConn.Do("CLIENT", "LIST"))
	cls := strings.Split(values, "\n")
	return cls, err
}

//断开单个连接的指定ip客户端
func KillClient(rci RedisConnInfo, args ... interface{}) (Results []string, err error) {
	ips := args[0]
	defer rci.RedisConn.Close()
	values, err := redis.String(rci.RedisConn.Do("CLIENT", "LIST"))
	cls := strings.Split(values, "\n")
	for _, cl := range cls {
		for _, ip := range strings.Split(ips.(string), ",") {
			if strings.Contains(cl, "addr="+ip+":") {
				addr := strings.Split(strings.Split(cl, " ")[1], "=")[1]
				values, err := rci.RedisConn.Do("CLIENT", "KILL", addr)
				//fmt.Println(reflect.TypeOf(values))
				//fmt.Println(values)
				if values == "OK" {
					fmt.Println("redis "+rci.Host+":"+rci.Port+" kill client ", addr)
				}
				if err != nil {
					fmt.Println(err)
					return nil, err
				}
			}
		}
	}
	return nil, err
}

//内存使用率监控
//func MemoryMonitor(rci RedisConnInfo, _ ... interface{}) (Results []string, err error) {
//	err = ui.Init()
//	if err != nil {
//		panic(err)
//	}
//	defer ui.Close()
//	ui.Body.AddRows(
//		ui.NewRow(
//			ui.NewCol(6, 0, sp)))
//
//	ui.Body.Align()
//
//	ui.Render(ui.Body)
//
//	ui.Handle("/sys/kbd/q", func(ui.Event) {
//		ui.StopLoop()
//	})
//
//	defer rci.RedisConn.Close()
//	values, err := redis.String(rci.RedisConn.Do("CLIENT", "LIST"))
//	cls := strings.Split(values, "\n")
//	return nil, err
//}

//已使用多连接模板方式代替多链接扫描
//返回多连接的scan结果
//func MutliScan(ci atkbase.ConnInfo, mconn []redis.Conn, match string, count int) (mutliScanResults [][]string, err error) {
//	chs := make(chan []string, len(mconn))
//	ma := strings.Split(ci.MutliAddr, ",")
//	for i, conn := range mconn {
//		go func(i int, conn redis.Conn) (err error) {
//			results, err := Scan(conn, match, count)
//			results = append([]string{ma[i]}, results...)
//			chs <- results
//			return
//		}(i, conn)
//		if err != nil {
//			return nil, err
//		}
//	}
//	for i := 0; i < len(mconn); i++ {
//		r := <-chs
//		mutliScanResults = append(mutliScanResults, r)
//	}
//	return
//}

//多连接执行模板
func MutliExec(rcis []RedisConnInfo,
	f func(rci RedisConnInfo, fargs ... interface{}) (results []string, err error),
	args ... interface{}) (mutliScanResults [][]string, err error) {
	chs := make(chan []string, len(rcis))
	for i, rci := range rcis {
		go func(i int, rci RedisConnInfo) (err error) {
			results, err := f(rci, args...)
			if len(results) == 0 {
				results = []string{rci.Host + ":" + rci.Port}
			} else {
				results = append([]string{rci.Host + ":" + rci.Port}, results...)
			}
			chs <- results
			return
		}(i, rci)
		if err != nil {
			return nil, err
		}
	}
	for i := 0; i < len(rcis); i++ {
		r := <-chs
		mutliScanResults = append(mutliScanResults, r)
	}
	return
}

//func main() {
//	var c, err = connect()
//	if err != nil {
//		fmt.Println("redis连接错误：", err)
//		return
//	}
//	defer c.Close()
//
//	s, err := scan(c)
//	if err != nil {
//		fmt.Println("scan函数错误：", err)
//	}
//	fmt.Println(s)
//
//	var a1 string
//	a1, err = redis.String(c.Do("get", "a1"))
//	fmt.Println("here")
//	if err != nil {
//		fmt.Println("get错误：", err)
//	}
//	fmt.Println(a1)
//}
