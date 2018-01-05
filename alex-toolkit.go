package main

import (
	"alex-toolkit/atkbase"
	"alex-toolkit/atkmysql"
	"alex-toolkit/atkredis"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli"
)

func main() {
	var ci atkbase.ConnInfo

	app := cli.NewApp()
	app.HideHelp = true
	app.Name = "alex-toolkit"
	app.Usage = "a toolkit from alex"
	app.Version = "alpha 0.0.1"
	app.Commands = []cli.Command{
		{
			Name:    "redis", //该命令下是redis系列命令
			Aliases: []string{"r"},
			Usage:   "with this cmd, you can use some redis tools",
			Subcommands: []cli.Command{
				{
					Name:    "scan", //scan命令
					Aliases: []string{"s"},
					Usage:   "扫描Redis中的键",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "match,m",
							Usage: "scan命令的匹配参数",
						},
						cli.IntFlag{
							Name:  "count,c",
							Value: 1000,
							Usage: "每次获取的key数",
						},
					},
					Action: func(c *cli.Context) (err error) {
						//获取多个连接
						rcis, err := atkredis.Connect(ci)
						if err != nil {
							fmt.Println("Redis连接错误：", err)
						}
						//对多链接进行扫描
						scanResults, err := atkredis.MutliExec(rcis, atkredis.Scan, c.String("match"), c.Int("count"))
						if err != nil {
							fmt.Println("Redis SCAN命令错误：", err)
						}
						//打印扫描结果
						for _, s := range scanResults {
							fmt.Printf("扫描地址%c[1;40;32m%s:%c[0m\n", 0x1B, s[0], 0x1B)
							for _, v := range s[1:] {
								fmt.Println(v)
							}
							fmt.Println()
						}
						return
					},
				},
				{
					Name:    "del", //del命令
					Aliases: []string{"d"},
					Usage:   "删除Redis中的键",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "match,m",
							Usage: "scan命令的匹配参数",
						},
						cli.IntFlag{
							Name:  "count,c",
							Value: 1000,
							Usage: "每次获取的key数",
						},
					},
					Action: func(c *cli.Context) (err error) {
						//获取多个连接
						rcis, err := atkredis.Connect(ci)
						if err != nil {
							fmt.Println("Redis连接错误：", err)
						}
						//对多链接进行删除
						_, err = atkredis.MutliExec(rcis, atkredis.Del, c.String("match"), c.Int("count"))
						if err != nil {
							fmt.Println("Redis Del命令错误：", err)
						}
						return
					},
				},
				{
					Name:    "clientlist", //clientlist命令
					Aliases: []string{"cl"},
					Usage:   "获取所有正在访问Redis的连接地址",
					Action: func(c *cli.Context) (err error) {
						//获取多个连接
						rcis, err := atkredis.Connect(ci)
						if err != nil {
							fmt.Println("Redis连接错误：", err)
						}
						//对多链接进行连接来源检测
						results, err := atkredis.MutliExec(rcis, atkredis.ClientList)
						if err != nil {
							fmt.Println("Redis ClentList命令错误：", err)
						}
						//打印检测结果
						for _, s := range results {
							fmt.Printf("扫描地址%c[1;40;32m%s:%c[0m\n", 0x1B, s[0], 0x1B)
							for _, v := range s[1:] {
								fmt.Println(v)
							}
							fmt.Println()
						}
						return
					},
				},
				{
					Name:    "killclient", //clientlist命令
					Aliases: []string{"kc"},
					Usage:   "断开指定ip的连接",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "ip",
							Usage: "指定ip,多ip用逗号分隔",
						},
					},
					Action: func(c *cli.Context) (err error) {
						//获取多个连接
						rcis, err := atkredis.Connect(ci)
						if err != nil {
							fmt.Println("Redis连接错误：", err)
						}
						//对多链接进行连接来源检测
						_, err = atkredis.MutliExec(rcis, atkredis.KillClient, c.String("ip"))
						if err != nil {
							fmt.Println("Redis ClentList命令错误：", err)
						}
						return
					},
				},
				//{
				//	Name:    "memorymonitor", //监控内存
				//	Aliases: []string{"mm"},
				//	Usage:   "监控内存使用量",
				//	Action: func(c *cli.Context) (err error) {
				//		//获取多个连接
				//		rcis, err := atkredis.Connect(ci)
				//		if err != nil {
				//			fmt.Println("Redis连接错误：", err)
				//		}
				//		//对多链接进行连接来源检测
				//		_, err = atkredis.MutliExec(rcis, atkredis.MemoryMonitor)
				//		if err != nil {
				//			fmt.Println("Redis ClentList命令错误：", err)
				//		}
				//		return
				//	},
				//},
			},
		},

		{
			Name:    "mysql", //该命令下是mysql系列命令
			Aliases: []string{"m"},
			Usage:   "with this cmd, you can use some mysql tools",
			Subcommands: []cli.Command{
				{
					Name:    "query", //scan命令
					Aliases: []string{"s"},
					Usage:   "扫描Redis中的键",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "stmt,s",
							Usage: "输入查询语句",
						},
						cli.BoolFlag{
							Name:  "connect,c",
							Usage: "查询连接信息",
						},
					},
					Action: func(c *cli.Context) (err error) {
						//获取多个连接
						mcis, err := atkmysql.Connect(ci)
						if err != nil {
							fmt.Println("MySQL连接错误：", err)
						}
						//对多链接进行扫描
						var queryResults []atkmysql.MySQLResults
						var stmt string
						switch {
						case c.String("stmt") != "":
							stmt = c.String("stmt")
						case c.Bool("connect"):
							stmt = `SELECT
		USER,
		LEFT(host,LOCATE(':',host)-1) AS HOST,
		count(*) AS CONNECTS
FROM information_schema.PROCESSLIST
WHERE 1=1
GROUP BY USER,LEFT(host,LOCATE(':',host)-1)
ORDER BY count(*) desc`
						default:
							return errors.New("缺少必要参数")
						}
						queryResults, err = atkmysql.MutliExec(mcis, atkmysql.Query, stmt)
						if err != nil {
							fmt.Println("MySQL SCAN命令错误：", err)
						}
						//打印扫描结果
						for _, q := range queryResults {
							fmt.Printf("扫描地址%c[1;40;32m%s:%c[0m\n", 0x1B, q.Source, 0x1B)
							table := tablewriter.NewWriter(os.Stdout)
							table.SetHeader(q.Names)
							for _, v := range q.Results {
								// fmt.Println(v)
								table.Append(v)
							}
							footer := make([]string, len(q.Names))
							footer[0] = "Count"
							footer[1] = strconv.Itoa(q.Count)
							table.SetFooter(footer)
							table.Render()
							fmt.Println()
						}
						return
					},
				},
			},
		},
	}
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "host,h",
			Destination: &ci.Host,
		},
		cli.StringFlag{
			Name:        "port,P",
			Destination: &ci.Port,
		},
		cli.StringFlag{
			Name:        "database,D",
			Destination: &ci.Database,
		},
		cli.StringFlag{
			Name:        "user,u",
			Destination: &ci.User,
		},
		cli.StringFlag{
			Name:        "password,p",
			Destination: &ci.Password,
		},
		cli.StringFlag{
			Name:        "mutliaddr",
			Destination: &ci.MutliAddr,
			Usage:       "输入多个ip:port形式的地址，以逗号分隔，可以进行多实例批量查询",
		},
	}
	app.Run(os.Args)
}
