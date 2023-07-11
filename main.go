package main

import (
	"fmt"

	"github.com/gogf/gf/v2/frame/g"
	_ "github.com/yongchengchen/gf-dbsync/boot"
	"github.com/yongchengchen/gf-dbsync/library/mybinlog"

	// "github.com/yongchengchen/gf-dbsync/library/mybinlog"
	_ "github.com/yongchengchen/gf-dbsync/router"
)

// @title       `gf-demo`示例服务API
// @version     1.0
// @description `GoFrame`基础开发框架示例服务API接口文档。
// @schemes     http
func main() {
	// cx := gctx.New()

	// m, _ := g.Cfg().Get(cx, "cannaldatabase.masterdb")
	// // m.Map()
	// fmt.Printf("configs %+v\n", m.Map()["user"])

	// go mybinlog.BinlogListener()
	// fmt.Println("init BinlogListener")

	r, err := mybinlog.NewPipeLine()
	if err != nil {
		// println(errors.ErrorStack(err))
		fmt.Printf("%+v\n", err)
		return
	}

	done := make(chan struct{}, 1)
	go func() {
		r.Run()
		done <- struct{}{}
	}()

	fmt.Println("started. Listening on port....")
	g.Server().Run()
}
