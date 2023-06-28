package router

import (
	"io/ioutil"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/net/ghttp"
	"github.com/gogf/gf/v2/os/gfile"
	// "github.com/yongchengchen/gf-dbsync/app/api"
)

func init() {
	s := g.Server()

	s.Group("/api/v1", func(group *ghttp.RouterGroup) {
		// group.Middleware(
		// 	service.Middleware.Ctx,
		// 	service.Middleware.CORS,
		// 	service.Middleware.InnerAuth,
		// )
	})

	// s.BindHandler("/ws/:token", api.WsSsh)

	path := gfile.MainPkgPath() + "/dist"

	s.BindStatusHandler(404, func(r *ghttp.Request) {
		// r.Response.w
		file := path + "/index.html"
		c, err := ioutil.ReadFile(file)
		if err != nil {
			r.Response.WriteStatus(404, "Not Found")
		}
		r.Response.WriteStatus(200, c)
	})

	// logrus.Println(path)
	s.SetServerRoot(path)
	s.SetPort(8199)
}
