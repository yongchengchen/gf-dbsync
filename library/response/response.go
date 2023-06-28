package response

import (
	"time"

	"github.com/gogf/gf/v2/net/ghttp"
	"github.com/gorilla/websocket"
)

// 数据返回通用JSON数据结构
type JsonResponse struct {
	Code    int         `json:"code"`    // 错误码((0:成功, 1:失败, >1:错误码))
	Message string      `json:"message"` // 提示信息
	Data    interface{} `json:"data"`    // 返回数据(业务接口定义具体数据结构)
	Success bool        `json:"success"` // 返回数据(业务接口定义具体数据结构)
}

// 标准返回结果数据结构封装。
func Json(r *ghttp.Request, code int, message string, data ...interface{}) {
	success := true
	if code >= 400 {
		success = false
	}

	responseData := interface{}(nil)
	if len(data) > 0 {
		responseData = data[0]
	}
	r.Response.WriteJson(JsonResponse{
		Code:    code,
		Message: message,
		Data:    responseData,
		Success: success,
	})
}

// 返回JSON数据并退出当前HTTP执行函数。
func JsonExit(r *ghttp.Request, err int, msg string, data ...interface{}) {
	Json(r, err, msg, data...)
	r.Exit()
}

func WsHandleError(ws *websocket.Conn, err error) bool {
	if err != nil {
		dt := time.Now().Add(time.Second)
		ws.WriteControl(websocket.CloseMessage, []byte(err.Error()), dt)
		return true
	}
	return false
}
