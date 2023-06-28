package mybinlog

import (
	"fmt"
	"runtime/debug"

	"github.com/siddontang/go-mysql/canal"
)

type binlogHandler struct {
	canal.DummyEventHandler
	BinlogParser
}

func (h *binlogHandler) OnRow(e *canal.RowsEvent) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Print(r, " ", string(debug.Stack()))
		}
	}()

	// base value for canal.DeleteAction or canal.InsertAction
	var n = 0
	var k = 1

	if e.Action == canal.UpdateAction {
		n = 1
		k = 2
	}

	for i := n; i < len(e.Rows); i += k {

		key := e.Table.Schema + "." + e.Table.Name

		switch key {
		case User{}.SchemaName() + "." + User{}.TableName():
			user := User{}
			h.GetBinLogData(&user, e, i)
			switch e.Action {
			case canal.UpdateAction:
				oldUser := User{}
				h.GetBinLogData(&oldUser, e, i-1)
				fmt.Printf("User %d name changed from %s to %s\n", user.Id, oldUser.Name, user.Name)
			case canal.InsertAction:
				fmt.Printf("User %d is created with name %s\n", user.Id, user.Name)
			case canal.DeleteAction:
				fmt.Printf("User %d is deleted with name %s\n", user.Id, user.Name)
			default:
				fmt.Printf("Unknown action")
			}
		}

		// h.GetBinLogData(&user, e, i)

		fmt.Printf("%s %s", key, e.Action)
	}
	return nil
}

func (h *binlogHandler) String() string {
	return "binlogHandler"
}

func binlogListener() {
	c, err := getDefaultCanal()
	if err == nil {
		coords, err := c.GetMasterPos()
		if err == nil {
			c.SetEventHandler(&binlogHandler{})
			c.RunFrom(coords)
		}
		// var pos mysql.Position
		// pos.Name = "mysql-bin.000001"
		// pos.Pos = 1
		// c.RunFrom(pos)
	}
}

func getDefaultCanal() (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", "mysql57", 3306)
	cfg.User = "root"
	cfg.Password = "root"
	cfg.Flavor = "mysql"

	cfg.Dump.ExecutionPath = ""

	return canal.NewCanal(cfg)
}
