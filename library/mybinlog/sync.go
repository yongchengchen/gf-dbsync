package mybinlog

import (
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/juju/errors"
	"github.com/siddontang/go/log"
)

type posSaver struct {
	pos   mysql.Position
	force bool
}

type eventHandler struct {
	r *PipeLine
}

type QueryRequest struct {
	DBServer   string
	Query      string
	Parameters []interface{}
	Pos        mysql.Position
}

func (h *eventHandler) OnRotate(header *replication.EventHeader, e *replication.RotateEvent) error {
	pos := mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}

	h.r.syncCh <- posSaver{pos, true}

	return h.r.ctx.Err()
}

func (h *eventHandler) OnTableChanged(header *replication.EventHeader, db string, table string) error {
	err := h.r.updateRule(db, table)
	if err != nil {
		if errors.Cause(err) == schema.ErrTableNotExist {
			fmt.Println("which means table been droped")
			return nil
		}
		if err != ErrRuleNotExist {
			return errors.Trace(err)
		}
	}
	fmt.Printf("OnTableChanged %s.%s %+v\n", db, table, header)

	return nil
}

func (h *eventHandler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, e *replication.QueryEvent) error {
	// query := string(e.Query);
	db := string(e.Schema)
	stmts, _, _ := h.r.parser.Parse(string(e.Query), "", "")
	for _, stmt := range stmts {
		h.handelDDLByStmt(db, stmt, header)
	}

	h.r.syncCh <- posSaver{nextPos, true}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
	h.r.syncCh <- posSaver{nextPos, false}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Print(r, " ", string(debug.Stack()))
		}
	}()

	fmt.Printf("OnRow %+v\n", e.Header.LogPos)

	rule, ok := h.r.rules[ruleKey(e.Table.Schema, e.Table.Name)]
	if !ok {
		fmt.Printf("no rule found for %s %s\n", e.Table.Schema, e.Table.Name)
		return nil
	}

	var reqs []*QueryRequest
	var err error
	switch e.Action {
	case canal.InsertAction:
		reqs, err = h.r.makeInsertRequest(rule, e)
	case canal.DeleteAction:
		reqs, err = h.r.makeDeleteRequest(rule, e)
	case canal.UpdateAction:
		reqs, err = h.r.makeUpdateRequest(rule, e)
	default:
		err = errors.Errorf("invalid rows action %s", e.Action)
	}

	if err != nil {
		h.r.cancel()
		return errors.Errorf("make %s db request err %v, close sync", e.Action, err)
	}

	h.r.syncCh <- reqs

	return h.r.ctx.Err()
}

func (h *eventHandler) OnGTID(e *replication.EventHeader, s mysql.GTIDSet) error {
	fmt.Printf("OnGTID e %+v\n", e)
	fmt.Printf("OnGTID s %+v\n", s)
	return nil
}

func (h *eventHandler) OnPosSynced(header *replication.EventHeader, nextPos mysql.Position, gs mysql.GTIDSet, b bool) error {
	fmt.Printf("OnPosSynced %+v\n", nextPos)
	return nil
}

func (h *eventHandler) String() string {
	return "DBSyncerEventHandler"
}

func (r *PipeLine) syncLoop() {
	bulkSize := 128
	interval := 200 * time.Millisecond

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer r.wg.Done()

	lastSavedTime := time.Now()

	var pos mysql.Position
	reqs := make([]*QueryRequest, 0, bulkSize)
	for {
		needFlush := false
		needSavePos := false

		select {
		case v := <-r.syncCh:
			switch v := v.(type) {
			case posSaver:
				now := time.Now()
				if v.force || now.Sub(lastSavedTime) > 3*time.Second {
					lastSavedTime = now
					needFlush = true
					needSavePos = true
					pos = v.pos
					r.master.SyncPos(v.pos)
				}
			case []*QueryRequest:
				reqs = append(reqs, v...)
				needFlush = len(reqs) >= bulkSize
			}

		case <-ticker.C:
			needFlush = true
		case <-r.ctx.Done():
			return
		}

		if needFlush {
			// TODO: retry some times?
			if err := r.doBulk(reqs); err != nil {
				log.Errorf("do  bulk err %v, close sync", err)
				r.cancel()
				return
			}
			reqs = reqs[0:0]
			// reqs = reqs[0:0]
		}

		if needSavePos {
			if err := r.master.Save(pos); err != nil {
				fmt.Printf("save sync position %s err %+v, close sync\n", pos, err)
				r.cancel()
				return
			}
		}
	}
}

// func (r *PipeLine) makeInsertRequest(rule *Rule, e *canal.RowsEvent) ([]*QueryRequest, error) {
func (r *PipeLine) makeInsertRequest(rule *Rule, e *canal.RowsEvent) ([]*QueryRequest, error) {
	var reqs []*QueryRequest
	for i := 0; i < len(e.Rows); i++ {
		req := r.makeInsertData(rule, e, i)
		if req != nil {
			reqs = append(reqs, req)
		}
	}

	return reqs, nil
}

func (r *PipeLine) makeInsertData(rule *Rule, e *canal.RowsEvent, n int) *QueryRequest {
	tbl2 := rule.ToDB + "." + rule.Table
	l := len(e.Table.Columns)
	if l > len(e.Rows[n]) {
		return nil
	}

	var cols = make([]string, l)
	var phs = make([]string, l)
	var vals = make([]interface{}, l)
	i := 0

	for id, value := range e.Table.Columns {
		cols[i] = value.Name
		phs[i] = "?"
		vals[i] = e.Rows[n][id]
		i++
	}
	q := &QueryRequest{}
	q.DBServer = rule.ToDBServer
	q.Query = fmt.Sprintf("insert into %s (%s) VALUES(%s)", tbl2, strings.Join(cols, ","), strings.Join(phs, ","))
	q.Parameters = vals
	q.Pos.Name = r.master.Position().Name
	q.Pos.Pos = e.Header.LogPos
	return q
}

func (r *PipeLine) makeUpdateRequest(rule *Rule, e *canal.RowsEvent) ([]*QueryRequest, error) {
	var reqs []*QueryRequest
	for i := 1; i < len(e.Rows); i += 2 {
		req := r.makeUpdateData(rule, e, i)
		if req != nil {
			reqs = append(reqs, req)
		}
	}

	return reqs, nil
}

func (r *PipeLine) makeUpdateData(rule *Rule, e *canal.RowsEvent, n int) *QueryRequest {
	tbl2 := rule.ToDB + "." + rule.Table

	l := len(e.Table.Columns)
	if l > len(e.Rows[n]) {
		return nil
	}

	var cols []string
	var vals []interface{}
	var kvs []interface{} //keys' values

	i := 0
	for id, value := range e.Table.Columns {
		if e.Rows[n][id] != e.Rows[n-1][id] {
			cols = append(cols, fmt.Sprintf("%s=?", value.Name))
			vals = append(vals, e.Rows[n][id])
		}
		i++
	}
	for _, id := range rule.PKColumnIds {
		kvs = append(kvs, e.Rows[n-1][id]) //keys' values use old values
	}
	q := &QueryRequest{}
	q.DBServer = rule.ToDBServer
	q.Query = fmt.Sprintf("update %s set %s where %s", tbl2,
		strings.Join(cols, ","), rule.WherePK)
	vals = append(vals, kvs...)
	q.Parameters = vals
	q.Pos.Name = r.master.Position().Name
	q.Pos.Pos = e.Header.LogPos
	return q
}

func (r *PipeLine) makeDeleteRequest(rule *Rule, e *canal.RowsEvent) ([]*QueryRequest, error) {
	var reqs []*QueryRequest
	for i := 0; i < len(e.Rows); i++ {
		req := r.makeDeleteData(rule, e, i)
		if req != nil {
			reqs = append(reqs, req)
		}
	}

	return reqs, nil
}

func (r *PipeLine) makeDeleteData(rule *Rule, e *canal.RowsEvent, n int) *QueryRequest {
	tbl2 := rule.ToDB + "." + rule.Table

	var kvs []interface{} //keys' values
	for _, id := range rule.PKColumnIds {
		kvs = append(kvs, e.Rows[n][id]) //keys' values use old values
	}

	q := &QueryRequest{}
	q.DBServer = rule.ToDBServer
	q.Query = fmt.Sprintf("delete from %s where %s", tbl2, rule.WherePK)
	q.Parameters = kvs
	q.Pos.Name = r.master.Position().Name
	q.Pos.Pos = e.Header.LogPos
	return q
}

func strConvColumnValue(val interface{}) string {
	switch val.(type) {
	case int8:
		// return int64(val.(int8))
		return strconv.FormatInt(int64(val.(int8)), 10)
	case int32:
		return strconv.FormatInt(int64(val.(int32)), 10)
	case int64:
		return strconv.FormatInt(int64(val.(int64)), 10)
	case int:
		return strconv.FormatInt(int64(val.(int)), 10)
	case uint8:
		return strconv.FormatUint(uint64(val.(uint8)), 10)
	case uint16:
		return strconv.FormatUint(uint64(val.(uint16)), 10)
	case uint32:
		return strconv.FormatUint(uint64(val.(uint32)), 10)
	case uint64:
		return strconv.FormatUint(uint64(val.(uint64)), 10)
	case uint:
		return strconv.FormatUint(uint64(val.(uint)), 10)
	case float32:
		return strconv.FormatFloat(float64(val.(float32)), 'f', 6, 32)
	case float64:
		return strconv.FormatFloat(float64(val.(float64)), 'f', 6, 64)
	case []byte:
		return val.(string)
	case string:
		return val.(string)
	case nil:
		return "NULL"
	}
	return fmt.Sprintf("%+v", val)
}

func (r *PipeLine) doBulk(reqs []*QueryRequest) error {
	for _, q := range reqs {
		logCh := "success"
		if _, err := g.DB(q.DBServer).Exec(r.ctx, q.Query, q.Parameters...); err != nil {
			logCh = "fail"
		}

		g.Log(logCh).Infof(r.ctx, "%s:%d [%s]\n%34s! %s",
			q.Pos.Name, q.Pos.Pos, q.DBServer, logCh, q.Query)

		if len(q.Parameters) > 0 {
			var s []string
			for i := 0; i < len(q.Parameters); i++ {
				s = append(s, "%s")
			}
			g.Log(logCh).Infof(r.ctx, "   args:"+strings.Join(s, ","), q.Parameters...)
		}
	}
	return nil
}
