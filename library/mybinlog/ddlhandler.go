package mybinlog

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/parser/ast"
)

func (h *eventHandler) handelDDLByStmt(curDB string, stmt ast.StmtNode, header *replication.EventHeader) {
	var reqs []*QueryRequest
	qs := ""
	toDBServer := ""
	switch t := stmt.(type) {
	case *ast.RenameTableStmt:
		h.handleRenameTable(curDB, t)
	case *ast.AlterTableStmt:
		toDBServer, qs = h.handleAlterTable(curDB, t)
	case *ast.DropTableStmt:
		reqs = h.handleDropTable(curDB, t, header)
	case *ast.CreateTableStmt:
		toDBServer, qs = h.handleCreateTable(curDB, t)
	case *ast.TruncateTableStmt:
		toDBServer, qs = h.handleTruncateTable(curDB, t)
	}
	if len(qs) > 0 {
		q := &QueryRequest{}
		q.DBServer = toDBServer
		q.Query = qs
		q.Pos.Name = h.r.master.Position().Name
		q.Pos.Pos = header.LogPos
		reqs = append(reqs, q)
	}
	if len(reqs) > 0 {
		h.r.syncCh <- reqs
	}

	// h.r.DumpTable("Test", "User", "Test1")
	// <-h.r.dumper.WaitDumpDone()
	// h.r.dumper.Parse("/var/codebase/gf-dbsync/dump.txt")
}

func (h *eventHandler) handleRenameTable(curDB string, t *ast.RenameTableStmt) {
	for _, tableInfo := range t.TableToTables {
		db := tableInfo.NewTable.Schema.String()
		if len(db) == 0 {
			db = curDB
		}
		tbl := tableInfo.NewTable.Name.String()
		k := ruleKey(db, tbl)
		fmt.Printf("rename table %s %s\n", db, tbl)
		if rule, ok := h.r.rules[k]; ok {
			//there's a table renamed as current exists rule's table
			if f, ok := h.r.DumpTable(db, tbl); ok == nil {
				h.r.dumper.ImportSql(f, rule.ToDBServer, rule.ToDB)
				h.r.updateRule(db, tbl)
			}
		}
	}
}

func replaceDDLTableName(query string, db string, table string) string {
	query = strings.Replace(query, fmt.Sprintf("%s.%s", db, table), table, -1)
	query = strings.Replace(query, fmt.Sprintf("`%s`.%s", db, table), table, -1)
	query = strings.Replace(query, fmt.Sprintf("%s.`%s`", db, table), table, -1)
	query = strings.Replace(query, fmt.Sprintf("`%s`.`%s`", db, table), table, -1)
	return query
}

func (h *eventHandler) handleAlterTable(curDB string, t *ast.AlterTableStmt) (string, string) {
	db := t.Table.Schema.String()
	if len(db) == 0 {
		db = curDB
	}
	tn := t.Table.Name.String()
	k := ruleKey(db, tn)
	if rule, ok := h.r.rules[k]; ok {
		return rule.ToDBServer, replaceDDLTableName(t.Text(), db, tn)
	}
	return "", ""
}

func (h *eventHandler) handleCreateTable(curDB string, t *ast.CreateTableStmt) (string, string) {
	db := t.Table.Schema.String()
	if len(db) == 0 {
		db = curDB
	}
	tn := t.Table.Name.String()
	k := ruleKey(db, tn)
	if rule, ok := h.r.rules[k]; ok {
		return rule.ToDBServer, replaceDDLTableName(t.Text(), db, tn)
	}
	return "", ""
}

func (h *eventHandler) handleDropTable(curDB string, t *ast.DropTableStmt, header *replication.EventHeader) []*QueryRequest {
	var reqs []*QueryRequest
	for _, table := range t.Tables {
		db := table.Schema.String()
		if len(db) == 0 {
			db = curDB
		}
		tn := table.Name.String()
		k := ruleKey(db, tn)
		if rule, ok := h.r.rules[k]; ok {
			q := &QueryRequest{}
			q.DBServer = rule.ToDBServer
			q.Query = fmt.Sprintf("drop table %s", tn)
			q.Pos.Name = h.r.master.Position().Name
			q.Pos.Pos = header.LogPos
			reqs = append(reqs, q)
		}
	}
	return reqs
}

func (h *eventHandler) handleTruncateTable(curDB string, t *ast.TruncateTableStmt) (string, string) {
	db := t.Table.Schema.String()
	if len(db) == 0 {
		db = curDB
	}
	tn := t.Table.Name.String()
	k := ruleKey(db, tn)
	if rule, ok := h.r.rules[k]; ok {
		return rule.ToDBServer, fmt.Sprintf("truncate table %s", tn)
	}
	return "", ""
}
