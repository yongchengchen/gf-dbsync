package mybinlog

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/juju/errors"
)

type Rule struct {
	DB    string `toml:"db"`
	Table string `toml:"table"`

	ToDBServer string `toml:"to_dbserver"`
	ToDB       string `toml:"to_db"`

	RawKey string `toml:"keys"`

	PKColumnIds []int
	//build unique where condition base on PKColumns
	WherePK string

	// // MySQL table information
	TableInfo *schema.Table
}

func newDefaultRule(db string, table string) *Rule {
	r := new(Rule)

	r.DB = db
	r.Table = table

	return r
}

func newRule(db string, table string, toDbServer string, toDB string) *Rule {
	r := new(Rule)

	r.DB = db
	r.Table = table
	r.ToDBServer = toDbServer
	r.ToDB = toDB

	return r
}

func (r *Rule) prepare(keysLine interface{}, p *PipeLine) error {
	var err error
	if keysLine != nil {
		r.RawKey = keysLine.(string)
	}

	if r.TableInfo, err = p.canal.GetTable(r.DB, r.Table); err != nil {
		return errors.Trace(err)
	}

	var wks []string
	if len(r.TableInfo.PKColumns) > 0 {
		r.PKColumnIds = r.TableInfo.PKColumns
		for _, i := range r.TableInfo.PKColumns {
			wks = append(wks, fmt.Sprintf("%s=?", r.TableInfo.Columns[i].Name))
		}
	} else {
		if keysLine == nil {
			return errors.Errorf("%s.%s does not have a PK for a column", r.DB, r.Table)
		}
		keys := strings.Split(r.RawKey, ",")
		var ks []int
		for _, k := range keys {
			wks = append(wks, fmt.Sprintf("%s=?", k))
			for i, c := range r.TableInfo.Columns {
				if c.Name == k {
					ks = append(ks, i)
					break
				}
			}
		}
		r.PKColumnIds = ks
	}
	r.WherePK = strings.Join(wks, " and ")

	return nil
}
