package mybinlog

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/juju/errors"
	"github.com/siddontang/go/log"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gctx"
	"github.com/pingcap/tidb/parser"
)

// ErrRuleNotExist is the error if rule is not defined.
var ErrRuleNotExist = errors.New("rule is not exist")
var ErrTableNotExist = "table is not exist"

type PipeLine struct {
	cfgCtx context.Context

	canal *canal.Canal

	parser *parser.Parser

	rules map[string]*Rule

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup

	// es *elastic.Client

	master *masterInfo

	dumper *MyDumper

	syncCh chan interface{}
}

// NewPipeLine creates the PipeLine from config
func NewPipeLine() (*PipeLine, error) {
	r := new(PipeLine)

	r.cfgCtx = gctx.New()
	r.rules = make(map[string]*Rule)
	r.syncCh = make(chan interface{}, 4096)
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.parser = parser.New()

	var err error
	if r.master, err = loadMasterInfo("./config"); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.newCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.prepareRule(); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.prepareCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	// We must use binlog full row image
	if err = r.canal.CheckBinlogRowImage("FULL"); err != nil {
		return nil, errors.Trace(err)
	}

	return r, nil
}

func (r *PipeLine) newCanal() error {
	r.cfgCtx = gctx.New()
	con, _ := g.Cfg().Get(r.cfgCtx, "canal_on_db")

	mc, _ := g.Cfg().Get(r.cfgCtx, "canaldatabases."+con.String())
	mp := mc.Map()

	cfg := canal.NewDefaultConfig()
	cfg.Addr = mp["address"].(string)
	cfg.User = mp["user"].(string)
	cfg.Password = mp["password"].(string)
	cfg.Flavor = mp["flavor"].(string)
	cfg.Flavor = mp["flavor"].(string)
	// cfg.Charset = mp["charset"].(string)
	cfg.ServerID = uint32(mp["server_id"].(float64))
	// cfg.Dump.ExecutionPath = mp["dump_exec"].(string)
	cfg.Dump.ExecutionPath = ""
	cfg.Dump.DiscardErr = false
	cfg.Dump.SkipMasterData = mp["dump_skip_master_data"].(bool)

	var err error

	if r.dumper, err = NewMyDumper(mp["dump_exec"].(string),
		cfg.Addr, cfg.User, cfg.Password); err != nil {
		return errors.Trace(err)
	}

	r.canal, err = canal.NewCanal(cfg)
	return errors.Trace(err)
}

func (r *PipeLine) prepareCanal() error {

	// 	r.canal.AddDumpDatabases(keys...)
	// }

	r.canal.SetEventHandler(&eventHandler{r})

	return nil
}

func (p *PipeLine) DumpTable(db string, table string, toDB string) (string, error) {
	fmt.Printf("dump table data: %s %s\n", db, table)
	p.dumper.Reset()
	p.dumper.AddTables(db, table)

	tmpFile := fmt.Sprintf("/tmp/dump-%s-%s.sql", db, table)

	if _, err := os.Stat(tmpFile); err == nil {
		os.Remove(tmpFile)
	}
	f, err := os.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// defer close(p.dumper.dumpDoneCh)
	fmt.Printf("dump table data: %s %s\n", db, table)
	return tmpFile, p.dumper.Dump(f, toDB)
}

func (r *PipeLine) newRule(db string, table string) error {
	key := ruleKey(db, table)

	if _, ok := r.rules[key]; ok {
		return errors.Errorf("duplicate source %s, %s defined in config", db, table)
	}

	r.rules[key] = newDefaultRule(db, table)
	return nil
}

func (r *PipeLine) updateRule(db string, table string) error {
	rule, ok := r.rules[ruleKey(db, table)]
	if !ok {
		return ErrRuleNotExist
	}

	if err := rule.prepare(rule.RawKey, r); err != nil {
		fmt.Printf("tony err on updateRule %+v\n", err)
		return errors.Trace(err)
	}

	return nil
}

func (r *PipeLine) prepareRule() error {
	b, _ := g.Cfg().Get(r.cfgCtx, "uselocalrule")
	if b.Bool() {
		crs, _ := g.Cfg().Get(r.cfgCtx, "rules")
		for _, rc := range crs.Array() {
			m := rc.(map[string]interface{})
			s := m["db"].(string)
			t := m["table"].(string)
			key := ruleKey(s, t)
			if _, ok := r.rules[key]; ok {
				return errors.Errorf("duplicated rule %s, %s", s, t)
			}

			rule := newRule(s, t, m["to_dbserver"].(string), m["to_db"].(string))
			if err := rule.prepare(m["keys"], r); err != nil {
				if err.Error() == ErrTableNotExist {
					fmt.Printf("rule %s %s %s", s, t, ErrTableNotExist)
				}
			}
			// fmt.Printf("rules.TableInfo:%+v\n", rule.TableInfo.Columns)

			if rule.TableInfo != nil && len(rule.TableInfo.PKColumns) == 0 {
				return errors.Errorf("%s.%s must have a PK for a column", rule.DB, rule.Table)
			}

			fmt.Printf("rule:%+v\n", rule)

			r.rules[key] = rule
		}
		fmt.Printf("rules:%+v\n", r.rules)
	}

	return nil
}

func ruleKey(db string, table string) string {
	return strings.ToLower(fmt.Sprintf("%s:%s", db, table))
}

func (r *PipeLine) Run() error {
	r.wg.Add(1)
	// canalSyncState.Set(float64(1))
	go r.syncLoop()

	pos := r.master.Position()
	if err := r.canal.RunFrom(pos); err != nil {
		log.Errorf("start canal err %v", err)
		// canalSyncState.Set(0)
		return errors.Trace(err)
	}

	return nil
}

// Ctx returns the internal context for outside use.
func (r *PipeLine) Ctx() context.Context {
	return r.ctx
}

// Close closes the PipeLine
func (r *PipeLine) Close() {
	log.Infof("closing pipeline")

	r.cancel()

	r.canal.Close()

	r.master.Close()

	r.wg.Wait()
}

func isValidTables(tables []string) bool {
	if len(tables) > 1 {
		for _, table := range tables {
			if table == "*" {
				return false
			}
		}
	}
	return true
}

func buildTable(table string) string {
	if table == "*" {
		return "." + table
	}
	return table
}
