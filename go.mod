module github.com/yongchengchen/gf-dbsync

go 1.11

require (
	github.com/BurntSushi/toml v0.4.1
	github.com/go-mysql-org/go-mysql v1.7.0
	github.com/gogf/gf/v2 v2.0.3
	// github.com/gogf/swagger v1.2.0
	github.com/gorilla/websocket v1.4.2
	github.com/json-iterator/go v1.1.12
	github.com/juju/errors v1.0.0
	github.com/logoove/sqlite v1.13.0
	github.com/mattn/go-sqlite3 v1.14.12 // indirect
	github.com/pingcap/errors v0.11.5-0.20210425183316-da1aaba5fb63
	github.com/pingcap/tidb/parser v0.0.0-20221126021158-6b02a5d8ba7d
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726
	github.com/siddontang/go-log v0.0.0-20180807004314-8d05993dda07
	modernc.org/sqlite v1.15.1 // indirect
)

replace github.com/siddontang/go-mysql => github.com/go-mysql-org/go-mysql v1.7.0
