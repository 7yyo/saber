package main

import (
	"github.com/go-mysql-org/go-mysql/client"
	tpc_ds "saber/tpc-ds"
)

type DbSource struct {
	h  string
	u  string
	p  string
	db string
}

func main() {

	ds := DbSource{
		h: "172.16.5.133:4000",
		//h:  "10.2.102.28:4000",
		u:  "root",
		p:  "",
		db: "tp",
	}

	conn, err := client.Connect(ds.h, ds.u, ds.p, ds.db)
	if err != nil {
		panic(err)
	}

	j := tpc_ds.Job{
		Conn: *conn,
		Do:   "rows",
	}

	job := "checkResultSet"

	switch job {
	case "partition":
		j.Partition()
	case "analyzePartition":
		j.AnalyzePartition()
	case "checkResultSet":
		j.CheckResultSet()
	case "do":
		j.DoTable()
	}
}
