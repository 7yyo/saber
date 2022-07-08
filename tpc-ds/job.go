package tpc_ds

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/soniakeys/meeus/v3/julian"
	"io/ioutil"
	"strconv"
	"strings"
	"time"
)

var partitionKeyMap = map[string]string{
	"catalog_returns": "cr_returned_date_sk",
	"catalog_sales":   "cs_sold_date_sk",
	"inventory":       "inv_date_sk",
	"store_returns":   "sr_returned_date_sk",
	"store_sales":     "ss_sold_date_sk",
	"web_returns":     "wr_returned_date_sk",
	"web_sales":       "ws_sold_date_sk",
}

var partitionTables = []string{
	"catalog_sales",
	"catalog_returns",
	"inventory",
	"store_sales",
	"store_returns",
	"web_sales",
	"web_returns",
}

var catalog_returns_table = "CREATE TABLE `catalog_returns` (" +
	"  `cr_returned_date_sk` int(11) ," +
	"  `cr_returned_time_sk` int(11) DEFAULT NULL," +
	"  `cr_item_sk` int(11) NOT NULL," +
	"  `cr_refunded_customer_sk` int(11) DEFAULT NULL," +
	"  `cr_refunded_cdemo_sk` int(11) DEFAULT NULL," +
	"  `cr_refunded_hdemo_sk` int(11) DEFAULT NULL," +
	"  `cr_refunded_addr_sk` int(11) DEFAULT NULL," +
	"  `cr_returning_customer_sk` int(11) DEFAULT NULL," +
	"  `cr_returning_cdemo_sk` int(11) DEFAULT NULL," +
	"  `cr_returning_hdemo_sk` int(11) DEFAULT NULL," +
	"  `cr_returning_addr_sk` int(11) DEFAULT NULL," +
	"  `cr_call_center_sk` int(11) DEFAULT NULL," +
	"  `cr_catalog_page_sk` int(11) DEFAULT NULL," +
	"  `cr_ship_mode_sk` int(11) DEFAULT NULL," +
	"  `cr_warehouse_sk` int(11) DEFAULT NULL," +
	"  `cr_reason_sk` int(11) DEFAULT NULL," +
	"  `cr_order_number` int(11) NOT NULL," +
	"  `cr_return_quantity` int(11) DEFAULT NULL," +
	"  `cr_return_amount` decimal(7,2) DEFAULT NULL," +
	"  `cr_return_tax` decimal(7,2) DEFAULT NULL," +
	"  `cr_return_amt_inc_tax` decimal(7,2) DEFAULT NULL," +
	"  `cr_fee` decimal(7,2) DEFAULT NULL," +
	"  `cr_return_ship_cost` decimal(7,2) DEFAULT NULL," +
	"  `cr_refunded_cash` decimal(7,2) DEFAULT NULL," +
	"  `cr_reversed_charge` decimal(7,2) DEFAULT NULL," +
	"  `cr_store_credit` decimal(7,2) DEFAULT NULL," +
	"  `cr_net_loss` decimal(7,2) DEFAULT NULL," +
	"  PRIMARY KEY (`cr_item_sk`,`cr_order_number`,`cr_returned_date_sk`)) "

var catalog_sales_table = "CREATE TABLE `catalog_sales` (" +
	"  `cs_sold_date_sk` int(11) ," +
	"  `cs_sold_time_sk` int(11) DEFAULT NULL," +
	"  `cs_ship_date_sk` int(11) DEFAULT NULL," +
	"  `cs_bill_customer_sk` int(11) DEFAULT NULL," +
	"  `cs_bill_cdemo_sk` int(11) DEFAULT NULL," +
	"  `cs_bill_hdemo_sk` int(11) DEFAULT NULL," +
	"  `cs_bill_addr_sk` int(11) DEFAULT NULL," +
	"  `cs_ship_customer_sk` int(11) DEFAULT NULL," +
	"  `cs_ship_cdemo_sk` int(11) DEFAULT NULL," +
	"  `cs_ship_hdemo_sk` int(11) DEFAULT NULL," +
	"  `cs_ship_addr_sk` int(11) DEFAULT NULL," +
	"  `cs_call_center_sk` int(11) DEFAULT NULL," +
	"  `cs_catalog_page_sk` int(11) DEFAULT NULL," +
	"  `cs_ship_mode_sk` int(11) DEFAULT NULL," +
	"  `cs_warehouse_sk` int(11) DEFAULT NULL," +
	"  `cs_item_sk` int(11) NOT NULL," +
	"  `cs_promo_sk` int(11) DEFAULT NULL," +
	"  `cs_order_number` int(11) NOT NULL," +
	"  `cs_quantity` int(11) DEFAULT NULL," +
	"  `cs_wholesale_cost` decimal(7,2) DEFAULT NULL," +
	"  `cs_list_price` decimal(7,2) DEFAULT NULL," +
	"  `cs_sales_price` decimal(7,2) DEFAULT NULL," +
	"  `cs_ext_discount_amt` decimal(7,2) DEFAULT NULL," +
	"  `cs_ext_sales_price` decimal(7,2) DEFAULT NULL," +
	"  `cs_ext_wholesale_cost` decimal(7,2) DEFAULT NULL," +
	"  `cs_ext_list_price` decimal(7,2) DEFAULT NULL," +
	"  `cs_ext_tax` decimal(7,2) DEFAULT NULL," +
	"  `cs_coupon_amt` decimal(7,2) DEFAULT NULL," +
	"  `cs_ext_ship_cost` decimal(7,2) DEFAULT NULL," +
	"  `cs_net_paid` decimal(7,2) DEFAULT NULL," +
	"  `cs_net_paid_inc_tax` decimal(7,2) DEFAULT NULL," +
	"  `cs_net_paid_inc_ship` decimal(7,2) DEFAULT NULL," +
	"  `cs_net_paid_inc_ship_tax` decimal(7,2) DEFAULT NULL," +
	"  `cs_net_profit` decimal(7,2) DEFAULT NULL," +
	"  PRIMARY KEY (`cs_item_sk`,`cs_order_number`,`cs_sold_date_sk`)) "

var inventory_table = "CREATE TABLE `inventory` (" +
	"  `inv_date_sk` int(11) ," +
	"  `inv_item_sk` int(11) NOT NULL," +
	"  `inv_warehouse_sk` int(11) NOT NULL," +
	"  `inv_quantity_on_hand` int(11) DEFAULT NULL," +
	"  PRIMARY KEY (`inv_date_sk`,`inv_item_sk`,`inv_warehouse_sk`)) "

var store_returns_table = "CREATE TABLE `store_returns` (" +
	"  `sr_returned_date_sk` int(11) ," +
	"  `sr_return_time_sk` int(11) DEFAULT NULL," +
	"  `sr_item_sk` int(11) NOT NULL," +
	"  `sr_customer_sk` int(11) DEFAULT NULL," +
	"  `sr_cdemo_sk` int(11) DEFAULT NULL," +
	"  `sr_hdemo_sk` int(11) DEFAULT NULL," +
	"  `sr_addr_sk` int(11) DEFAULT NULL," +
	"  `sr_store_sk` int(11) DEFAULT NULL," +
	"  `sr_reason_sk` int(11) DEFAULT NULL," +
	"  `sr_ticket_number` int(11) NOT NULL," +
	"  `sr_return_quantity` int(11) DEFAULT NULL," +
	"  `sr_return_amt` decimal(7,2) DEFAULT NULL," +
	"  `sr_return_tax` decimal(7,2) DEFAULT NULL," +
	"  `sr_return_amt_inc_tax` decimal(7,2) DEFAULT NULL," +
	"  `sr_fee` decimal(7,2) DEFAULT NULL," +
	"  `sr_return_ship_cost` decimal(7,2) DEFAULT NULL," +
	"  `sr_refunded_cash` decimal(7,2) DEFAULT NULL," +
	"  `sr_reversed_charge` decimal(7,2) DEFAULT NULL," +
	"  `sr_store_credit` decimal(7,2) DEFAULT NULL," +
	"  `sr_net_loss` decimal(7,2) DEFAULT NULL," +
	"  PRIMARY KEY (`sr_item_sk`,`sr_ticket_number`,`sr_returned_date_sk`)) "

var store_sales_table = "CREATE TABLE `store_sales` (" +
	"  `ss_sold_date_sk` int(11) ," +
	"  `ss_sold_time_sk` int(11) DEFAULT NULL," +
	"  `ss_item_sk` int(11) NOT NULL," +
	"  `ss_customer_sk` int(11) DEFAULT NULL," +
	"  `ss_cdemo_sk` int(11) DEFAULT NULL," +
	"  `ss_hdemo_sk` int(11) DEFAULT NULL," +
	"  `ss_addr_sk` int(11) DEFAULT NULL," +
	"  `ss_store_sk` int(11) DEFAULT NULL," +
	"  `ss_promo_sk` int(11) DEFAULT NULL," +
	"  `ss_ticket_number` int(11) NOT NULL," +
	"  `ss_quantity` int(11) DEFAULT NULL," +
	"  `ss_wholesale_cost` decimal(7,2) DEFAULT NULL," +
	"  `ss_list_price` decimal(7,2) DEFAULT NULL," +
	"  `ss_sales_price` decimal(7,2) DEFAULT NULL," +
	"  `ss_ext_discount_amt` decimal(7,2) DEFAULT NULL," +
	"  `ss_ext_sales_price` decimal(7,2) DEFAULT NULL," +
	"  `ss_ext_wholesale_cost` decimal(7,2) DEFAULT NULL," +
	"  `ss_ext_list_price` decimal(7,2) DEFAULT NULL," +
	"  `ss_ext_tax` decimal(7,2) DEFAULT NULL," +
	"  `ss_coupon_amt` decimal(7,2) DEFAULT NULL," +
	"  `ss_net_paid` decimal(7,2) DEFAULT NULL," +
	"  `ss_net_paid_inc_tax` decimal(7,2) DEFAULT NULL," +
	"  `ss_net_profit` decimal(7,2) DEFAULT NULL," +
	"  PRIMARY KEY (`ss_item_sk`,`ss_ticket_number`,`ss_sold_date_sk`)) "

var web_returns_table = "CREATE TABLE `web_returns` (" +
	"  `wr_returned_date_sk` int(11) ," +
	"  `wr_returned_time_sk` int(11) DEFAULT NULL," +
	"  `wr_item_sk` int(11) NOT NULL," +
	"  `wr_refunded_customer_sk` int(11) DEFAULT NULL," +
	"  `wr_refunded_cdemo_sk` int(11) DEFAULT NULL," +
	"  `wr_refunded_hdemo_sk` int(11) DEFAULT NULL," +
	"  `wr_refunded_addr_sk` int(11) DEFAULT NULL," +
	"  `wr_returning_customer_sk` int(11) DEFAULT NULL," +
	"  `wr_returning_cdemo_sk` int(11) DEFAULT NULL," +
	"  `wr_returning_hdemo_sk` int(11) DEFAULT NULL," +
	"  `wr_returning_addr_sk` int(11) DEFAULT NULL," +
	"  `wr_web_page_sk` int(11) DEFAULT NULL," +
	"  `wr_reason_sk` int(11) DEFAULT NULL," +
	"  `wr_order_number` int(11) NOT NULL," +
	"  `wr_return_quantity` int(11) DEFAULT NULL," +
	"  `wr_return_amt` decimal(7,2) DEFAULT NULL," +
	"  `wr_return_tax` decimal(7,2) DEFAULT NULL," +
	"  `wr_return_amt_inc_tax` decimal(7,2) DEFAULT NULL," +
	"  `wr_fee` decimal(7,2) DEFAULT NULL," +
	"  `wr_return_ship_cost` decimal(7,2) DEFAULT NULL," +
	"  `wr_refunded_cash` decimal(7,2) DEFAULT NULL," +
	"  `wr_reversed_charge` decimal(7,2) DEFAULT NULL, " +
	" `wr_account_credit` decimal(7,2) DEFAULT NULL," +
	"  `wr_net_loss` decimal(7,2) DEFAULT NULL," +
	"  PRIMARY KEY (`wr_item_sk`,`wr_order_number`,`wr_returned_date_sk`)) "

var web_sales_table = "CREATE TABLE `web_sales` (" +
	"  `ws_sold_date_sk` int(11) ," +
	"  `ws_sold_time_sk` int(11) DEFAULT NULL," +
	"  `ws_ship_date_sk` int(11) DEFAULT NULL," +
	"  `ws_item_sk` int(11) NOT NULL," +
	"  `ws_bill_customer_sk` int(11) DEFAULT NULL," +
	"  `ws_bill_cdemo_sk` int(11) DEFAULT NULL," +
	"  `ws_bill_hdemo_sk` int(11) DEFAULT NULL," +
	"  `ws_bill_addr_sk` int(11) DEFAULT NULL," +
	"  `ws_ship_customer_sk` int(11) DEFAULT NULL," +
	"  `ws_ship_cdemo_sk` int(11) DEFAULT NULL," +
	"  `ws_ship_hdemo_sk` int(11) DEFAULT NULL," +
	"  `ws_ship_addr_sk` int(11) DEFAULT NULL," +
	"  `ws_web_page_sk` int(11) DEFAULT NULL," +
	"  `ws_web_site_sk` int(11) DEFAULT NULL," +
	"  `ws_ship_mode_sk` int(11) DEFAULT NULL," +
	"  `ws_warehouse_sk` int(11) DEFAULT NULL," +
	"  `ws_promo_sk` int(11) DEFAULT NULL," +
	"  `ws_order_number` int(11) NOT NULL," +
	"  `ws_quantity` int(11) DEFAULT NULL," +
	"  `ws_wholesale_cost` decimal(7,2) DEFAULT NULL," +
	"  `ws_list_price` decimal(7,2) DEFAULT NULL," +
	"  `ws_sales_price` decimal(7,2) DEFAULT NULL," +
	"  `ws_ext_discount_amt` decimal(7,2) DEFAULT NULL," +
	"  `ws_ext_sales_price` decimal(7,2) DEFAULT NULL," +
	"  `ws_ext_wholesale_cost` decimal(7,2) DEFAULT NULL," +
	"  `ws_ext_list_price` decimal(7,2) DEFAULT NULL," +
	"  `ws_ext_tax` decimal(7,2) DEFAULT NULL," +
	"  `ws_coupon_amt` decimal(7,2) DEFAULT NULL," +
	"  `ws_ext_ship_cost` decimal(7,2) DEFAULT NULL," +
	"  `ws_net_paid` decimal(7,2) DEFAULT NULL," +
	"  `ws_net_paid_inc_tax` decimal(7,2) DEFAULT NULL," +
	"  `ws_net_paid_inc_ship` decimal(7,2) DEFAULT NULL," +
	"  `ws_net_paid_inc_ship_tax` decimal(7,2) DEFAULT NULL," +
	"  `ws_net_profit` decimal(7,2) DEFAULT NULL," +
	"  PRIMARY KEY (`ws_item_sk`,`ws_order_number`,`ws_sold_date_sk`)) "

type Job struct {
	Conn client.Conn
	Do   string
}

func (j *Job) Partition() {
	for tbl, key := range partitionKeyMap {

		r, err := j.Conn.Execute(fmt.Sprintf(`select distinct %s from test.%s order by %s;`, key, tbl, key))
		if err != nil {
			panic(err)
		}

		pSQL := fmt.Sprintf(" partition by range (%s)(\n", key)

		var partitions []string
		var partition string
		var dateChunk string

		first := true
		for _, row := range r.Values {
			for _, v := range row {
				if v.AsInt64() == 0 {
					pSQL += "partition p0 values less than (1),\n"
				} else {
					t := julian.JDToTime(float64(v.AsInt64())).String()
					if first {
						first = false
						dateChunk = doDateChunk(t)
						partition = strconv.Itoa(int(v.AsInt64()))
					} else {
						if dateChunk != doDateChunk(t) {
							dateChunk = doDateChunk(t)
							partitions = append(partitions, partition)
							partition = strconv.Itoa(int(v.AsInt64()))
						}
					}
				}
			}
		}
		for i := 0; i < len(partitions); i++ {
			pSQL += fmt.Sprintf("partition p%d values less than (%s),\n", i+1, partitions[i])
		}
		pSQL += "partition pMax values less than (maxvalue));"

		switch tbl {
		case "catalog_sales":
			_, err = j.Conn.Execute(catalog_sales_table + pSQL)
		case "catalog_returns":
			_, err = j.Conn.Execute(catalog_returns_table + pSQL)
		case "inventory":
			_, err = j.Conn.Execute(inventory_table + pSQL)
		case "store_sales":
			_, err = j.Conn.Execute(store_sales_table + pSQL)
		case "store_returns":
			_, err = j.Conn.Execute(store_returns_table + pSQL)
		case "web_sales":
			_, err = j.Conn.Execute(web_sales_table + pSQL)
		case "web_returns":
			_, err = j.Conn.Execute(web_returns_table + pSQL)
		default:
			panic("unknown table")
		}
		if err != nil {
			panic(err)
		}
		fmt.Printf("create table %s success, min: %s, max: %s\n", tbl, partitions[0], partitions[len(partitions)-1])
	}
}

func doDateChunk(time string) string {
	return strings.Split(time, "-")[0] + "-" + strings.Split(time, "-")[1]
}

func (j *Job) AnalyzePartition() {
	for _, tbl := range partitionTables {
		_, err := j.Conn.Execute("set global tidb_partition_prune_mode = 'dynamic';")
		if err != nil {
			panic(err)
		}
		r, err := j.Conn.Execute(fmt.Sprintf("select partition_name from information_schema.partitions where table_name = '%s' and table_schema = '%s';", tbl, j.Conn.GetDB()))
		if err != nil {
			panic(err)
		}
		for _, row := range r.Values {
			fmt.Printf("analyze table %s partition %s\n", tbl, row[0].AsString())
			startTime := time.Now()
			_, err := j.Conn.Execute(fmt.Sprintf("analyze table %s partition %s;", tbl, row[0].AsString()))
			if err != nil {
				panic(err)
			}
			fmt.Printf("complete, duration: %s \n", time.Since(startTime))
		}
	}

}

func (j *Job) DoTable() {

	r, err := j.Conn.Execute(fmt.Sprintf("show tables;"))
	if err != nil {
		panic(err)
	}

	for _, row := range r.Values {
		table := row[0].AsString()
		switch j.Do {
		case "rows":
			r, err := j.Conn.Execute(fmt.Sprintf("select count(*) from %s;", table))
			if err != nil {
				fmt.Println(err)
				continue
			}
			for _, row := range r.Values {
				fmt.Printf("%s: %d\n", table, row[0].AsInt64())
			}
		case "analyze":
			startTime := time.Now()
			_, err := j.Conn.Execute(fmt.Sprintf("%s table %s;", j.Do, row[0].AsString()))
			if err != nil {
				panic(err)
			}
			fmt.Printf("%s table %s complete, duration: %s\n", j.Do, row[0].AsString(), time.Since(startTime))
		case "":
			_, err := j.Conn.Execute(fmt.Sprintf("alter table %s set tiflash mode fast;", row[0].AsString()))
			if err != nil {
				panic(err)
			}
			fmt.Printf("alter table %s set tiflash mode fast;\n", row[0].AsString())
		case "normalMode":
			_, err := j.Conn.Execute(fmt.Sprintf("alter table %s set tiflash mode normal;", row[0].AsString()))
			if err != nil {
				panic(err)
			}
			fmt.Printf("alter table %s set tiflash mode normal;\n", row[0].AsString())
		case "compact":
			_, err := j.Conn.Execute(fmt.Sprintf("alter table %s compact tiflash replica;", row[0].AsString()))
			if err != nil {
				panic(err)
			}
			fmt.Printf("alter table %s compact tiflash replica;\n", row[0].AsString())
		}
	}
}

var variables = []string{
	"set @@tidb_mem_quota_query = 429496729600;",
	"set @@tidb_allow_mpp = 1;",
	"set @@tidb_isolation_read_engines='tiflash';",
}

func (j *Job) CheckResultSet() {

	rFile, err := ioutil.ReadDir("/Users/yuyang/go/src/saber/tpc-ds/r")
	if err != nil {
		panic(err)
	}

	for _, rf := range rFile {

		if rf.Name() == "q14_0.sql" || rf.Name() == "q14_1.sql" || rf.Name() == "q23_0.sql" || rf.Name() == "q23_1.sql" || rf.Name() == "q23_1.sql" || rf.Name() == "q97.sql" {
			continue
		}

		fmt.Printf("%s | ", rf.Name())

		conn, err := client.Connect("10.2.102.83:4000", "root", "", "test")
		for _, v := range variables {
			_, err := conn.Execute(v)
			if err != nil {
				panic(err)
			}
		}

		// renew SQL
		rfbs, err := ioutil.ReadFile(fmt.Sprintf("/Users/yuyang/go/src/saber/tpc-ds/r/%s", rf.Name()))
		if err != nil {
			panic(err)
		}
		startTime := time.Now()
		rsr, err := conn.Execute(string(rfbs))
		if err != nil {
			fmt.Println(" execute failed")
			time.Sleep(time.Second * 60)
			continue
		}
		fmt.Printf(" r: %.2f | ", time.Since(startTime).Seconds())
		rs := ""
		for _, row := range rsr.Values {
			rs += fmt.Sprintf("%s\n", row[0].AsString())
		}

		// original SQL
		ofbs, err := ioutil.ReadFile(fmt.Sprintf("/Users/yuyang/go/src/saber/tpc-ds/o/%s", rf.Name()))
		if err != nil {
			panic(err)
		}
		startTime = time.Now()
		rso, err := conn.Execute(string(ofbs))
		if err != nil {
			fmt.Println(" execute failed")
			time.Sleep(time.Second * 60)
			continue
		}
		fmt.Printf("o: %.2f | ", time.Since(startTime).Seconds())
		os := ""
		for _, row := range rso.Values {
			os += fmt.Sprintf("%s\n", row[0].AsString())
		}

		// compare
		if rs != os {
			fmt.Println(" failed")
		} else {
			fmt.Println(" success")
		}

	}

}

func CheckQueries() {

	fs1, err := ioutil.ReadDir("/root/150")
	if err != nil {
		panic(err)
	}

	for _, f1 := range fs1 {
		f1bs, err := ioutil.ReadFile(fmt.Sprintf("/root/150/%s", f1.Name()))
		if err != nil {
			panic(err)
		}
		f2bs, err := ioutil.ReadFile(fmt.Sprintf("/root/3t/%s", f1.Name()))
		if err != nil {
			panic(err)
		}
		if string(f1bs) != string(f2bs) {
			fmt.Println(f1.Name())
		}
	}
}
