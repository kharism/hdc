package main

import (
	"fmt"
	"os"

	//"github.com/eaciit/dbox"

	"github.com/kharism/hdc/hive"
)

var ctx *hive.Hive
var Buff string
var FinishReading chan bool

func connect() error {
	var e error
	if ctx == nil {
		ctx = hive.HiveConfig("127.0.0.1:10000", "default", "hive", "", "d:\\hive_hrtonworks\\usrlibhive\\lib\\*")
	}
	e = ctx.Conn.Open()
	if e != nil {
		fmt.Println("PPPP", e.Error())
		os.Exit(-1)
	}
	fmt.Println("Testing connection")

	/*go func() {

		for {
			if ctx.Conn. {
				uu, _ := ioutil.ReadAll(ctx.Conn.ErrorReader)
				fmt.Println(string(uu))
			}
		}

	}()*/

	e = ctx.Conn.TestConnection()

	return e
}
func main() {
	FinishReading = make(chan bool, 1)
	err := connect()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	defer ctx.Conn.Close()

	fmt.Println("Connection Success")
	res := []map[string]interface{}{}
	e := ctx.Exec("SELECT code, description, salary FROM sample_07", func(x hive.HiveResult) error {
		fmt.Println("Appending")
		res = append(res, x.ResultObj.(map[string]interface{}))
		return nil
	})

	if e != nil {
		fmt.Println("Error Query", err.Error())
		os.Exit(1)
	}
	fmt.Println(len(res))
	for _, r := range res {
		fmt.Println(r)
	}
}
