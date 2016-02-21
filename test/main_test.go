package test

import (
	//"fmt"
	"github.com/eaciit/toolkit"
	. "github.com/frezadev/hdc/hive"
	//. "github.com/eaciit/hdc/hive"
	// "reflect"
	"log"
	"os"
	"testing"
)

var h *Hive
var e error

type Sample7 struct {
	Code        string `tag_name:"code"`
	Description string `tag_name:"description"`
	Total_emp   string `tag_name:"total_emp"`
	Salary      string `tag_name:"salary"`
}

func killApp(code int) {
	if h != nil {
		h.Conn.Close()
	}
	os.Exit(code)
}

func fatalCheck(t *testing.T, what string, e error) {
	if e != nil {
		t.Fatalf("%s: %s", what, e.Error())
	}
}

func TestHiveConnect(t *testing.T) {
	h = HiveConfig("192.168.0.223:10000", "default", "hdfs", "", "")
}

/* Populate will exec query and immidiately return the value into object
Populate is suitable for short type query that return limited data,
Exec is suitable for long type query that return massive amount of data and require time to produce it

Ideally Populate should call Exec as well but already have predefined function on it receiving process
*/
func TestHivePopulate(t *testing.T) {
	q := "select * from sample_07 limit 5;"

	var result []toolkit.M

	h.Conn.Open()

	_, e := h.Populate(q, &result)
	fatalCheck(t, "Populate", e)

	if len(result) != 5 {
		log.Printf("Error want %d got %d", 5, len(result))
	}

	log.Printf("Result: \n%s", toolkit.JsonString(result))

	h.Conn.Close()
}

func TestHiveExec(t *testing.T) {
	q := "select * from sample_07 limit 5;"
	x := "select * from sample_07 limit 10;"
	DoSomething := func(res interface{}) (e error) {
		tmp := toolkit.M{}
		toolkit.Serde(res, &tmp, "json")
		log.Printf("limit 5: %v", tmp)
		return
	}

	DoElse := func(res interface{}) (e error) {
		tmp := toolkit.M{}
		toolkit.Serde(res, &tmp, "json")
		log.Printf("limit 10: %v", tmp)
		return
	}

	// h.Conn.SetFn(DoSomething)
	h.Conn.Open()

	h.Conn.FnReceive = DoSomething
	h.Exec(q)

	h.Conn.FnReceive = DoElse
	h.Exec(x)

	h.Conn.Close()
}
