package hive

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/eaciit/errorlib"
	// "log"
	"os/exec"
	"strings"
)

const (
	BEE_CLI_STR      = "jdbc:hive2:"
	CLOSE_SCRIPT     = "!quit"
	BEE_CLOSED       = "(closed)>"
	TEST_CONN_SCRIPT = "show tables;"
)

type DuplexTerm struct {
	Writer      *bufio.Writer
	Reader      *bufio.Reader
	ErrorReader *bufio.Reader
	Cmd         *exec.Cmd
	CmdStr      string
	Stdin       io.WriteCloser
	Stdout      io.ReadCloser
	Stderr      io.ReadCloser
	FnReceive   FnHiveReceive
	ConnParam   []string
	OutputType  string
	DateFormat  string
}

var hr HiveResult

func (d *DuplexTerm) Open() (e error) {
	if len(d.ConnParam) >= 4 {
		//arg := append([]string{"-c"}, d.CmdStr)
		//arg := strings.Split(d.CmdStr, " ")
		arg := append([]string{"-Djline.WindowsTerminal.directConsole=false"}, d.ConnParam...)
		fmt.Println("java", arg)
		d.Cmd = exec.Command("java", arg...)
		//d.Cmd.Stdin = strings.NewReader("show tables;\n!quit\n")
		if d.Stdin, e = d.Cmd.StdinPipe(); e != nil {
			return
		}

		if d.Stdout, e = d.Cmd.StdoutPipe(); e != nil {
			return
		}
		if d.Stderr, e = d.Cmd.StderrPipe(); e != nil {
			return
		}

		d.Writer = bufio.NewWriter(d.Stdin)
		d.Reader = bufio.NewReader(d.Stdout)
		d.ErrorReader = bufio.NewReader(d.Stderr)
		d.FnReceive = nil
		e = d.Cmd.Start()
	} else {
		e = errorlib.Error("", "", "Open", "The Connection Config not Set")
	}

	return
}

func (d *DuplexTerm) TestConnection() (e error) {
	if d.CmdStr != "" {
		hr, e = d.SendInput(TEST_CONN_SCRIPT)
	} else {
		e = errorlib.Error("", "", "Open", "The Connection Config not Set")
	}

	return
}

func (d *DuplexTerm) Close() {
	result, e := d.SendInput(CLOSE_SCRIPT)

	_ = result
	_ = e

	d.FnReceive = nil
	d.Cmd.Wait()
	d.Stdin.Close()
	d.Stdout.Close()
}

func (d *DuplexTerm) SendInput(input string) (res HiveResult, err error) {
	if d.FnReceive != nil {
		done := make(chan bool)
		go func() {
			res, err = d.process()
			if err != nil {
				d.FnReceive = nil
				close(done)
				return
			}
			done <- true
		}()
		iwrite, e := d.Writer.WriteString(input + "\n")
		err = e
		if iwrite == 0 {
			err = errors.New("Writing only 0 byte")
		} else {
			err = d.Writer.Flush()
		}

		<-done
		d.FnReceive = nil
	} else {
		iwrite, e := d.Writer.WriteString(input + "\n")
		err = e
		if iwrite == 0 {
			err = errors.New("Writing only 0 byte")
		} else {
			err = d.Writer.Flush()
		}
		if err != nil {
			fmt.Println(err.Error())
			buff, errstream := ioutil.ReadAll(d.ErrorReader)
			if errstream == nil {
				fmt.Println("FROM STDERR", string(buff))
			}
		}
		if err == nil && d.FnReceive == nil {
			done := make(chan bool)
			go func() {
				res, err = d.process()
				if err != nil {
					close(done)
					return
				}
				done <- true
			}()
			<-done
		}
	}
	return
}

func (d *DuplexTerm) process() (result HiveResult, e error) {
	isHeader := false
	bread := ""

loop:
	for {
		peekBefore, _ := d.Reader.Peek(14)
		peekBeforeStr := string(peekBefore)

		bread, e = d.Reader.ReadString('\n')
		bread = strings.TrimRight(bread, "\n")

		peek, _ := d.Reader.Peek(14)
		peekStr := string(peek)

		delimiter := "\t"

		if strings.Contains(bread, BEE_CLOSED) {
			// the connection is closed/configuration is wrong
			e = errorlib.Error("", "", "Process Query", "The Connection is Closed, pleace check your connection configuration")
			break loop
		} else {

			if d.OutputType == CSV {
				delimiter = ","
			}

			if isHeader {
				hr = HiveResult{}
				hr.constructHeader(bread, delimiter)
				isHeader = false
			} else if !strings.Contains(bread, BEE_CLI_STR) {
				Parse(hr.Header, bread, &hr.ResultObj, d.OutputType, d.DateFormat)
				if d.FnReceive != nil {
					hr.Result = []string{bread}
					d.FnReceive(hr)
				} else {
					hr.Result = append(hr.Result, bread)
				}
			}

			if strings.Contains(peekBeforeStr, BEE_CLI_STR) {
				isHeader = true
			}
			if (e != nil && e.Error() == "EOF") || strings.Contains(peekStr, BEE_CLI_STR) {
				if d.FnReceive == nil {
					result = hr
				}
				break
			}

		}
	}

	return
}
