package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Urethramancer/signor/opt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/grimdork/mysqldump"
)

var o struct {
	opt.DefaultHelp
	Config string `short:"C" long:"config" help:"Configuration file for database and backup options." default:"/etc/dbak/config.json"`
	Path   string `short:"p" long:"path" help:"Output directory for dump files." default:"/tmp"`
	Base   string `short:"b" long:"base" help:"Base name for dump files." default:"dump"`
	Full   bool   `short:"F" long:"full" help:"Perform full backup even if no changes."`
}

func main() {
	a := opt.Parse(&o)
	if o.Help || o.Path == "" || o.Base == "" || o.Config == "" {
		a.Usage()
		return
	}

	cfg, err := loadConfig(o.Config)
	fail(err)

	db, err := sql.Open("mysql", fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s", cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Name,
	))
	fail(err)

	dumper, err := mysqldump.Register(db, o.Path, o.Base+"-20060102T150405")
	fail(err)

	defer dumper.Close()
	var res string
	if o.Full || len(cfg.Tables) == 0 {
		res, err = dumper.Dump()
	} else {
		res, err = dumper.Dump(cfg.Tables...)
	}
	fail(err)
	defer os.Remove(res)

	pr("Dumped to %s", res)

	b, err := NewBucket(cfg.Region, cfg.Bucket)
	fail(err)

	fn := filepath.Join(res)
	err = b.Upload(fn)
	fail(err)
}

func pr(format string, v ...interface{}) {
	fmt.Printf(format+"\n", v...)
}

func fail(err error) {
	if err == nil {
		return
	}

	fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
	os.Exit(2)
}
