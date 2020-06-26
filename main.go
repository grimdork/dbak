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

	db.SetMaxIdleConns(100)
	db.SetMaxOpenConns(100)
	dumper, err := mysqldump.Register(db, o.Path, o.Base+"-20060102T150405"+".sql")
	fail(err)

	defer dumper.Close()
	b, err := NewBucket(cfg.Region, cfg.Bucket)
	fail(err)

	dates := b.LoadTableDates()
	var res string
	if o.Full || len(cfg.Tables) == 0 {
		res, err = dumper.Dump()
	} else {
		// In selective table mode we're also looking for the last updated date.
		// NOTE: This doesn't work with all database storage types.
		list := []string{}
		for _, t := range cfg.Tables {
			date, err := getLastUpdate(db, cfg.Name, t)
			if err != nil {
				// No date exists, just include the table.
				list = append(list, t)
			} else {
				d, ok := dates.Dates[t]
				if !ok || d == date {
					// No previous date, or last modified date is different.
					list = append(list, t)
				}
			}
			// Update the map with the latest date found.
			dates.Dates[t] = date
		}
		res, err = dumper.Dump(list...)
	}
	fail(err)

	err = b.UpdateTableDates(dates)
	fail(err)

	defer os.Remove(res)
	pr("Dumped to %s", res)
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

func getLastUpdate(db *sql.DB, dbname, table string) (string, error) {
	var date string
	err := db.QueryRow("SELECT UPDATE_TIME FROM information_schema.tables WHERE TABLE_SCHEMA = '" + dbname + "' AND TABLE_NAME ='" + table + "'").Scan(&date)
	return date, err
}
