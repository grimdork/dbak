package main

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/Urethramancer/signor/opt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/grimdork/sqldump"
	_ "github.com/lib/pq"
)

var o struct {
	opt.DefaultHelp
	Config string `short:"C" long:"config" help:"Configuration file for database and backup options." default:"/etc/dbak/config.json" placeholder:"FILE"`
	Path   string `short:"p" long:"path" help:"Output directory for dump files." default:"/tmp" placeholder:"PATH"`
	Base   string `short:"b" long:"base" help:"Base name for dump files." default:"dump" placeholder:"NAME"`
	Full   bool   `short:"F" long:"full" help:"Perform full backup even if no changes."`
	Prune  int    `short:"P" long:"prune" help:"Remove files older than the specified number of days." placeholder:"DAYS"`
}

func main() {
	a := opt.Parse(&o)
	if o.Help || o.Path == "" || o.Base == "" || o.Config == "" {
		a.Usage()
		return
	}

	cfg, err := loadConfig(o.Config)
	fail(err)

	t := cfg.Type
	if t != "mysql" && t != "postgres" {
		t = "mysql"
	}
	sslmode := "disable"
	var conn string
	if cfg.Type == "postgres" {
		conn = fmt.Sprintf(
			"host=%s port=%s dbname=%s user=%s password=%s sslmode=%s",
			cfg.Host, cfg.Port, cfg.Name, cfg.Username, cfg.Password, sslmode,
		)
	} else {
		conn = fmt.Sprintf(
			"%s:%s@tcp(%s:%s)/%s",
			cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Name,
		)
	}
	db, err := sql.Open(t, conn)
	fail(err)

	db.SetMaxIdleConns(100)
	db.SetMaxOpenConns(100)
	dumper, err := sqldump.NewDumper(db, o.Path, o.Base+"-20060102T150405.sql")
	fail(err)

	defer dumper.Close()
	b, err := NewBucket(cfg.Region, cfg.Bucket)
	fail(err)

	dates := b.LoadTableDates()
	if o.Full || len(cfg.Tables) == 0 {
		err = dumper.Dump()
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
		err = dumper.Dump(list...)
	}
	fail(err)

	err = b.UpdateTableDates(dates)
	fail(err)

	defer os.Remove(dumper.Path())
	pr("Dumped to %s", dumper.Path())
	err = b.Upload(dumper.Path())
	fail(err)

	if o.Prune > 0 {
		c, err := b.Prune(o.Base, o.Prune)
		fail(err)
		pr("Pruned %d files.", c)
	}
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
