package main

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/Urethramancer/signor/opt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/go-sql-driver/mysql"
	"github.com/grimdork/mysqldump"
)

var o struct {
	opt.DefaultHelp
	Config string `short:"C" long:"config" help:"Configuration file for database and backup options." default:"/etc/dbak/config.json"`
	Path   string `short:"p" long:"path" help:"Output directory for dump files." default:"/tmp"`
	Base   string `short:"b" long:"base" help:"Base file name for the output file." default:"dump"`
	Full   bool   `short:"F" long:"full" help:"Perform full backup even if no changes."`
}

func main() {
	a := opt.Parse(&o)
	if o.Help {
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

	fmt.Printf("Dumped to %s\n", res)
	awscfg := &aws.Config{
		Region: aws.String(cfg.Region),
	}
	// Use shared credentials in ~/.aws/credentials or from envvars.
	// Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
	sess, err := session.NewSession(awscfg)
	fail(err)

	_, err = sess.Config.Credentials.Get()
	fail(err)

	b := NewBucket(s3.New(sess), cfg.Bucket)
	list, err := b.List()
	fail(err)

	for _, item := range list {
		fmt.Println("Name:         ", *item.Key)
		fmt.Println("Last modified:", *item.LastModified)
		fmt.Println("Size:         ", *item.Size)
		fmt.Println("Storage class:", *item.StorageClass)
		fmt.Println("")
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
