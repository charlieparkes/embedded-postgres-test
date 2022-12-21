package embeddedpostgres

import (
	"io"
	"os"
	"time"

	"go.uber.org/zap"
)

type config struct {
	version             PostgresVersion
	port                uint32
	database            string
	username            string
	password            string
	runtimePath         string
	dataPath            string
	locale              string
	binaryRepositoryURL string
	startTimeout        time.Duration
	logger              *zap.Logger
	pgLogger            io.Writer
	pgConf              map[string]string
}

func Config(opts ...opt) *config {
	c := &config{
		version:             V14,
		database:            "postgres",
		username:            "postgres",
		password:            "postgres",
		startTimeout:        15 * time.Second,
		pgLogger:            os.Stdout,
		binaryRepositoryURL: "https://repo1.maven.org/maven2",
		pgConf: map[string]string{
			"fsync":                   "off",
			"synchronous_commit":      "off",
			"full_page_writes":        "off",
			"random_page_cost":        "1.1",
			"shared_buffers":          "12MB",
			"work_mem":                "12MB",
			"log_min_messages":        "PANIC",
			"log_min_error_statement": "PANIC",
			"log_statement":           "none",
			"client_min_messages":     "ERROR",
			"commit_delay":            "100000",
			"wal_level":               "minimal",
			"archive_mode":            "off",
			"max_wal_senders":         "0",
		},
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.logger == nil {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		c.logger = logger
	}
	return c
}

type opt func(*config)

// WithVersion will set the Postgres binary version
func WithVersion(version PostgresVersion) opt {
	return func(c *config) {
		c.version = version
	}
}

// WithLocale sets the default locale for initdb
func WithLocale(locale string) opt {
	return func(c *config) {
		c.locale = locale
	}
}

// WithLogger sets the logger for postgres output
func WithPGLogger(logger io.Writer) opt {
	return func(c *config) {
		c.pgLogger = logger
	}
}

// WithRepository sets BinaryRepositoryURL to fetch PG Binary in case of Maven proxy
func WithRepository(url string) opt {
	return func(c *config) {
		c.binaryRepositoryURL = url
	}
}

func WithStartTimeout(d time.Duration) opt {
	return func(c *config) {
		c.startTimeout = d
	}
}

func WithDatabase(database string) opt {
	return func(c *config) {
		c.database = database
	}
}

func WithPort(port uint32) opt {
	return func(c *config) {
		c.port = port
	}
}

func WithRuntimePath(path string) opt {
	return func(c *config) {
		c.runtimePath = path
	}
}

func WithLogger(logger *zap.Logger) opt {
	return func(c *config) {
		c.logger = logger
	}
}

func WithPGConfig(conf map[string]string) opt {
	return func(c *config) {
		c.pgConf = conf
	}
}

// PostgresVersion represents the semantic version used to fetch and run the Postgres process
type PostgresVersion string

// Predefined supported Postgres versions
const (
	V14 = PostgresVersion("14.5.0")
	V13 = PostgresVersion("13.8.0")
	V12 = PostgresVersion("12.12.0")
	V11 = PostgresVersion("11.17.0")
	V10 = PostgresVersion("10.22.0")
	V9  = PostgresVersion("9.6.24")
)
