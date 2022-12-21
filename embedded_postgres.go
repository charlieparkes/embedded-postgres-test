package embeddedpostgres

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
)

var wg sync.WaitGroup

var ErrPortNotAvailable = errors.New("port not available")

// EmbeddedPostgres maintains all configuration and runtime functions for maintaining the lifecycle of one Postgres process.
type EmbeddedPostgres struct {
	config          *config
	logger          *zap.Logger
	initDatabase    initDatabase
	createDatabase  createDatabase
	started         bool
	syncedLogger    *syncedLogger
	versionStrategy VersionStrategy
}

// NewDatabase creates a new EmbeddedPostgres struct that can be used to start and stop a Postgres process.
// When called with no parameters it will assume a default configuration state provided by the DefaultConfig method.
// When called with parameters the first Config parameter will be used for configuration.
func NewDatabase(config ...*config) *EmbeddedPostgres {
	if len(config) > 1 {
		panic("may not provide more than one config")
	}
	if len(config) < 1 {
		return newDatabaseWithConfig(Config())
	}
	return newDatabaseWithConfig(config[0])
}

func newDatabaseWithConfig(config *config) *EmbeddedPostgres {
	return &EmbeddedPostgres{
		config:         config,
		logger:         config.logger,
		initDatabase:   defaultInitDatabase,
		createDatabase: defaultCreateDatabase,
		started:        false,
		versionStrategy: defaultVersionStrategy(
			config,
			runtime.GOOS,
			runtime.GOARCH,
			linuxMachineName,
			shouldUseAlpineLinuxBuild,
		),
	}
}

// Start will try to start the configured Postgres process returning an error when there were any problems with invocation.
// If any error occurs Start will try to also Stop the Postgres process in order to not leave any sub-process running.
//nolint:funlen
func (ep *EmbeddedPostgres) Start(ctx context.Context) error {
	if ep.started {
		return errors.New("server is already started")
	}
	t := time.Now()
	err := ep.start(ctx)
	d := time.Since(t)
	if err != nil {
		if err := ep.cleanDirectory(ep.config.dataPath); err != nil {
			return fmt.Errorf("startup error, but failed to clean up directory: %w", err)
		}
	} else {
		ep.logger.Debug("start", zap.Duration("elapsed", d))
	}
	if err != nil {
		return fmt.Errorf("failed to start: %w", err)
	}
	return nil
}

func (ep *EmbeddedPostgres) start(ctx context.Context) error {
	logger, err := newSyncedLogger("", ep.config.pgLogger)
	if err != nil {
		return errors.New("unable to create logger")
	}
	ep.syncedLogger = logger

	cacheLocator := defaultCacheLocator(ep.versionStrategy)
	cacheLocation, cacheExists := cacheLocator()

	if ep.config.runtimePath == "" {
		ep.config.runtimePath = filepath.Join(filepath.Dir(cacheLocation), string(ep.config.version))
	}

	baseDataPath := filepath.Join(filepath.Dir(cacheLocation), "data")
	if ep.config.dataPath == "" {
		ep.config.dataPath = filepath.Join(baseDataPath, uuid.NewString())
		if err := os.MkdirAll(ep.config.dataPath, 0700); err != nil {
			return fmt.Errorf("unable to create data directory %s with error: %s", ep.config.dataPath, err)
		}
	}

	if err := os.MkdirAll(ep.config.runtimePath, 0755); err != nil {
		return fmt.Errorf("unable to create runtime directory %s with error: %s", ep.config.runtimePath, err)
	}

	cacheKeyParts := []string{
		string(ep.config.version),
		ep.config.database,
		ep.config.username,
		ep.config.password,
		ep.config.locale,
	}
	algorithm := fnv.New32a()
	algorithm.Write([]byte(strings.Join(cacheKeyParts, "")))
	cacheKey := algorithm.Sum32()

	cachedDataPath := filepath.Join(baseDataPath, string(ep.config.version)+"-"+fmt.Sprint(cacheKey))
	cachedDataFound := dataDirIsValid(cachedDataPath, ep.config.version)

	// Only start one postgres instance at a time to ensure no port or caching collisions.
	wg.Wait()
	if !cachedDataFound {
		wg.Add(1)
		defer wg.Done()
	}

	// If no port is provided, find an available port. Otherwise, attempt to use the requested port.
	if ep.config.port == 0 {
		port, err := findPort()
		if err != nil {
			return fmt.Errorf("failed to find port: %w", err)
		}
		ep.config.port = port
	}

	if err := ensurePortAvailable(ep.config.port); err != nil {
		return fmt.Errorf("port was not available: %w", err)
	}

	_, binDirErr := os.Stat(filepath.Join(ep.config.runtimePath, "bin"))
	if os.IsNotExist(binDirErr) {
		if !cacheExists {
			remoteFetchStrategy := defaultRemoteFetchStrategy(ep.config.binaryRepositoryURL, ep.versionStrategy, cacheLocator)
			if err := remoteFetchStrategy(); err != nil {
				return fmt.Errorf("failed to fetch remote: %w", err)
			}
		}

		if err := decompressTarXz(defaultTarReader, cacheLocation, ep.config.runtimePath); err != nil {
			return fmt.Errorf("failed to decompress archive: %w", err)
		}
	}

	if !cachedDataFound {
		if err := ep.cleanDataDirectoryAndInit(cachedDataPath); err != nil {
			return fmt.Errorf("failed to init data cache: %w", err)
		}
	}
	ep.logger.Debug("cache", zap.String("path", cachedDataPath))

	if err := copyDirectory(cachedDataPath, ep.config.dataPath); err != nil {
		return fmt.Errorf("failed to copy dir: %w", err)
	}
	ep.logger.Debug("copy", zap.String("path", ep.config.dataPath))

	if err := startPostgres(ep); err != nil {
		return err
	}

	if err := ep.syncedLogger.flush(); err != nil {
		return err
	}

	ep.started = true

	err = ep.createDatabase(ctx, ep.config.port, ep.config.username, ep.config.password, ep.config.database)
	if err != nil && pgErrorCode(err) != pgerrcode.DuplicateDatabase {
		if stopErr := stopPostgres(ep); stopErr != nil {
			return fmt.Errorf("unable to stop database: %w", err)
		}
		return fmt.Errorf("failed to create database: %w", err)
	}

	if err := healthCheckDatabaseOrTimeout(ctx, ep.config); err != nil {
		if stopErr := stopPostgres(ep); stopErr != nil {
			return fmt.Errorf("unable to stop database casused by error %s", err)
		}

		return err
	}

	return nil
}

func (ep *EmbeddedPostgres) cleanDirectory(path string) error {
	if !exists(path) {
		return nil
	}
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("unable to clean up directory %s with error: %s", path, err)
	}
	return nil
}

func (ep *EmbeddedPostgres) cleanDataDirectoryAndInit(cachedDataPath string) error {
	if err := ep.cleanDirectory(cachedDataPath); err != nil {
		return fmt.Errorf("unable to clean up data directory %s with error: %s", cachedDataPath, err)
	}
	if err := os.MkdirAll(cachedDataPath, 0700); err != nil {
		return fmt.Errorf("unable to create data directory %s with error: %s", cachedDataPath, err)
	}

	if err := ep.initDatabase(ep.config.runtimePath, ep.config.runtimePath, cachedDataPath, ep.config.username, ep.config.password, ep.config.locale, ep.syncedLogger.file); err != nil {
		return err
	}

	conf := []byte{}
	for k, v := range ep.config.pgConf {
		conf = append(conf, []byte(fmt.Sprintf("%v = '%v'\n", k, v))...)
	}
	confPath := filepath.Join(cachedDataPath, "postgresql.auto.conf")
	if err := os.WriteFile(confPath, conf, 0666); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

// Stop will try to stop the Postgres process gracefully returning an error when there were any problems.
func (ep *EmbeddedPostgres) Stop(_ context.Context) error {
	t := time.Now()
	defer func() {
		d := time.Since(t)
		ep.logger.Debug("stop", zap.Duration("elapsed", d))
	}()

	if !ep.started {
		return errors.New("server has not been started")
	}

	go func() {
		defer func() { _ = ep.cleanDirectory(ep.config.dataPath) }()

		if err := stopPostgres(ep); err != nil {
			log.Printf("failed to stop: %s\n", err)
		}

		ep.started = false

		if err := ep.syncedLogger.flush(); err != nil {
			log.Printf("failed to flush logger: %s\n", err)
		}
	}()

	return nil
}

// RecoverStop returns a deferrable function that will teardown in the event of a panic.
func (ep *EmbeddedPostgres) RecoverStop(ctx context.Context) {
	if r := recover(); r != nil {
		// if err := ep.Stop(ctx); err != nil {
		// 	f.log.Warn("failed to tear down", zap.Error(err))
		// }
		_ = ep.Stop(ctx)
		panic(r)
	}
}

func (ep *EmbeddedPostgres) URL() string {
	return fmt.Sprintf("postgresql://%v:%v@localhost:%v/%v?sslmode=disable", ep.config.username, ep.config.password, ep.config.port, ep.config.database)
}

func (ep *EmbeddedPostgres) ConnectionString() string {
	return connectionString(ep.config.port, ep.config.username, ep.config.password, ep.config.database)
}

func (ep *EmbeddedPostgres) Connect(ctx context.Context) (*pgxpool.Pool, error) {
	return connect(ctx, ep.config.port, ep.config.username, ep.config.password, ep.config.database)
}

func (ep *EmbeddedPostgres) Ping(ctx context.Context) error {
	pool, err := ep.Connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	return pool.Ping(ctx)
}

func startPostgres(ep *EmbeddedPostgres) error {
	postgresBinary := filepath.Join(ep.config.runtimePath, "bin/pg_ctl")
	postgresProcess := exec.Command(postgresBinary, "start", "-w",
		"-D", ep.config.dataPath,
		"-o", fmt.Sprintf(`"-p %d"`, ep.config.port))
	postgresProcess.Stdout = ep.syncedLogger.file
	postgresProcess.Stderr = ep.syncedLogger.file

	if err := postgresProcess.Run(); err != nil {
		return fmt.Errorf("could not start postgres using %s", postgresProcess.String())
	}

	return nil
}

func stopPostgres(ep *EmbeddedPostgres) error {
	postgresBinary := filepath.Join(ep.config.runtimePath, "bin/pg_ctl")
	postgresProcess := exec.Command(postgresBinary, "stop", "-w",
		"-D", ep.config.dataPath)
	postgresProcess.Stderr = ep.syncedLogger.file
	postgresProcess.Stdout = ep.syncedLogger.file

	if err := postgresProcess.Run(); err != nil {
		return err
	}

	return nil
}

func findPort() (uint32, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, fmt.Errorf("%v: %w", err, ErrPortNotAvailable)
	}

	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		return 0, err
	}

	return uint32(port), nil
}

func ensurePortAvailable(port uint32) error {
	conn, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return fmt.Errorf("process already listening on port %d: %w", port, ErrPortNotAvailable)
	}

	if err := conn.Close(); err != nil {
		return err
	}

	return nil
}

func dataDirIsValid(dataDir string, version PostgresVersion) bool {
	pgVersion := filepath.Join(dataDir, "PG_VERSION")

	d, err := ioutil.ReadFile(pgVersion)
	if err != nil {
		return false
	}

	v := strings.TrimSuffix(string(d), "\n")

	return strings.HasPrefix(string(version), v)
}

// https://stackoverflow.com/questions/51779243/copy-a-folder-in-go
// https://github.com/moby/moby/blob/master/daemon/graphdriver/copy/copy.go
func copyDirectory(scrDir, dest string) error {
	entries, err := os.ReadDir(scrDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		sourcePath := filepath.Join(scrDir, entry.Name())
		destPath := filepath.Join(dest, entry.Name())

		fileInfo, err := os.Stat(sourcePath)
		if err != nil {
			return err
		}

		stat, ok := fileInfo.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("failed to get raw syscall.Stat_t data for '%s'", sourcePath)
		}

		switch fileInfo.Mode() & os.ModeType {
		case os.ModeDir:
			if err := createIfNotExists(destPath, 0700); err != nil {
				return err
			}
			if err := copyDirectory(sourcePath, destPath); err != nil {
				return err
			}
		case os.ModeSymlink:
			if err := copySymLink(sourcePath, destPath); err != nil {
				return err
			}
		default:
			if err := copy(sourcePath, destPath); err != nil {
				return err
			}
		}

		if err := os.Lchown(destPath, int(stat.Uid), int(stat.Gid)); err != nil {
			return err
		}

		fInfo, err := entry.Info()
		if err != nil {
			return err
		}

		isSymlink := fInfo.Mode()&os.ModeSymlink != 0
		if !isSymlink {
			if err := os.Chmod(destPath, fInfo.Mode()); err != nil {
				return err
			}
		}
	}
	return nil
}

func copy(srcFile, dstFile string) error {
	out, err := os.Create(dstFile)
	if err != nil {
		return err
	}

	defer out.Close()

	in, err := os.Open(srcFile)
	defer func() {
		if err := in.Close(); err != nil {
			panic(err)
		}
	}()
	if err != nil {
		return err
	}

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}

	return nil
}

func exists(filePath string) bool {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return false
	}

	return true
}

func createIfNotExists(dir string, perm os.FileMode) error {
	if exists(dir) {
		return nil
	}

	if err := os.MkdirAll(dir, perm); err != nil {
		return fmt.Errorf("failed to create directory: '%s', error: '%s'", dir, err.Error())
	}

	return nil
}

func copySymLink(source, dest string) error {
	link, err := os.Readlink(source)
	if err != nil {
		return err
	}
	return os.Symlink(link, dest)
}
