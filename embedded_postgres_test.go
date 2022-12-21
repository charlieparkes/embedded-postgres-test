package embeddedpostgres

import (
	"context"
	"net"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_DefaultConfig(t *testing.T) {
	defer verifyLeak(t)

	database := NewDatabase()
	err := database.Start(context.Background())
	assert.NoError(t, err)
	defer database.RecoverStop(context.Background())
	defer func() { _ = database.Stop(context.Background()) }()

	db, err := database.Connect(context.Background())
	require.NoError(t, err)
	defer db.Close()

	err = db.Ping(context.Background())
	require.NoError(t, err)

	err = database.Stop(context.Background())
	require.NoError(t, err)
}

func Test_ErrorWhenPortAlreadyTaken(t *testing.T) {
	listener, err := net.Listen("tcp", "localhost:9887")
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := listener.Close(); err != nil {
			panic(err)
		}
	}()

	cfg := Config(
		WithPort(9887),
		WithPGLogger(nil),
	)
	database := NewDatabase(cfg)

	err = database.Start(context.Background())

	assert.ErrorIs(t, err, ErrPortNotAvailable)
}

func Test_TimesOutWhenCannotStart(t *testing.T) {
	database := NewDatabase(Config(
		WithStartTimeout(time.Second),
		WithDatabase("something-fancy"),
		WithPGLogger(nil),
	))

	database.createDatabase = func(ctx context.Context, port uint32, username, password, database string) error {
		return nil
	}

	err := database.Start(context.Background())

	assert.EqualError(t, err, "failed to start: unhealthy after 1s")
}

func Test_ErrorWhenStopCalledBeforeStart(t *testing.T) {
	database := NewDatabase(Config(WithPGLogger(nil)))

	err := database.Stop(context.Background())

	assert.EqualError(t, err, "server has not been started")
}

func Test_ErrorWhenStartCalledWhenAlreadyStarted(t *testing.T) {
	database := NewDatabase(Config(WithPGLogger(nil)))
	defer database.RecoverStop(context.Background())

	err := database.Start(context.Background())
	require.NoError(t, err)

	require.NoError(t, database.Ping(context.Background()), "database should be running")

	err = database.Start(context.Background())
	require.EqualError(t, err, "server is already started")

	require.NoError(t, database.Ping(context.Background()), "database should be running")

	err = database.Stop(context.Background())
	require.NoError(t, err)

	require.Error(t, database.Ping(context.Background()))
}

func Test_CustomConfig(t *testing.T) {
	tempDir, cleanupTempDir := makeTempDir("embedded_postgres_test")
	defer cleanupTempDir()

	database := NewDatabase(Config(
		WithVersion(V12),
		WithRuntimePath(tempDir),
		WithPort(9876),
		WithLocale("C"),
		WithPGLogger(nil),
	))

	if err := database.Start(context.Background()); err != nil {
		shutdownDBAndFail(t, err, database)
	}

	db, err := database.Connect(context.Background())
	if err != nil {
		shutdownDBAndFail(t, err, database)
	}
	defer db.Close()

	if err = db.Ping(context.Background()); err != nil {
		shutdownDBAndFail(t, err, database)
	}

	if err := database.Stop(context.Background()); err != nil {
		shutdownDBAndFail(t, err, database)
	}
}

func Test_CustomLocaleConfig(t *testing.T) {
	// C is the only locale we can guarantee to always work
	database := NewDatabase(Config(
		WithLocale("C"),
		WithPGLogger(nil),
	))
	if err := database.Start(context.Background()); err != nil {
		shutdownDBAndFail(t, err, database)
	}

	db, err := database.Connect(context.Background())
	if err != nil {
		shutdownDBAndFail(t, err, database)
	}
	defer db.Close()

	if err = db.Ping(context.Background()); err != nil {
		shutdownDBAndFail(t, err, database)
	}

	if err := database.Stop(context.Background()); err != nil {
		shutdownDBAndFail(t, err, database)
	}
}

func Test_CustomBinariesRepo(t *testing.T) {
	tempDir, cleanupTempDir := makeTempDir("embedded_postgres_test")
	defer cleanupTempDir()

	database := NewDatabase(Config(
		WithVersion(V12),
		WithRuntimePath(tempDir),
		WithPort(9876),
		WithLocale("C"),
		WithPGLogger(nil),
	))

	if err := database.Start(context.Background()); err != nil {
		shutdownDBAndFail(t, err, database)
	}

	db, err := database.Connect(context.Background())
	if err != nil {
		shutdownDBAndFail(t, err, database)
	}
	defer db.Close()

	if err = db.Ping(context.Background()); err != nil {
		shutdownDBAndFail(t, err, database)
	}

	if err := database.Stop(context.Background()); err != nil {
		shutdownDBAndFail(t, err, database)
	}
}

func Test_RunningInParallel(t *testing.T) {
	tempDir, cleanupTempDir := makeTempDir("parallel_tests_path")
	defer cleanupTempDir()

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(2)

	runTestWithPortAndPath := func(port uint32, path string) {
		defer waitGroup.Done()

		database := NewDatabase(Config(
			WithRuntimePath(path),
			WithPort(port),
			WithPGLogger(nil),
		))
		if err := database.Start(context.Background()); err != nil {
			shutdownDBAndFail(t, err, database)
		}

		db, err := database.Connect(context.Background())
		if err != nil {
			shutdownDBAndFail(t, err, database)
		}
		defer db.Close()

		if err = db.Ping(context.Background()); err != nil {
			shutdownDBAndFail(t, err, database)
		}

		if err := database.Stop(context.Background()); err != nil {
			shutdownDBAndFail(t, err, database)
		}
	}

	go runTestWithPortAndPath(8765, path.Join(tempDir, "1"))
	go runTestWithPortAndPath(8766, path.Join(tempDir, "2"))

	waitGroup.Wait()
}

func Test_FastStart(t *testing.T) {
	t1 := time.Now()
	database := NewDatabase(Config(WithPGLogger(nil), WithDatabase("faststart")))
	require.NoError(t, database.Start(context.Background()))
	d1 := time.Since(t1)
	t.Cleanup(func() { database.Stop(context.Background()) })

	// Downloads should be cached.
	// Data for config should be cached.
	t2 := time.Now()
	database2 := NewDatabase(Config(WithPGLogger(nil), WithDatabase("faststart")))
	require.NoError(t, database2.Start(context.Background()))
	d2 := time.Since(t2)
	t.Cleanup(func() { database2.Stop(context.Background()) })

	t.Logf("first start in %v, second start in %v", d1, d2)
	require.Less(t, d2, d1)
}
