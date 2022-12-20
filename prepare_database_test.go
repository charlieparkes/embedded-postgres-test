package embeddedpostgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_defaultInitDatabase_ErrorWhenCannotCreatePasswordFile(t *testing.T) {
	err := defaultInitDatabase("path_not_exists", "path_not_exists", "path_not_exists", "Tom", "Beer", "", os.Stderr)

	assert.Contains(t, err.Error(), "unable to write password file to path_not_exists/pwfile")
}

func Test_defaultInitDatabase_ErrorWhenCannotStartInitDBProcess(t *testing.T) {
	binTempDir, cleanupBinTempDir := makeTempDir("prepare_database_test_bin")
	defer cleanupBinTempDir()

	runtimeTempDir, cleanupRuntimeTempDir := makeTempDir("prepare_database_test_runtime")
	defer cleanupRuntimeTempDir()

	err := defaultInitDatabase(binTempDir, runtimeTempDir, filepath.Join(runtimeTempDir, "data"), "Tom", "Beer", "", os.Stderr)

	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), fmt.Sprintf("unable to init database using '%s/bin/initdb -A password -U Tom -D %s/data --pwfile=%s",
		binTempDir,
		runtimeTempDir,
		runtimeTempDir))
	// assert.FileExists(t, filepath.Join(runtimeTempDir, "pwfile"))
}

func Test_defaultInitDatabase_ErrorInvalidLocaleSetting(t *testing.T) {
	tempDir, cleanupTempDir := makeTempDir("prepare_database_test")
	defer cleanupTempDir()

	err := defaultInitDatabase(tempDir, tempDir, filepath.Join(tempDir, "data"), "postgres", "postgres", "en_XY", os.Stderr)

	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), fmt.Sprintf("unable to init database using '%s/bin/initdb -A password -U postgres -D %s/data --pwfile=%s",
		tempDir,
		tempDir,
		tempDir))
	assert.Contains(t, err.Error(), "--locale=en_XY")
}

func Test_defaultInitDatabase_PwFileRemoved(t *testing.T) {
	tempDir, cleanupTempDir := makeTempDir("prepare_database_test")
	defer cleanupTempDir()

	database := NewDatabase(Config(WithRuntimePath(tempDir)))
	if err := database.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := database.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	pwFile := filepath.Join(tempDir, "pwfile")
	_, err := os.Stat(pwFile)

	assert.True(t, os.IsNotExist(err), "pwfile (%v) still exists after starting the db", pwFile)
}

func Test_defaultCreateDatabase_ErrorWhenSQLOpenError(t *testing.T) {
	err := defaultCreateDatabase(context.Background(), 1234, "user client_encoding=lol", "password", "database")
	assert.Contains(t, err.Error(), "failed to connect to")
}

func Test_defaultCreateDatabase_ErrorWhenQueryError(t *testing.T) {
	port, err := findPort()
	require.NoError(t, err)
	database := NewDatabase(Config(WithPort(port), WithDatabase("b33r")))
	if err := database.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := database.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	err = defaultCreateDatabase(context.Background(), port, "postgres", "postgres", "b33r")

	require.NotNil(t, err)
	pe, e := pgError(err)
	require.NoError(t, e)
	fmt.Printf("%+v\n", pe)
	assert.Contains(t, err.Error(), "unable to create database 'b33r'")
	assert.Equal(t, pgerrcode.DuplicateDatabase, pgErrorCode(err))
}

func Test_healthCheckDatabase_ErrorWhenSQLConnectingError(t *testing.T) {
	err := healthCheck(context.Background(), 1234, "tom client_encoding=lol", "more", "b33r")
	assert.Contains(t, err.Error(), "failed to connect")
}
