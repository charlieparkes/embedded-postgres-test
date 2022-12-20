package embeddedpostgres

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/google/uuid"
)

type initDatabase func(binaryExtractLocation, runtimePath, pgDataDir, username, password, locale string, logger *os.File) error
type createDatabase func(ctx context.Context, port uint32, username, password, database string) error

func defaultInitDatabase(binaryExtractLocation, runtimePath, pgDataDir, username, password, locale string, logger *os.File) error {
	passwordFile, err := createPasswordFile(runtimePath, password)
	if err != nil {
		return err
	}

	args := []string{
		"-A", "password",
		"-U", username,
		"-D", pgDataDir,
		fmt.Sprintf("--pwfile=%s", passwordFile),
	}

	if locale != "" {
		args = append(args, fmt.Sprintf("--locale=%s", locale))
	}

	postgresInitDBBinary := filepath.Join(binaryExtractLocation, "bin/initdb")
	postgresInitDBProcess := exec.Command(postgresInitDBBinary, args...)
	postgresInitDBProcess.Stderr = logger
	postgresInitDBProcess.Stdout = logger

	if err = postgresInitDBProcess.Run(); err != nil {
		return fmt.Errorf("unable to init database using '%s': %w", postgresInitDBProcess.String(), err)
	}

	if err = os.Remove(passwordFile); err != nil {
		return fmt.Errorf("unable to remove password file '%v': %w", passwordFile, err)
	}

	return nil
}

func createPasswordFile(runtimePath, password string) (string, error) {
	passwordFileLocation := filepath.Join(runtimePath, "pwfile-"+uuid.NewString())
	if err := ioutil.WriteFile(passwordFileLocation, []byte(password), 0600); err != nil {
		return "", fmt.Errorf("unable to write password file to %s", passwordFileLocation)
	}

	return passwordFileLocation, nil
}

func defaultCreateDatabase(ctx context.Context, port uint32, username, password, database string) (err error) {
	if database == "postgres" {
		return nil
	}

	pool, err := connect(ctx, port, username, password, "postgres")
	if err != nil {
		return err
	}
	defer pool.Close()

	_, err = pool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", database))
	if err != nil {
		return fmt.Errorf("unable to create database '%s': %w", database, err)
	}
	return nil
}
