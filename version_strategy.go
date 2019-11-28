package embeddedpostgres

import "runtime"

type VersionStrategy func() (string, string, PostgresVersion)

func defaultVersionStrategy(config Config) VersionStrategy {
	return func() (operatingSystem, architecture string, version PostgresVersion) {
		return runtime.GOOS, runtime.GOARCH, config.version
	}
}
