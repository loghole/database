package hooks

// Config is internal hook config with specified information.
type Config struct {
	ReconnectFn func() error

	Addr           string
	User           string
	Database       string
	CertPath       string
	Type           string
	ReadTimeout    string
	WriteTimeout   string
	DataSourceName string
	DriverName     string
	Instance       string
}
