package logger_factory

func NewExample() (Logger, error) {
	return NewLogger(
		Config{
			DriverType: DriverSlog,
			OutputMode: OutputModeBoth,
			File: &FileConfig{
				Path: "/tmp/log_factory",
				Name: "log_factory-test.log",
			},
		},
	)
}
