package logger_factory

func NewExample() (Logger, error) {
	return NewLogger(
		Config{
			DriverType: DriverSlog,
			OutputMode: OutputModeConsole,
		},
	)
}
