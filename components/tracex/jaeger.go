package tracex

import (
	"fmt"
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
)

func GetDefaultConfig(endpoint string, param float64) *config.Configuration {
	cfg := &config.Configuration{
		Sampler: &config.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: param,
		},
		Reporter: &config.ReporterConfig{
			LogSpans: true,
			// CollectorEndpoint: "http://127.0.0.1:14268/api/traces",
			CollectorEndpoint: endpoint,
			// LocalAgentHostPort: "127.0.0.1:6831",
		},
	}
	return cfg
}

func Init(service string, endpoint string, param float64, logger logger_factory.Logger) {
	InitJaeger(service, GetDefaultConfig(endpoint, param), logger)
}

func InitJaeger(service string, cfg *config.Configuration, logger logger_factory.Logger) (opentracing.Tracer, io.Closer) {
	cfg.ServiceName = service
	tracer, closer, err := cfg.NewTracer(
		// config.Logger(jaeger.StdLogger),
		config.Logger(decoratorJaegerLog(logger)),
	)
	if err != nil {
		panic(fmt.Sprintf("Error: connot init Jaeger: %v\n", err))
	}
	opentracing.SetGlobalTracer(tracer)
	return tracer, closer
}

type jaegerLogger struct {
	logger_factory.Logger
}

func (j jaegerLogger) Error(msg string) {
	j.Logger.Error(msg)
}

func decoratorJaegerLog(logger logger_factory.Logger) jaeger.Logger {
	return jaegerLogger{Logger: logger}
}
