package tracex

//
// import (
// 	"context"
// 	"os"
//
// 	"go.opentelemetry.io/otel"
// 	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
// 	"go.opentelemetry.io/otel/propagation"
// 	"go.opentelemetry.io/otel/sdk/resource"
// 	"go.opentelemetry.io/otel/sdk/trace"
// 	semconv "go.opentelemetry.io/otel/semconv/v1.14.0"
// )
//
// func InitTracerProvider(ctx context.Context, url string, serviceName string) error {
// 	exp, err := otlptracehttp.New(
// 		ctx,
// 		otlptracehttp.WithEndpoint(url),
// 		otlptracehttp.WithInsecure(),
// 	)
// 	if err != nil {
// 		return err
// 	}
//
// 	// 获取当前服务器的主机名
// 	hostName, err := os.Hostname()
// 	if err != nil {
// 		return err
// 	}
//
// 	tp := trace.NewTracerProvider(
// 		trace.WithBatcher(exp),
// 		trace.WithResource(
// 			resource.NewWithAttributes(
// 				semconv.SchemaURL,
// 				semconv.ServiceNameKey.String(serviceName),
// 				semconv.HostNameKey.String(hostName),
// 			),
// 		),
// 	)
// 	otel.SetTracerProvider(tp)
// 	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}))
// 	return nil
// }
