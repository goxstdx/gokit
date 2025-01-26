package hooks

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type prefixHook struct {
	prefix string

	_logger *zap.SugaredLogger
}

func (r prefixHook) DialHook(hook redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		r._logger.Info("dialing %s %s", network, addr)
		conn, err := hook(ctx, network, addr)
		r._logger.Info("finished dialing %s %s", network, addr)
		return conn, err
	}
}

func (r prefixHook) ProcessHook(hook redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		r._logger.Info("starting processing: <%s>", cmd)
		err := r.process(ctx, cmd)
		r._logger.Info("finished processing: <%s>, err: %v", cmd, err)

		if err == nil {
			err = hook(ctx, cmd)
			r._logger.Info("exec err: <%v>", err)
		}

		return err
	}
}

func (r prefixHook) ProcessPipelineHook(hook redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		r._logger.Info("pipeline starting processing: %v", cmds)

		var err error
		for i := range cmds {
			err = r.process(ctx, cmds[i])
			if err != nil {
				r._logger.Info("process fail, cmds[%d]: <%s>, err: %v", i, cmds[i], err)

				break
			}
		}

		r._logger.Info("pipeline finished processing: %v, err: %v", cmds, err)

		if err == nil {
			err = hook(ctx, cmds)
			r._logger.Info("exec err: <%v>", err)
		}

		return err
	}
}

func (r *prefixHook) process(_ context.Context, cmd redis.Cmder) error {
	// 第一个是命令，第二个才可能是key
	if len(cmd.Args()) > 1 {
		switch cmd.Name() {
		case "BLMOVE", "LMOVE", "BRPOPLPUSH", "RPOPLPUSH", "SMOVE", "RENAMENX", "NAMENX":
			cmd.Args()[1] = r.addPrefixToKey(cmd.Args()[1])
			cmd.Args()[2] = r.addPrefixToKey(cmd.Args()[2])
		case "BLMPOP", "LMPOP":
			num, ok := cmd.Args()[1].(int)
			if ok {
				// 下标 2 开始为key
				for i := 0; i < num; i++ {
					pStr, ok := cmd.Args()[2+i].(string)
					if ok && strings.ToUpper(pStr) != "LEFT" && strings.ToUpper(pStr) != "RIGHT" && strings.ToUpper(pStr) != "COUNT" {
						cmd.Args()[2+i] = r.addPrefixToKey(pStr)
					}
				}
			}
		default:
			cmd.Args()[1] = r.addPrefixToKey(cmd.Args()[1])
		}
	}

	return nil
}

func (r *prefixHook) addPrefixToKey(key interface{}) string {
	// 为空或者有 : 后缀
	if r.prefix == "" || strings.HasSuffix(r.prefix, ":") {
		return fmt.Sprintf("%s%s", r.prefix, key)
	} else {
		return fmt.Sprintf("%s:%s", r.prefix, key)
	}
}

func NewPrefixHook(prefix string, logger *zap.SugaredLogger) redis.Hook {
	return prefixHook{prefix: prefix, _logger: logger}
}
