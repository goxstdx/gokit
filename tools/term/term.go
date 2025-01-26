package term

import (
	"fmt"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"

	"go.uber.org/zap"

	"haochezhu.club/go_template_common/tools"
)

var (
	once            sync.Once
	defaultReceiver *TerminateReceiver
)

type ExeParam struct {
	Obj    interface{}
	Method string
}

// TerminateReceiver 项目发布或者自动伸缩时，k8s会给本程序的应用进程发送SIGTERM信号量，这里就是监控该信号量
// 用于程序有序推出
type TerminateReceiver struct {
	wg sync.WaitGroup

	isStop bool
	logger *zap.Logger

	listenChan   chan os.Signal
	listenSignal []os.Signal // 监听的端口

	defaultHandler func() // 默认的处理程序
	// signalHandler  map[os.Signal]func() // 监听到Term信号量是进行的操作
}

// GetTerminateReceiver 单例模式
func GetTerminateReceiver() *TerminateReceiver {
	once.Do(func() {
		defaultReceiver = &TerminateReceiver{
			wg:             sync.WaitGroup{},
			isStop:         false,
			logger:         nil,
			listenChan:     make(chan os.Signal),
			listenSignal:   []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL},
			defaultHandler: nil,
		}
	})

	return defaultReceiver
}

// watch 监听信号量TERM
func (v *TerminateReceiver) watch() *TerminateReceiver {
	defer v.wg.Done()

	if v.defaultHandler == nil {
		panic("[TerminateReceiver] [Watch] defaultHandler is nil")
	}

	// 监听退出信号，用于测试 容器平滑重启和自动扩容
	signal.Notify(v.listenChan, v.listenSignal...)
	v.log("[TerminateReceiver] [Watch] listen signal:%v", v.listenSignal)

	v.log("[TerminateReceiver] [Watch] start watch")

	sin := <-v.listenChan

	v.isStop = true
	v.log("[TerminateReceiver] [Watch] sin:%v", sin)

	defer func() {
		if x := recover(); x != nil {
			v.log("[TerminateReceiver] [Watch] panic err:%v", x)
			v.log(tools.FullStack())
		}
	}()
	// TODO 后面可以根据不同的信号执行不同的 heandler for 括起来
	// v.signalHandler
	v.defaultHandler()
	v.log("[TerminateReceiver] [Watch] program is exiting. sign:%v", sin)

	return v
}

// Watch 异步 监听信号量TERM，需要配合 Wait 一起使用
func (v *TerminateReceiver) Watch() *TerminateReceiver {
	v.wg.Add(1)

	go v.watch()

	return v
}

func (v *TerminateReceiver) Wait() {
	v.log("[TerminateReceiver] wait...")

	v.wg.Wait()

	v.log("[TerminateReceiver] isStop: %v", v.isStop)

	//os.Exit(0)
}

// Watch 同步监听信号量TERM，会直接阻塞
func (v *TerminateReceiver) SyncWatch() *TerminateReceiver {
	v.wg.Add(1)

	v.watch()

	return v
}

// AddDefaultHandler 增加监听信号量TERM时，所需要进行的操作
func (v *TerminateReceiver) AddDefaultHandler(run func()) *TerminateReceiver {
	if run == nil {
		return v
	}
	v.defaultHandler = run

	return v
}

// AddSignal 添加额外的监听信号量
func (v *TerminateReceiver) AddSignal(signal ...os.Signal) *TerminateReceiver {
	for _, s := range signal {
		if !slices.Contains(v.listenSignal, s) {
			v.listenSignal = append(v.listenSignal, s)
		}
	}

	return v
}

// SetLogger 使用log
func (v *TerminateReceiver) SetLogger(logger *zap.Logger) *TerminateReceiver {
	v.logger = logger
	return v
}

func (v *TerminateReceiver) log(format string, args ...interface{}) {
	if v.logger != nil {
		v.logger.Sugar().Infof(format, args...)
	} else {
		fmt.Printf(format+"\n", args...)
	}
}

// IsStop 用于检测是否收到了Term信号量
func IsStop() bool {
	return GetTerminateReceiver().isStop
}
