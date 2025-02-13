package term

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"go.uber.org/zap"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/tools"
)

var (
	once            sync.Once
	defaultReceiver *TerminateReceiver
)

// TerminateReceiver 项目发布或者自动伸缩时，k8s会给本程序的应用进程发送SIGTERM信号量，这里就是监控该信号量
// 用于程序有序推出
type TerminateReceiver struct {
	NotifyChan     chan struct{} // 用于通知主程序的
	MainIsDoneChan chan struct{} // 主程序已经执行完毕，可以退出了，取消监听

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
			NotifyChan:     make(chan struct{}),
			MainIsDoneChan: make(chan struct{}),
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

	var (
		sin        os.Signal
		isMainDone bool
	)
	select {
	case <-v.MainIsDoneChan:
		isMainDone = true
		break
	case sin = <-v.listenChan:
		break
	}

	v.isStop = true
	v.log("[TerminateReceiver] [Watch] sin:%v, isMainDone:%v", sin, isMainDone)

	defer func() {
		if x := recover(); x != nil {
			v.log("[TerminateReceiver] [Watch] panic err:%v", x)
			v.log(tools.FullStack())
		}
	}()
	// TODO 后面可以根据不同的信号执行不同的 heandler for 括起来
	// v.signalHandler
	v.defaultHandler()
	v.log("[TerminateReceiver] [Watch] program is exiting. sin:%v, isMainDone:%v", sin, isMainDone)

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

	close(v.NotifyChan)
}

// Watch 同步监听信号量TERM，会直接阻塞
func (v *TerminateReceiver) SyncWatch() *TerminateReceiver {
	v.wg.Add(1)

	v.watch()

	close(v.NotifyChan)

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
		if !v.checkSignal(v.listenSignal, s) {
			v.listenSignal = append(v.listenSignal, s)
		}
	}

	return v
}

func (*TerminateReceiver) checkSignal(sl []os.Signal, v os.Signal) bool {
	for _, vv := range sl {
		if vv == v {
			return true
		}
	}
	return false
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
