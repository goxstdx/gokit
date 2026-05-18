// Package term provides a process-wide singleton terminate receiver for service startup.
//
// 该组件用于服务启动阶段注册一次退出信号监听，并在收到退出信号或主动退出通知后执行清理逻辑。
// 它适合主服务生命周期内的单一实例使用，不适合做可反复 start/stop 的通用组件。
//
// 使用约束：
//  1. 先完成 AddDefaultHandler、SetLogger、AddSignal 等配置，再调用 Watch 或 SyncWatch。
//  2. Watch 或 SyncWatch 在单个进程生命周期内只应启动一次，重复调用会被忽略。
//  3. 单例实例在本进程内不可重启；Watch/SyncWatch 结束后不支持再次启动监听。
//  4. 异步 Watch 模式下，Wait 是推荐的退出清理完成同步点。
//  5. VoluntaryWithdrawal 仅做最佳努力通知；若监听尚未启动，通知可能被忽略。
//
// 示例：
//
//	receiver := term.GetTerminateReceiver().
//		AddDefaultHandler(func() {
//			// 在这里执行服务退出前的清理逻辑。
//		}).
//		SetLogger(log)
//
//	receiver.Watch()
//	// 主服务逻辑...
//	receiver.Wait()
package terminate

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/tools"
)

var (
	once            sync.Once
	defaultReceiver *TerminateReceiver

	defaultListenSignal = []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}
)

// TerminateReceiver 项目发布或者自动伸缩时，k8s会给本程序的应用进程发送SIGTERM信号量，这里就是监控该信号量
// 用于程序有序推出
//
// 注意：该对象设计给服务启动阶段使用的单一实例，不是可反复 start/stop 的通用组件。
// 风险：
// 1. 单例实例在进程生命周期内只应启动一次，重复 Watch/SyncWatch 只会被忽略。
// 2. 单例实例在本进程内不可重启；Watch/SyncWatch 结束后不支持再次启动监听。
// 3. Wait 是异步 Watch 模式下推荐的退出清理完成同步点。
// 4. VoluntaryWithdrawal 仅做最佳努力通知，不保证在未启动监听前触发退出流程。
type TerminateReceiver struct {
	mainDoneChan chan struct{} // 主程序已经执行完毕，可以退出了，取消监听

	wg sync.WaitGroup

	isStop atomic.Bool
	logger logger_factory.Logger

	listenChan   chan os.Signal
	listenSignal []os.Signal // 监听的端口

	watchStarted atomic.Bool

	defaultHandler func() // 默认的处理程序
	// signalHandler  map[os.Signal]func() // 监听到Term信号量是进行的操作
}

func NewTerminateReceiver(signals ...os.Signal) *TerminateReceiver {
	signals = filterSignals(tools.SliceUnique(append(signals, defaultListenSignal...)))

	return &TerminateReceiver{
		mainDoneChan:   make(chan struct{}, 1),
		wg:             sync.WaitGroup{},
		logger:         nil,
		listenChan:     make(chan os.Signal, 1),
		listenSignal:   signals,
		defaultHandler: nil,
	}
}

// GetTerminateReceiver 返回服务启动阶段使用的进程级单例。
// 该单例按设计只应在主服务生命周期内启动一次，不支持完整重置后再复用。
func GetTerminateReceiver() *TerminateReceiver {
	once.Do(
		func() {
			defaultReceiver = NewTerminateReceiver()
		},
	)

	return defaultReceiver
}

// watch 监听信号量TERM
func (v *TerminateReceiver) watch() *TerminateReceiver {
	defer v.wg.Done()

	// 监听退出信号，用于测试 容器平滑重启和自动扩容
	signal.Notify(v.listenChan, v.listenSignal...)
	defer signal.Stop(v.listenChan)
	v.log("[TerminateReceiver] [Watch] listen signal:%v", v.listenSignal)

	v.log("[TerminateReceiver] [Watch] start watch")

	var (
		sin      os.Signal
		mainDone bool
	)
	select {
	case <-v.mainDoneChan:
		mainDone = true
		break
	case sin = <-v.listenChan:
		break
	}

	v.markStop()
	v.log("[TerminateReceiver] [Watch] sin:%v, mainDone:%v", sin, mainDone)

	defer func() {
		if x := recover(); x != nil {
			v.log("[TerminateReceiver] [Watch] panic err:%v", x)
			v.log("[TerminateReceiver] [Watch] recover %v ", tools.FullStack())
		}
	}()
	// TODO 后面可以根据不同的信号执行不同的 heandler for 括起来
	// v.signalHandler
	if v.defaultHandler != nil {
		v.defaultHandler()
	} else {
		v.log("[TerminateReceiver] [Watch] defaultHandler is nil, skip cleanup callback")
	}
	v.log("[TerminateReceiver] [Watch] program is exiting. sin:%v, mainDone:%v", sin, mainDone)

	return v
}

// Watch 异步监听信号量TERM，需要配合 Wait 一起使用。
// 该方法给服务启动阶段调用一次；重复调用会被忽略，以避免单例重复起监听协程。
func (v *TerminateReceiver) Watch() *TerminateReceiver {
	if !v.watchStarted.CompareAndSwap(false, true) {
		v.log("[TerminateReceiver] [Watch] receiver already started, ignore duplicate watch")
		return v
	}

	v.wg.Add(1)

	go v.watch()

	return v
}

func (v *TerminateReceiver) Wait() {
	v.log("[TerminateReceiver] wait...")

	v.wg.Wait()

	v.log("[TerminateReceiver] isStop: %v", v.IsStop())
}

// SyncWatch 同步监听信号量TERM，会直接阻塞。
// 该方法给服务启动阶段调用一次；重复调用会被忽略，以避免单例重复注册信号监听。
func (v *TerminateReceiver) SyncWatch() *TerminateReceiver {
	if !v.watchStarted.CompareAndSwap(false, true) {
		v.log("[TerminateReceiver] [SyncWatch] receiver already started, ignore duplicate watch")
		return v
	}

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
	for _, s := range filterSignals(signal) {
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
func (v *TerminateReceiver) SetLogger(logger logger_factory.Logger) *TerminateReceiver {
	v.logger = logger
	return v
}

// IsStop 用于检测当前实例是否已经收到了退出信号或主动退出通知。
func (v *TerminateReceiver) IsStop() bool {
	return v.isStop.Load()
}

// TriggerShutdown 用于服务内部主动触发当前实例的退出流程。
// 注意：它只做最佳努力通知，不会阻塞等待监听器启动。
func (v *TerminateReceiver) TriggerShutdown() {
	select {
	case v.mainDoneChan <- struct{}{}:
	default:
	}
}

func (v *TerminateReceiver) log(format string, args ...interface{}) {
	if v.logger != nil {
		v.logger.Infof(format, args...)
	} else {
		fmt.Printf(format+"\n", args...)
	}
}
func filterSignals(signals []os.Signal) []os.Signal {
	filtered := make([]os.Signal, 0, len(signals))
	for _, signalV := range signals {
		if signalV == nil {
			continue
		}

		switch signalV {
		case syscall.SIGKILL, syscall.SIGSTOP:
			continue
		default:
			filtered = append(filtered, signalV)
		}
	}

	return filtered
}

func (v *TerminateReceiver) markStop() {
	v.isStop.Store(true)
}
