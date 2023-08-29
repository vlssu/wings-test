package server

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"emperror.dev/errors"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/environment"
)

type CrashHandler struct {
	mu sync.RWMutex

	// Tracks the time of the last server crash event.
	lastCrash time.Time
}

// Returns the time of the last crash for this server instance.
func (cd *CrashHandler) LastCrashTime() time.Time {
	cd.mu.RLock()
	defer cd.mu.RUnlock()

	return cd.lastCrash
}

// Sets the last crash time for a server.
func (cd *CrashHandler) SetLastCrash(t time.Time) {
	cd.mu.Lock()
	cd.lastCrash = t
	cd.mu.Unlock()
}

// Looks at the environment exit state to determine if the process exited cleanly or
// if it was the result of an event that we should try to recover from.
//
// This function assumes it is called under circumstances where a crash is suspected
// of occurring. It will not do anything to determine if it was actually a crash, just
// look at the exit state and check if it meets the criteria of being called a crash
// by Wings.
//
// If the server is determined to have crashed, the process will be restarted and the
// counter for the server will be incremented.
func (s *Server) handleServerCrash() error {
	// No point in doing anything here if the server isn't currently offline, there
	// is no reason to do a crash detection event. If the server crash detection is
	// disabled we want to skip anything after this as well.
	if s.Environment.State() != environment.ProcessOfflineState || !s.Config().CrashDetectionEnabled {
		if !s.Config().CrashDetectionEnabled {
			s.Log().Debug("服务器触发了崩溃检测，但处理程序已禁用服务器进程")
			s.PublishConsoleOutputFromDaemon("中止自动重启，此实例禁用崩溃检测。")
		}

		return nil
	}

	exitCode, oomKilled, err := s.Environment.ExitState()
	if err != nil {
		return errors.Wrap(err, "无法获取服务器进程的退出状态")
	}

	// If the system is not configured to detect a clean exit code as a crash, and the
	// crash is not the result of the program running out of memory, do nothing.
	if exitCode == 0 && !oomKilled && !config.Get().System.CrashDetection.DetectCleanExitAsCrash {
		s.Log().Debug("服务器退出并成功退出代码;系统配置为不将其检测为崩溃")
		return nil
	}

	s.PublishConsoleOutputFromDaemon("---------- 检测到服务器进程处于崩溃状态！ ----------")
	s.PublishConsoleOutputFromDaemon(fmt.Sprintf("退出代码: %d", exitCode))
	s.PublishConsoleOutputFromDaemon(fmt.Sprintf("内存不足: %t", oomKilled))

	c := s.crasher.LastCrashTime()
	timeout := config.Get().System.CrashDetection.Timeout

	// If the last crash time was within the last `timeout` seconds we do not want to perform
	// an automatic reboot of the process. Return an error that can be handled.
	//
	// If timeout is set to 0, always reboot the server (this is probably a terrible idea, but some people want it)
	if timeout != 0 && !c.IsZero() && c.Add(time.Second*time.Duration(config.Get().System.CrashDetection.Timeout)).After(time.Now()) {
		s.PublishConsoleOutputFromDaemon("正在中止自动重启，上次崩溃发生在 " + strconv.Itoa(timeout) + " 秒内。")
		return &crashTooFrequent{}
	}

	s.crasher.SetLastCrash(time.Now())

	return errors.Wrap(s.HandlePowerAction(PowerActionStart), "检测到崩溃后无法启动服务器")
}
