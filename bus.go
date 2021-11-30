package apibus

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-apibox/api"
	"github.com/go-apibox/apiclient"
	"github.com/go-apibox/apiproxy"
	"github.com/go-apibox/apisign"
	"github.com/go-apibox/utils"
	shellquote "github.com/kballard/go-shellquote"
	"gopkg.in/fsnotify.v1"
)

type Bus struct {
	app      *api.App
	disabled bool

	moduleParamName string // 请求参数中的模块参数名，默认为api_module

	exePaths []string // 模块程序路径，{module}将被替换为模块名
	sockPath string   // unix domain socket路径，{module}将被替换为模块名
	dbPath   string   // 数据库存储路径，{module}将被替换为模块名

	watcher    *fsnotify.Watcher // 程序监控器
	exePathMap map[string]string // 程序路径对应模块代号

	modules      map[string]*Module
	subProcesses map[string]*os.Process
	liveReqCount map[string]int // 尚未结束的请求数

	mutex *sync.RWMutex
	proxy *apiproxy.Proxy

	statGateway string
	statData    map[string]*ModuleStat
}

type Module struct {
	signKey      string
	startTime    time.Time // 启动时间
	lastSendTime time.Time // 最后发送API请求的时间
	lastRecvTime time.Time // 最后接收API请求的时间
}

// 模块统计
type ModuleStat struct {
	StartCount int // 启动次数
	CallCount  int // 调用次数
}

func NewBus(app *api.App) *Bus {
	app.Error.RegisterGroupErrors("bus", ErrorDefines)

	bus := new(Bus)
	bus.app = app

	cfg := app.Config
	bus.moduleParamName = cfg.GetDefaultString("apibus.module_param_name", "api_module")
	disabled := cfg.GetDefaultBool("apibus.disabled", false)
	if disabled {
		return bus
	}

	statGateway := cfg.GetDefaultString("apibus.stat_gateway", "")
	statInterval := cfg.GetDefaultInt("apibus.stat_interval", 3600)

	modules := make(map[string]*Module, 0)

	progDir, _ := filepath.Abs(filepath.Dir(os.Args[0]))

	// 配置文件中支持多个路径，用:分隔
	exePath := cfg.GetDefaultString("apibus.exe_path", "./modules/{module}/{module}")
	exePaths := strings.Split(exePath, ":")
	for i, path := range exePaths {
		if !filepath.IsAbs(path) {
			exePaths[i] = filepath.Join(progDir, path)
		}
	}
	sockPath := cfg.GetDefaultString("apibus.sock_path", "./run/{module}.sock")
	if !filepath.IsAbs(sockPath) {
		sockPath = filepath.Join(progDir, sockPath)
	}
	dbPath := cfg.GetDefaultString("apibus.db_path", "default:./db/{module}.db")
	dbStrs := strings.Split(dbPath, ";")
	dbConfigs := make([]string, 0, len(dbStrs))
	for _, dbStr := range dbStrs {
		fields := strings.SplitN(dbStr, ":", 2)
		if len(fields) != 2 {
			continue
		}
		dbAlias, p := fields[0], fields[1]
		if !filepath.IsAbs(p) {
			p = filepath.Join(progDir, p)
		}
		dbConfigs = append(dbConfigs, fmt.Sprintf("%s:%s", dbAlias, p))
	}
	dbPath = strings.Join(dbConfigs, ";")

	bus.disabled = disabled
	bus.exePaths = exePaths
	bus.sockPath = sockPath
	bus.dbPath = dbPath
	bus.exePathMap = make(map[string]string, 0)
	bus.modules = modules
	bus.subProcesses = make(map[string]*os.Process, 0)
	bus.liveReqCount = make(map[string]int, 0)
	bus.mutex = new(sync.RWMutex)
	bus.proxy = nil

	bus.statGateway = statGateway
	bus.statData = make(map[string]*ModuleStat, 0)

	// 统计
	if bus.statGateway != "" {
		statClient := apiclient.NewClient(statGateway)
		statStartTime := utils.Timestamp()
		outIp, _ := getOutgoingIp()
		go func() {
			for {
				time.Sleep(time.Second * time.Duration(statInterval))

				if len(bus.statData) > 0 {
					dataArr := []string{outIp, fmt.Sprintf("%d", statStartTime)}
					for k, v := range bus.statData {
						dataArr = append(dataArr, fmt.Sprintf("%s|%d|%d", k, v.StartCount, v.CallCount))
					}

					params := make(url.Values)
					params.Set("Data", strings.Join(dataArr, ","))
					_, err := statClient.Post("", params, nil)
					if err != nil {
						continue
					}
				}

				// 清空计数
				statStartTime = utils.Timestamp()
				outIp, _ = getOutgoingIp()
				bus.statData = make(map[string]*ModuleStat, 0)
			}
		}()
	}

	// 程序文件变动后自动杀死旧进程
	autoKill := cfg.GetDefaultBool("apibus.autokill", true)
	if autoKill {
		// 程序文件监控
		var err error
		bus.watcher, err = fsnotify.NewWatcher()
		if err != nil {
			bus.watcher = nil
		} else {
			go func() {
				for {
					select {
					case ev := <-bus.watcher.Events:
						// 文件变更、删除、改名、创建
						if ev.Op&fsnotify.Write == fsnotify.Write ||
							ev.Op&fsnotify.Remove == fsnotify.Remove ||
							ev.Op&fsnotify.Rename == fsnotify.Rename ||
							ev.Op&fsnotify.Create == fsnotify.Create {
							moduleCode, _ := bus.exePathMap[ev.Name]
							if p, has := bus.subProcesses[moduleCode]; has {
								if err := p.Signal(os.Interrupt); err != nil {
									p.Kill()
								}
								p.Wait()
							}
						}
						// case err := <-bus.watcher.Error:
						// 	app.Logger.Warning("(apibus) inotify error: %s", err.Error())
					}
				}
			}()
		}
	}

	// 进程关闭事件处理
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, os.Kill, syscall.SIGTERM)
	go func(c chan os.Signal) {
		// Wait for a SIGINT or SIGKILL:
		sig := <-c
		bus.app.Logger.Notice("(apibus) Caught signal %s: shutting down.", sig)
		// 关闭所有子进程
		for _, p := range bus.subProcesses {
			if err := p.Signal(os.Interrupt); err != nil {
				p.Kill()
			}
			p.Wait()
		}
		signal.Stop(c)
	}(sigc)

	go bus.moduleCleaner()

	return bus
}

// 清理不活跃的模块
func (bus *Bus) moduleCleaner() {
	// 每30秒检测一次
	cleanInterval := time.Second * time.Duration(30)
	// 不活跃超时：5分钟
	livePeriod := time.Minute * time.Duration(5)

	for {
		time.Sleep(cleanInterval)

		nowTime := time.Now()

		bus.mutex.RLock()
		for moduleCode, module := range bus.modules {
			if bus.liveReqCount[moduleCode] > 0 {
				// 尚有请求未结束，跳过本次检测
				continue
			}

			// 不活跃的模块，都将被清理
			if module.lastRecvTime.Add(livePeriod).Before(nowTime) &&
				module.lastSendTime.Add(livePeriod).Before(nowTime) {
				if p, has := bus.subProcesses[moduleCode]; has {
					if err := p.Signal(os.Interrupt); err != nil {
						p.Kill()
					}
					p.Wait()
				}
			}
		}
		bus.mutex.RUnlock()
	}
}

func (bus *Bus) initProxy() error {
	bus.mutex.Lock()
	defer bus.mutex.Unlock()

	if bus.proxy == nil {
		if obj, has := bus.app.Middlewares["apiproxy"]; has {
			bus.proxy = obj.(*apiproxy.Proxy)
			bus.proxy.Enable()
		}
		// 代理中间件未启用
		if bus.proxy == nil {
			return errors.New("proxy not enabled")
		}
	}

	return nil
}

func (bus *Bus) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	if err := bus.initProxy(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	c, err := api.NewContext(bus.app, w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 检测要调用的模块
	moduleCode := c.Input.Get(bus.moduleParamName)
	if moduleCode == "" {
		action := c.Input.GetAction()
		switch action {
		case "Module.List":
			modules := make(map[string]map[string]interface{}, len(bus.modules))

			bus.mutex.RLock()
			for code, m := range bus.modules {
				lastSendTime := m.lastSendTime.Unix()
				if lastSendTime < 0 {
					lastSendTime = 0
				}
				lastRecvTime := m.lastRecvTime.Unix()
				if lastRecvTime < 0 {
					lastRecvTime = 0
				}
				modules[code] = map[string]interface{}{
					"SignKey":      m.signKey,
					"StartTime":    m.startTime.Unix(),
					"LastSendTime": lastSendTime,
					"LastRecvTime": lastRecvTime,
				}
			}
			bus.mutex.RUnlock()

			api.WriteResponse(c, modules)
			return

		case "Module.Kill":
			moduleCode := c.Input.Get("ModuleCode")
			if moduleCode == "" {
				api.WriteResponse(c, c.Error.New(api.ErrorMissingParam, "ModuleCode"))
			}

			bus.mutex.RLock()
			if p, has := bus.subProcesses[moduleCode]; has {
				if err := p.Signal(os.Interrupt); err != nil {
					p.Kill()
				}
				p.Wait()
			}
			bus.mutex.RUnlock()

			api.WriteResponse(c, nil)
		}
		next(w, r)
		return
	} else if matched, err := regexp.MatchString(`^[0-9a-zA-Z\-_]+$`, moduleCode); err != nil && !matched {
		// 必须为数字字母或-_
		http.NotFound(w, r)
		return
	}

	// 调用者身份识别
	var callerModule *Module
	callerModuleCode := c.Input.Get("api_caller")
	if callerModuleCode != "" { // code为空的是网关
		bus.mutex.RLock()
		if callerModule, has := bus.modules[callerModuleCode]; has {
			callerModule.lastSendTime = time.Now()
		}
		bus.mutex.RUnlock()
	}

	// 检测模块是否已启动
	bus.mutex.Lock()
	if module, has := bus.modules[moduleCode]; has {
		module.lastRecvTime = time.Now()

		// 不能立即解锁，存在并发写map
		// // 立即解锁
		// bus.mutex.Unlock()

		if bus.statGateway != "" {
			if v, has := bus.statData[moduleCode]; has {
				v.CallCount += 1
			} else {
				bus.statData[moduleCode] = &ModuleStat{0, 1}
			}
		}

		// 代理调用
		bus.liveReqCount[moduleCode] += 1
		if callerModule != nil {
			bus.liveReqCount[callerModuleCode] += 1
		}

		bus.mutex.Unlock()

		next(w, r)

		bus.mutex.Lock()
		nowTime := time.Now()
		module.lastRecvTime = nowTime
		bus.liveReqCount[moduleCode] -= 1
		if callerModule != nil {
			callerModule.lastSendTime = nowTime
			bus.liveReqCount[callerModuleCode] -= 1
		}
		bus.mutex.Unlock()

		return
	}

	// 取模块路径
	var moduleExe string
	var moduleExePath string
	var cmdFields []string
	hasValidPath := false
	for _, exePath := range bus.exePaths {
		moduleExe = strings.Replace(exePath, "{module}", moduleCode, -1)
		if moduleExe == "" {
			continue
		}

		// 分析出路径和参数
		var err error
		cmdFields, err = shellquote.Split(moduleExe)
		if err != nil {
			continue
		}
		moduleExePath, err = filepath.Abs(cmdFields[0])
		if err != nil {
			continue
		}

		if utils.FileExists(moduleExePath) {
			hasValidPath = true
			break
		}
	}
	if !hasValidPath {
		bus.mutex.Unlock()
		api.WriteResponse(c, bus.app.Error.NewGroupError("bus", errorWrongModulePath))
		return
	}

	moduleSock := strings.Replace(bus.sockPath, "{module}", moduleCode, -1)
	moduleDb := strings.Replace(bus.dbPath, "{module}", moduleCode, -1)
	if _, err := os.Stat(moduleSock); err == nil {
		os.Remove(moduleSock)
	}

	// 启动模块
	cmd := exec.Command(cmdFields[0], cmdFields[1:]...)
	debugLevel := os.Getenv("DEBUG_LEVEL")
	cmd.Env = append(cmd.Env, fmt.Sprintf("PATH=%s", os.Getenv("PATH")))
	cmd.Env = append(cmd.Env, fmt.Sprintf("LANG=%s", os.Getenv("LANG")))
	cmd.Env = append(cmd.Env, fmt.Sprintf("TERM=%s", os.Getenv("TERM")))
	cmd.Env = append(cmd.Env, fmt.Sprintf("LS_COLORS=%s", os.Getenv("LS_COLORS")))
	if debugLevel != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("DEBUG_LEVEL=%s", debugLevel))
	}
	cmd.Env = append(cmd.Env, fmt.Sprintf("APP_HOST=modules.%s", moduleCode))
	cmd.Env = append(cmd.Env, fmt.Sprintf("APP_ADDR=%s", moduleSock))
	cmd.Env = append(cmd.Env, fmt.Sprintf("APP_DB=%s", moduleDb))
	accessLog := os.Getenv("ACCESS_LOG")
	if accessLog != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("ACCESS_LOG=%s", accessLog))
	}
	errorLog := os.Getenv("ERROR_LOG")
	if errorLog != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("ERROR_LOG=%s", errorLog))
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Start()
	if err != nil {
		bus.mutex.Unlock()
		api.WriteResponse(c, bus.app.Error.NewGroupError("bus", errorModuleStartFailed).SetMessage(err.Error()))
		return
	}

	nowTime := time.Now()
	module := new(Module)
	module.signKey = utils.RandStringN(32)
	module.startTime = nowTime
	module.lastRecvTime = nowTime

	bus.subProcesses[moduleCode] = cmd.Process
	bus.modules[moduleCode] = module
	bus.liveReqCount[moduleCode] = 0

	// 模块进程结束检测
	go func() {
		cmd.Wait()

		bus.mutex.Lock()

		delete(bus.modules, moduleCode)
		delete(bus.subProcesses, moduleCode)
		delete(bus.liveReqCount, moduleCode)

		if bus.watcher != nil {
			moduleExeDir := filepath.Dir(moduleExePath)
			bus.watcher.Remove(moduleExeDir)
			delete(bus.exePathMap, moduleExePath)
		}

		bus.mutex.Unlock()

		bus.proxy.DeleteBackend("module-" + moduleCode)
	}()

	bus.addModule(moduleCode, moduleSock, module.signKey)

	// 监听文件变化
	if bus.watcher != nil {
		moduleExeDir := filepath.Dir(moduleExePath)
		if err := bus.watcher.Add(moduleExeDir); err == nil {
			bus.exePathMap[moduleExePath] = moduleCode
		}
	}

	if bus.statGateway != "" {
		if v, has := bus.statData[moduleCode]; has {
			v.StartCount += 1
			v.CallCount += 1
		} else {
			bus.statData[moduleCode] = &ModuleStat{1, 1}
		}
	}

	// 代理调用
	bus.liveReqCount[moduleCode] += 1
	if callerModule != nil {
		bus.liveReqCount[callerModuleCode] += 1
	}

	// 启动并初始化完成后才解锁
	bus.mutex.Unlock()

	next(w, r)

	bus.mutex.Lock()
	nowTime = time.Now()
	module.lastRecvTime = nowTime
	bus.liveReqCount[moduleCode] -= 1
	if callerModule != nil {
		callerModule.lastSendTime = nowTime
		bus.liveReqCount[callerModuleCode] -= 1
	}
	bus.mutex.Unlock()
}

// 添加预设模块
func (bus *Bus) AddModule(moduleCode, moduleSock string) error {
	if err := bus.initProxy(); err != nil {
		return err
	}

	bus.mutex.Lock()
	nowTime := time.Now()
	module := new(Module)
	module.signKey = utils.RandStringN(32)
	module.startTime = nowTime
	module.lastRecvTime = nowTime

	bus.modules[moduleCode] = module

	bus.addModule(moduleCode, moduleSock, module.signKey)

	bus.mutex.Unlock()

	return nil
}

func (bus *Bus) addModule(moduleCode, moduleSock, signKey string) {
	// 添加到 proxy 模块中
	backend := new(apiproxy.Backend)
	backend.GWURL = fmt.Sprintf("http://modules.%s/", moduleCode)
	backend.GWADDR = moduleSock
	backend.Method = "" // 与请求一致
	backend.SignKey = signKey
	// 启用apinonce会额外占用内存，而且对内部应用来讲没什么用
	backend.NonceLength = 0
	backend.MatchParams = map[string][]string{
		bus.moduleParamName: []string{moduleCode},
	}
	backend.DeleteParams = []string{bus.moduleParamName}
	bus.proxy.AddBackend("module-"+moduleCode, backend)

	// sign key写入到模块
	initCostTime := 0
	interval := 100 // 100ms
	for {
		// 判断进程是否还存活
		if p, has := bus.subProcesses[moduleCode]; has {
			if _, err := os.FindProcess(p.Pid); err != nil {
				// 查找不到进程，则退出
				break
			} else {
				// 查找到进程，但进程已结束，则退出
				// os: process already finished
				if err := p.Signal(syscall.Signal(0)); err != nil {
					break
				}
			}
		}

		// 如果是unix domain sock，得等到sock文件出来后再连接
		if strings.HasPrefix(backend.GWADDR, "/") {
			if !utils.FileExists(backend.GWADDR) {
				time.Sleep(time.Second)
				continue
			}
		}

		// 重试时间间隔，每次加300ms
		time.Sleep(time.Millisecond * time.Duration(interval))
		client := apiclient.NewClient(backend.GWURL)
		client.GWADDR = backend.GWADDR
		// 启用apinonce会额外占用内存，而且对内部应用来讲没什么用
		// client.NonceEnabled = true
		// client.NonceLength = 16
		client.NonceEnabled = false
		params := make(url.Values)
		params.Set("apisign.sign_key", backend.SignKey)
		params.Set("apisign.disabled", "N")
		params.Set("apiproxy.bus.gwurl", fmt.Sprintf("http://%s/", bus.app.Host))
		params.Set("apiproxy.bus.gwaddr", bus.app.Addr)
		// 启用apinonce会额外占用内存，而且对内部应用来讲没什么用
		// params.Set("apiproxy.bus.nonce_enabled", "Y")
		// params.Set("apiproxy.bus.nonce_length", "16")
		params.Set("apiproxy.bus.override_params.api_caller", moduleCode)
		bus.app.Logger.Notice("(apibus) Initialize module:%s...", moduleCode)
		if obj, ok := bus.app.Middlewares["apisign"]; ok {
			sign := obj.(*apisign.Sign)
			params.Set("apiproxy.bus.sign_key", sign.GetSignKey())
		}
		resp, err := client.Get("APIBox.Init", params, nil)
		if err == nil {
			if result, err := resp.Result(); err == nil {
				bus.app.Logger.Notice("(apibus) module:%s return code: %s", moduleCode, result.CODE)
				if result.CODE == "ok" {
					break
				}
			}
		} else {
			bus.app.Logger.Notice("(apibus) module:%s init failed: %s", moduleCode, err.Error())
		}
		initCostTime += interval
		interval += 300

		// 初始化超时
		if initCostTime > 30000 {
			bus.app.Logger.Notice("(apibus) module:%s init failed: init timeout", moduleCode)
			break
		}
	}
}
