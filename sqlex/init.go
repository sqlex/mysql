package main

import (
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/logutil"
)

// ffi 容器
var container *FFIContainer

func init() {
	//初始化日志配置
	logConfig := logutil.NewLogConfig("error", "text", "", logutil.EmptyFileLogConfig, true)
	err := logutil.InitLogger(logConfig)
	if err != nil {
		panic("无法初始化Log系统")
	}
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableTelemetry = false
		conf.Performance.RunAutoAnalyze = false
		conf.Instance.EnableSlowLog.Store(false)
		conf.Instance.RecordPlanInSlowLog = 0
		conf.Security.SkipGrantTable = true
		conf.Binlog.Enable = false
	})

	//初始化ffi容器
	container = NewFFIContainer()
	//数据库管理模块
	databaseAPI, err := NewDatabaseAPI()
	err = container.Inject(databaseAPI)
	if err != nil {
		panic(err)
	}
	//系统模块
	err = container.Inject(&SystemAPI{})
	if err != nil {
		panic(err)
	}
}
