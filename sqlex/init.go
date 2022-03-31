package main

import "github.com/pingcap/tidb/util/logutil"

//ffi 容器
var container *FFIContainer

func init() {
	//初始化日志配置
	logConfig := logutil.NewLogConfig("error", "text", "", logutil.EmptyFileLogConfig, true)
	err := logutil.InitLogger(logConfig)
	if err != nil {
		panic("无法初始化Log系统")
	}

	//初始化ffi容器
	container = NewFFIContainer()
	//数据库管理模块
	database, err := NewMemoryDatabase()
	err = container.Inject(&DatabaseAPI{
		database: database,
		sessions: map[int64]*Session{},
	})
	if err != nil {
		panic(err)
	}
	//系统模块
	err = container.Inject(&SystemAPI{})
	if err != nil {
		panic(err)
	}
}
