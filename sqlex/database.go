package main

import (
	"context"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
)

type Database struct {
	store  kv.Storage
	domain *domain.Domain
}

func NewDatabase(dataPath string) (*Database, error) {
	//创建存储
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.MockTiKV), mockstore.WithPath(dataPath))
	if err != nil {
		return nil, errors.Wrap(err, "无法初始化存储")
	}
	//创建domain
	dom, err := session.BootstrapSession(store)
	if err != nil {
		return nil, errors.Wrap(err, "无法初始化Domain")
	}
	//返回结果
	return &Database{
		store:  store,
		domain: dom,
	}, nil
}

func NewMemoryDatabase() (*Database, error) {
	return NewDatabase("")
}

func (d *Database) Close() error {
	d.domain.Close()
	return d.store.Close()
}

func (d *Database) CreateSessionOnDatabase(ctx context.Context, database string) (s *Session, err error) {
	//创建session
	se, err := session.CreateSessionWithDomain(d.store, d.domain)
	if err != nil {
		return nil, err
	}
	//session变量初始化
	//默认关闭clustered index
	err = se.SetGlobalSysVar(ctx, "tidb_enable_clustered_index", "OFF")
	if err != nil {
		return nil, err
	}
	s = &Session{
		Session: se,
		dbName:  database,
		domain:  d.domain,
	}
	_, err = s.Execute(ctx, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		return nil, err
	}
	_, err = s.Execute(ctx, "use "+database)
	if err != nil {
		return nil, err
	}
	return s, nil
}
