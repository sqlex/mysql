package main

import (
	"context"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/hint"
)

type Database struct {
	store  kv.Storage
	domain *domain.Domain
}

func NewDatabase(dataPath string) (*Database, error) {
	//创建存储
	store, err := mockstore.NewMockStore(mockstore.WithPath(dataPath))
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

func (d *Database) CreateSession() (s *Session, err error) {
	//创建session
	se, err := session.CreateSessionWithDomain(d.store, d.domain)
	if err != nil {
		return nil, err
	}
	//创建计划构建器
	builder := core.NewPlanBuilder()
	//初始化builder
	builder, _ = builder.Init(se, d.domain.InfoSchema(), &hint.BlockHintProcessor{})
	return &Session{
		Session: se,
		domain:  d.domain,
	}, nil
}

func (d *Database) CreateSessionOnDatabase(ctx context.Context, database string) (s *Session, err error) {
	s, err = d.CreateSession()
	if err != nil {
		return nil, err
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
