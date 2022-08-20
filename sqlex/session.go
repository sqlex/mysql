package main

import (
	"context"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/util/hint"
	"golang.org/x/exp/slices"
	"strings"
	"sync"
)

type Session struct {
	session.Session
	dbName      string
	sessionLock sync.Mutex
	domain      *domain.Domain
}

func (s *Session) getDBInfo() (*model.DBInfo, error) {
	var dbInfo *model.DBInfo
	dbInfos := sessiontxn.GetTxnManager(s).GetTxnInfoSchema().AllSchemas()
	for _, current := range dbInfos {
		if current.Name.String() == s.dbName {
			dbInfo = current
		}
	}
	if dbInfo == nil {
		return nil, errors.Errorf("找不到数据库 %s", s.dbName)
	}
	return dbInfo, nil
}

func (s *Session) GetPlan(ctx context.Context, sql string) (core.Plan, error) {
	s.sessionLock.Lock()
	defer s.sessionLock.Unlock()
	stmts, err := s.Parse(ctx, sql)
	if err != nil {
		return nil, errors.Wrap(err, "SQL解析出错")
	}
	if len(stmts) <= 0 {
		return nil, errors.New("不存在合法的SQL语句")
	}
	stmt := stmts[0]
	//准备事务上下文
	err = s.Session.PrepareTxnCtx(ctx)
	if err != nil {
		return nil, err
	}
	//在方法结束时回滚
	defer s.Session.RollbackTxn(ctx)
	err = core.Preprocess(s, stmt, core.InPrepare)
	if err != nil {
		return nil, errors.Wrap(err, "SQL语句处理失败")
	}
	builder, _ := core.NewPlanBuilder().Init(s, s.domain.InfoSchema(), &hint.BlockHintProcessor{})
	plan, err := builder.Build(ctx, stmt)
	if err != nil {
		return nil, errors.Wrap(err, "无法创建逻辑计划")
	}
	return plan, nil
}

func (s *Session) GetAllTables() ([]string, error) {
	s.sessionLock.Lock()
	defer s.sessionLock.Unlock()

	dbInfo, err := s.getDBInfo()
	if err != nil {
		return nil, err
	}
	tables := make([]string, 0)
	for _, table := range dbInfo.Tables {
		tables = append(tables, table.Name.String())
	}
	slices.Sort(tables)
	return tables, nil
}

func (s *Session) GetTableInfo(table string) (*model.TableInfo, error) {
	s.sessionLock.Lock()
	defer s.sessionLock.Unlock()

	dbInfo, err := s.getDBInfo()
	if err != nil {
		return nil, err
	}

	var tableInfo *model.TableInfo
	for _, current := range dbInfo.Tables {
		if current.Name.String() == table {
			tableInfo = current
		}
	}
	if tableInfo == nil {
		return nil, errors.Errorf("找不到表 %s", table)
	}
	return tableInfo, nil
}

func (s *Session) GetTableDDL(ctx context.Context, tableName string) (string, error) {
	s.sessionLock.Lock()
	defer s.sessionLock.Unlock()
	results, err := s.Execute(ctx, "show create table "+tableName)
	if err != nil {
		return "", errors.Wrap(err, "无法执行表描述SQL")
	}
	if len(results) <= 0 {
		return "", errors.Wrap(err, "无结果集")
	}
	result := results[0]
	rows, err := session.ResultSetToStringSlice(ctx, s, result)
	if err != nil {
		return "", errors.Wrap(err, "结果集转换失败")
	}
	if len(rows) == 1 && len(rows[0]) == 2 {
		return rows[0][1], nil
	}
	return "", errors.New("表结构结果不正确")
}

func (s *Session) GetDDL(ctx context.Context) (string, error) {
	tableNames, err := s.GetAllTables()
	if err != nil {
		return "", err
	}
	databaseDDL := ""
	for _, tableName := range tableNames {
		tableDDL, err := s.GetTableDDL(ctx, tableName)
		if err != nil {
			return "", err
		}
		databaseDDL += fmt.Sprintf("/* %s */\n%s;\n\n", tableName, tableDDL)
	}
	return strings.TrimSpace(databaseDDL), nil
}

func (s *Session) ExecuteScript(ctx context.Context, script string) error {
	s.sessionLock.Lock()
	defer s.sessionLock.Unlock()
	//将script解析为单条语句集合
	stmts, err := s.Parse(ctx, script)
	if err != nil {
		return errors.Wrap(err, "SQL解析失败")
	}
	//挨个执行
	for _, stmt := range stmts {
		_, err = s.ExecuteStmt(ctx, stmt)
		if err != nil {
			return errors.Wrap(err, "SQL执行失败")
		}
	}
	return nil
}
