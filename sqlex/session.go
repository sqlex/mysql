package main

import (
	"context"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"strings"
)

type Session struct {
	session.Session
	planBuilder *core.PlanBuilder
}

func (s *Session) GetPlan(ctx context.Context, sql string) (core.Plan, error) {
	stmts, err := s.Parse(ctx, sql)
	if err != nil {
		return nil, errors.Wrap(err, "SQL解析出错")
	}
	if len(stmts) <= 0 {
		return nil, errors.New("不存在合法的SQL语句")
	}
	plan, err := s.planBuilder.Build(ctx, stmts[0])
	if err != nil {
		return nil, errors.Wrap(err, "无法创建逻辑计划")
	}
	return plan, nil
}

func (s *Session) GetAllTables(ctx context.Context) ([]string, error) {
	results, err := s.Execute(ctx, "show tables")
	if err != nil {
		return nil, errors.Wrap(err, "无法执行遍历表SQL")
	}
	if len(results) <= 0 {
		return nil, errors.Wrap(err, "无结果集")
	}
	result := results[0]
	rows, err := session.ResultSetToStringSlice(ctx, s, result)
	if err != nil {
		return nil, errors.Wrap(err, "结果集转换失败")
	}
	tables := make([]string, 0)
	for _, row := range rows {
		tables = append(tables, row[0])
	}
	return tables, nil
}

func (s *Session) GetTableDDL(ctx context.Context, tableName string) (string, error) {
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
	tableNames, err := s.GetAllTables(ctx)
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

func (s *Session) ExecuteAndShow(ctx context.Context, sql string) {
	//执行SQL
	results, err := s.Execute(ctx, sql)
	if err != nil {
		fmt.Println("执行SQL出错:", err)
		return
	}
	if len(results) <= 0 {
		fmt.Println("无结果集, Affected Row:", s.AffectedRows())
		return
	}
	result := results[0]
	//获取计划
	plan, err := s.GetPlan(ctx, sql)
	if err != nil {
		fmt.Println(err)
		return
	}
	cols := plan.Schema().Columns
	//转换成string
	rows, err := session.ResultSetToStringSlice(ctx, s, result)
	if err != nil {
		fmt.Println("转换ResultSet出错:", err)
		return
	}
	//新建table格式化
	t := table.NewWriter()
	t.SetAutoIndex(true)
	//添加table头部
	header := make([]interface{}, len(result.Fields()))
	for index, col := range result.Fields() {
		header[index] = fmt.Sprintf("%s - %s", col.ColumnAsName.L, cols[index].RetType.CompactStr())
	}
	t.AppendHeader(header)
	//添加数据行
	for _, row := range rows {
		r := make([]interface{}, len(row))
		for index, col := range row {
			r[index] = col
		}
		t.AppendRow(r)
	}
	//输出
	fmt.Println(t.Render())
}

func (s *Session) ExecuteScript(ctx context.Context, script string) error {
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
