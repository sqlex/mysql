package main

import "C"
import (
	"bytes"
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/planner/core"
	"golang.org/x/exp/slices"
	"math/rand"
	"sync"
	"unicode/utf8"
)

//export FFIInvoke
func FFIInvoke(reqJson string) *C.char {
	respJsonStr := container.InvokeByData(reqJson)
	return C.CString(respJsonStr)
}

// DatabaseAPI 业务模块
type DatabaseAPI struct {
	database     *Database
	sessions     map[int64]*Session
	sessionsLock sync.Mutex
}

func NewDatabaseAPI() (*DatabaseAPI, error) {
	database, err := NewMemoryDatabase()
	if err != nil {
		database.Close()
		return nil, err
	}
	return &DatabaseAPI{
		database: database,
		sessions: map[int64]*Session{},
	}, nil
}

func (d *DatabaseAPI) CreateSession(database string) (int64, error) {
	//创建session
	session, err := d.database.CreateSessionOnDatabase(context.Background(), database)
	if err != nil {
		return 0, err
	}
	//session id
	sessionID := rand.Int63()
	d.sessionsLock.Lock()
	defer d.sessionsLock.Unlock()
	d.sessions[sessionID] = session
	return sessionID, nil
}

func (d *DatabaseAPI) Execute(sessionID int64, sql string) error {
	d.sessionsLock.Lock()
	if session, exist := d.sessions[sessionID]; exist {
		d.sessionsLock.Unlock()

		_, err := session.Execute(context.Background(), sql)
		return err
	} else {
		d.sessionsLock.Unlock()
		return errors.New("Session不存在")
	}
}

func (d *DatabaseAPI) ExecuteScript(sessionID int64, script string) error {
	d.sessionsLock.Lock()
	if session, exist := d.sessions[sessionID]; exist {
		d.sessionsLock.Unlock()

		return session.ExecuteScript(context.Background(), script)
	} else {
		d.sessionsLock.Unlock()
		return errors.New("Session不存在")
	}
}

type StatementType string

const (
	SelectStatementType StatementType = "Select"
	InsertStatementType StatementType = "Insert"
	UpdateStatementType StatementType = "Update"
	DeleteStatementType StatementType = "Delete"
	OtherStatementType  StatementType = "Other"
)

type InExprPosition struct {
	Not    bool `json:"not"`
	Marker int  `json:"marker"`
	Start  int  `json:"start"`
	End    int  `json:"end"`
}

type inExprNodeVisitor struct {
	sql      string
	position []InExprPosition
}

func newInExprNodeVisitor(sql string) *inExprNodeVisitor {
	return &inExprNodeVisitor{sql: sql, position: []InExprPosition{}}
}

func (v *inExprNodeVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	return n, false
}

func (v *inExprNodeVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	if inExprNode, ok := n.(*ast.PatternInExpr); ok {
		if inExprNode.Sel == nil && inExprNode.List != nil {
			if len(inExprNode.List) == 1 {
				if paramMarkerExpr, ok := inExprNode.List[0].(ast.ParamMarkerExpr); ok {
					//一路查找,直到找到 ')'
					for index := paramMarkerExpr.OriginTextPosition(); index < len(v.sql); index++ {
						if v.sql[index] == ')' {
							v.position = append(v.position, InExprPosition{
								Not:    inExprNode.Not,
								Marker: utf8.RuneCountInString(v.sql[:paramMarkerExpr.OriginTextPosition()]),
								Start:  utf8.RuneCountInString(v.sql[:inExprNode.OriginTextPosition()]),
								End:    utf8.RuneCountInString(v.sql[:index+1]),
							})
							break
						}
					}
				}
			}
		}
	}
	return n, true
}

type IsNullExprPosition struct {
	Not    bool `json:"not"`
	Marker int  `json:"marker"`
	Start  int  `json:"start"`
	End    int  `json:"end"`
}

type isNullExprNodeVisitor struct {
	sql      string
	position []IsNullExprPosition
}

func newIsNullExprNodeVisitor(sql string) *isNullExprNodeVisitor {
	return &isNullExprNodeVisitor{sql: sql, position: []IsNullExprPosition{}}
}

func (v *isNullExprNodeVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	return n, false
}

func (v *isNullExprNodeVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	if isNullExprNode, ok := n.(*ast.IsNullExpr); ok {
		if isNullExprNode.Expr != nil {
			if paramMarkerExpr, ok := isNullExprNode.Expr.(ast.ParamMarkerExpr); ok {
				//一路查找,直到找到 "null"
				for index := paramMarkerExpr.OriginTextPosition(); index+3 < len(v.sql); index++ {
					if v.sql[index:index+4] == "null" {
						v.position = append(v.position, IsNullExprPosition{
							Not:    isNullExprNode.Not,
							Marker: utf8.RuneCountInString(v.sql[:paramMarkerExpr.OriginTextPosition()]),
							Start:  utf8.RuneCountInString(v.sql[:isNullExprNode.OriginTextPosition()]),
							End:    utf8.RuneCountInString(v.sql[:index+4]),
						})
					}
				}
			}
		}
	}
	return n, true
}

type StatementInfo struct {
	Type                StatementType        `json:"type"`
	InExprPositions     []InExprPosition     `json:"inExprPositions"`
	IsNullExprPositions []IsNullExprPosition `json:"isNullExprPositions"`
}

func (d *DatabaseAPI) GetStatementInfo(sql string) (*StatementInfo, error) {
	//新建parser
	p := parser.New()
	//解析语句
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, errors.Wrap(err, "SQL解析错误")
	}

	info := &StatementInfo{
		Type: OtherStatementType,
	}

	//获取语句类型
	switch stmt.(type) {
	case *ast.SelectStmt:
		info.Type = SelectStatementType
	case *ast.SetOprStmt:
		info.Type = SelectStatementType
	case *ast.InsertStmt:
		info.Type = InsertStatementType
	case *ast.UpdateStmt:
		info.Type = UpdateStatementType
	case *ast.DeleteStmt:
		info.Type = DeleteStatementType
	default:
		info.Type = OtherStatementType
	}

	//遍历/获取所有的 in (?) 表达式
	inExprVisitor := newInExprNodeVisitor(sql)
	stmt.Accept(inExprVisitor)
	info.InExprPositions = inExprVisitor.position

	//遍历/获取所有的 ? is null 表达式
	isNullExprVisitor := newIsNullExprNodeVisitor(sql)
	stmt.Accept(isNullExprVisitor)
	info.IsNullExprPositions = isNullExprVisitor.position

	return info, nil
}

func (d *DatabaseAPI) GetSQLsOfScript(script string) ([]string, error) {
	//新建parser
	p := parser.New()
	//解析script
	stmts, _, err := p.ParseSQL(script)
	if err != nil {
		return nil, errors.Wrap(err, "SQL解析错误")
	}
	sqls := make([]string, len(stmts))
	//反向构建为string
	for i := range stmts {
		stmt := stmts[i]
		buffer := bytes.NewBuffer(nil)
		err = stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, buffer))
		if err != nil {
			return nil, errors.Wrap(err, "无法restore为SQL文本")
		}
		sqls[i] = buffer.String()
	}
	return sqls, nil
}

func (d *DatabaseAPI) GetDDL(sessionID int64) (string, error) {
	d.sessionsLock.Lock()
	if session, exist := d.sessions[sessionID]; exist {
		d.sessionsLock.Unlock()

		return session.GetDDL(context.Background())
	} else {
		d.sessionsLock.Unlock()
		return "", errors.New("Session不存在")
	}
}

func (d *DatabaseAPI) GetTableDDL(sessionID int64, table string) (string, error) {
	d.sessionsLock.Lock()
	if session, exist := d.sessions[sessionID]; exist {
		d.sessionsLock.Unlock()

		return session.GetTableDDL(context.Background(), table)
	} else {
		d.sessionsLock.Unlock()
		return "", errors.New("Session不存在")
	}
}

func (d *DatabaseAPI) GetAllTable(sessionID int64) ([]string, error) {
	d.sessionsLock.Lock()
	if session, exist := d.sessions[sessionID]; exist {
		d.sessionsLock.Unlock()

		return session.GetAllTables()
	} else {
		d.sessionsLock.Unlock()
		return nil, errors.New("Session不存在")
	}
}

// Field 字段
type Field struct {
	Name     string   `json:"name"`
	DBType   string   `json:"dbType"`
	Length   int      `json:"length"`
	Unsigned bool     `json:"unsigned"`
	Binary   bool     `json:"binary"`
	Decimal  int      `json:"decimal"`
	Elements []string `json:"elements"`

	PrimaryKey    bool `json:"isPrimaryKey"`
	MultipleKey   bool `json:"isMultipleKey"`
	AutoIncrement bool `json:"isAutoIncrement"`
	Unique        bool `json:"isUnique"`
	NotNull       bool `json:"notNull"`
	Default       bool `json:"hasDefaultValue"`
}

func newField(name string, fieldType *types.FieldType) *Field {
	return &Field{
		Name:     name,
		DBType:   types.TypeToStr(fieldType.GetType(), fieldType.GetCharset()),
		Length:   fieldType.GetFlen(),
		Unsigned: mysql.HasUnsignedFlag(fieldType.GetFlag()),
		Binary:   mysql.HasBinaryFlag(fieldType.GetFlag()),
		Decimal:  fieldType.GetDecimal(),
		Elements: fieldType.GetElems(),

		PrimaryKey:    mysql.HasPriKeyFlag(fieldType.GetFlag()),
		MultipleKey:   mysql.HasMultipleKeyFlag(fieldType.GetFlag()),
		AutoIncrement: mysql.HasAutoIncrementFlag(fieldType.GetFlag()),
		Unique:        mysql.HasUniKeyFlag(fieldType.GetFlag()),
		NotNull:       mysql.HasNotNullFlag(fieldType.GetFlag()),
		Default:       !mysql.HasNoDefaultValueFlag(fieldType.GetFlag()),
	}
}

type TableInfo struct {
	Name       string     `json:"name"`
	PrimaryKey []string   `json:"primaryKey"`
	Uniques    [][]string `json:"uniques"`
	Columns    []*Field   `json:"columns"`
}

func isContains(list [][]string, value []string) bool {
	for _, current := range list {
		if slices.Equal(current, value) {
			return true
		}
	}
	return false
}

func (d *DatabaseAPI) GetTableInfo(sessionID int64, table string) (*TableInfo, error) {
	d.sessionsLock.Lock()
	if session, exist := d.sessions[sessionID]; exist {
		d.sessionsLock.Unlock()
		//获取表信息
		tableInfo, err := session.GetTableInfo(table)
		if err != nil {
			return nil, err
		}
		//获取列信息
		columns := tableInfo.Columns
		fields := make([]*Field, len(columns))
		for index, col := range columns {
			fields[index] = newField(col.Name.String(), &col.FieldType)
		}
		//获取唯一索引信息
		uniques := make([][]string, 0)
		if tableInfo.Indices != nil {
			for _, index := range tableInfo.Indices {
				//唯一索引,但是不是主键索引
				if index.Unique {
					columnNames := make([]string, len(index.Columns))
					for i, column := range index.Columns {
						columnNames[i] = column.Name.String()
					}
					slices.Sort(columnNames)
					//判断是否已经存在了
					if !isContains(uniques, columnNames) {
						uniques = append(uniques, columnNames)
					}
				}
			}
		}
		//获取主键信息
		primaryKey := make([]string, 0)
		for _, field := range fields {
			if field.PrimaryKey {
				primaryKey = append(primaryKey, field.Name)
			}
		}
		if len(primaryKey) > 0 {
			slices.Sort(primaryKey)
			if !isContains(uniques, primaryKey) {
				uniques = append(uniques, primaryKey)
			}
		}
		if len(primaryKey) == 0 {
			primaryKey = nil
		}
		return &TableInfo{
			Name:       table,
			PrimaryKey: primaryKey,
			Uniques:    uniques,
			Columns:    fields,
		}, nil
	} else {
		d.sessionsLock.Unlock()
		return nil, errors.New("Session不存在")
	}
}

// PlanInfo 计划信息
type PlanInfo struct {
	Fields      []*Field `json:"fields"`
	MaxOneRow   bool     `json:"maxOneRow"`
	InsertTable *string  `json:"insertTable"`
}

func buildKeyInfo(lp core.LogicalPlan) {
	for _, child := range lp.Children() {
		buildKeyInfo(child)
	}
	childSchema := make([]*expression.Schema, len(lp.Children()))
	for i, child := range lp.Children() {
		childSchema[i] = child.Schema()
	}
	lp.BuildKeyInfo(lp.Schema(), childSchema)
}

func (d *DatabaseAPI) GetPlanInfo(sessionID int64, sql string) (*PlanInfo, error) {
	d.sessionsLock.Lock()
	if session, exist := d.sessions[sessionID]; exist {
		d.sessionsLock.Unlock()
		//获取逻辑计划
		plan, err := session.GetPlan(context.Background(), sql)
		if err != nil {
			return nil, err
		}
		//获取字段信息
		fields := make([]*Field, 0)
		isMaxOneRow := false
		//如果计划是一个有输出的,比如查询
		if plan.Schema() != nil && len(plan.Schema().Columns) == len(plan.OutputNames()) {
			fields = make([]*Field, len(plan.OutputNames()))
			for index, name := range plan.OutputNames() {
				col := plan.Schema().Columns[index]
				fields[index] = newField(name.ColName.String(), col.RetType)
			}
			//构建索引信息
			if logicalPlan, ok := plan.(core.LogicalPlan); ok {
				buildKeyInfo(logicalPlan)
				isMaxOneRow = logicalPlan.MaxOneRow()
			}
		}
		//如果是insert集合,则获取它插入的表
		var insertTable *string = nil
		if insertPlan, ok := plan.(*core.Insert); ok {
			tableName := insertPlan.Table.Meta().Name.String()
			insertTable = &tableName
		}
		//返回
		return &PlanInfo{
			Fields:      fields,
			MaxOneRow:   isMaxOneRow,
			InsertTable: insertTable,
		}, nil
	} else {
		d.sessionsLock.Unlock()
		return nil, errors.New("Session不存在")
	}
}

func (d *DatabaseAPI) CloseSession(sessionID int64) {
	d.sessionsLock.Lock()
	defer d.sessionsLock.Unlock()
	if session, exist := d.sessions[sessionID]; exist {
		session.Close()
		delete(d.sessions, sessionID)
	}
}

// SystemAPI 系统模块
type SystemAPI struct {
}

func (s *SystemAPI) Name() string {
	return "SqlEx Native"
}

func main() {}
