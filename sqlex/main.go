package main

import "C"
import (
	"bytes"
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	types2 "github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
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

type StatementInfo struct {
	Type            StatementType    `json:"type"`
	InExprPositions []InExprPosition `json:"inExprPositions"`
	HasLimit        bool             `json:"hasLimit"`
	LimitRows       uint64           `json:"limitRows"`
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

	//顶级limit节点
	var topLevelLimitNode *ast.Limit

	//获取语句类型
	switch stmt := stmt.(type) {
	case *ast.SelectStmt:
		info.Type = SelectStatementType
		topLevelLimitNode = stmt.Limit
	case *ast.SetOprStmt:
		info.Type = SelectStatementType
		topLevelLimitNode = stmt.Limit
	case *ast.InsertStmt:
		info.Type = InsertStatementType
	case *ast.UpdateStmt:
		info.Type = UpdateStatementType
	case *ast.DeleteStmt:
		info.Type = DeleteStatementType
	default:
		info.Type = OtherStatementType
	}

	//判断顶级limit节点是否有常量值
	if topLevelLimitNode != nil {
		if topLevelLimitNode.Count != nil {
			if countNode, ok := topLevelLimitNode.Count.(*driver.ValueExpr); ok {
				if countNode.Kind() == types2.KindUint64 {
					info.HasLimit = true
					info.LimitRows = countNode.GetUint64()
				}
			}
		}
	}

	//遍历/获取所有的 in (?) 表达式
	inExprVisitor := newInExprNodeVisitor(sql)
	stmt.Accept(inExprVisitor)
	info.InExprPositions = inExprVisitor.position

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

		return session.GetAllTables(context.Background())
	} else {
		d.sessionsLock.Unlock()
		return nil, errors.New("Session不存在")
	}
}

// Field 字段
type Field struct {
	Name     string      `json:"name"`
	TypeId   JavaSQLType `json:"typeId"`
	TypeName string      `json:"typeName"`
	Length   int         `json:"length"`
	Unsigned bool        `json:"unsigned"`
	Binary   bool        `json:"binary"`
	Decimal  int         `json:"decimal"`
	Elements []string    `json:"elements"`
}

func (d *DatabaseAPI) GetFields(sessionID int64, sql string) ([]*Field, error) {
	d.sessionsLock.Lock()
	if session, exist := d.sessions[sessionID]; exist {
		d.sessionsLock.Unlock()
		//获取逻辑计划
		plan, err := session.GetPlan(context.Background(), sql)
		if err != nil {
			return nil, err
		}
		//获取字段
		fields := make([]*Field, len(plan.OutputNames()))
		for index, name := range plan.OutputNames() {
			col := plan.Schema().Columns[index]
			fields[index] = &Field{
				Name:     name.ColName.O,
				TypeId:   mySQLType2JavaType(col.RetType.Tp, false),
				TypeName: types.TypeToStr(col.RetType.Tp, col.RetType.Charset),
				Length:   col.RetType.Flen,
				Unsigned: mysql.HasUnsignedFlag(col.RetType.Flag),
				Binary:   mysql.HasBinaryFlag(col.RetType.Flag),
				Decimal:  col.RetType.Decimal,
				Elements: col.RetType.Elems,
			}
		}
		return fields, nil
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
