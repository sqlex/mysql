package main

import "C"
import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
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

type LimitPosition struct {
	HasOffset bool `json:"hasOffset"`
	Count     int  `json:"count"`
	Offset    int  `json:"offset"`
}

type limitNodeVisitor struct {
	sql      string
	position []LimitPosition
}

func newLimitNodeVisitor(sql string) *limitNodeVisitor {
	return &limitNodeVisitor{sql: sql, position: []LimitPosition{}}
}

func (l *limitNodeVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	return n, false
}

func (l *limitNodeVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	if limitNode, ok := n.(*ast.Limit); ok {
		hasParamMarker := false
		position := LimitPosition{}

		//FIXME: 目前是通过 (*driver.ParamMarkerExpr) 来获取Offset的,可能不稳定
		if limitNode.Offset != nil {
			if offsetParamMarker, ok := limitNode.Offset.(*driver.ParamMarkerExpr); ok {
				hasParamMarker = true
				position.HasOffset = true
				position.Offset = utf8.RuneCountInString(l.sql[:offsetParamMarker.Offset])
			}
		}
		if limitNode.Count != nil {
			if countParamMarker, ok := limitNode.Count.(*driver.ParamMarkerExpr); ok {
				hasParamMarker = true
				position.Count = utf8.RuneCountInString(l.sql[:countParamMarker.Offset])
			}
		}
		if hasParamMarker {
			l.position = append(l.position, position)
		}
	}
	return n, true
}

type StatementInfo struct {
	Type            StatementType    `json:"type"`
	InExprPositions []InExprPosition `json:"inExprPositions"`
	LimitPositions  []LimitPosition  `json:"limitPositions"`
}

func (d *DatabaseAPI) GetStatementInfo(sql string) (*StatementInfo, error) {
	//新建parser
	p := parser.New()
	//解析语句
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, errors.Wrap(err, "SQL解析错误")
	}

	//获取语句类型
	statementType := OtherStatementType
	switch stmt.(type) {
	case *ast.SelectStmt:
		statementType = SelectStatementType
	case *ast.SetOprStmt:
		statementType = SelectStatementType
	case *ast.InsertStmt:
		statementType = InsertStatementType
	case *ast.UpdateStmt:
		statementType = UpdateStatementType
	case *ast.DeleteStmt:
		statementType = DeleteStatementType
	default:
		statementType = OtherStatementType
	}

	//遍历/获取所有的 in (?) 表达式
	inExprVisitor := newInExprNodeVisitor(sql)
	stmt.Accept(inExprVisitor)

	//遍历获取所有的 limit ?[,?] 表达式
	limitVisitor := newLimitNodeVisitor(sql)
	stmt.Accept(limitVisitor)

	return &StatementInfo{
		Type:            statementType,
		InExprPositions: inExprVisitor.position,
		LimitPositions:  limitVisitor.position,
	}, nil
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
	Name     string   `json:"name"`
	DBType   string   `json:"dbType"`
	Length   int      `json:"length"`
	Unsigned bool     `json:"unsigned"`
	Binary   bool     `json:"binary"`
	Decimal  int      `json:"decimal"`
	Elements []string `json:"elements"`
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
				DBType:   types.TypeToStr(col.RetType.Tp, col.RetType.Charset),
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
