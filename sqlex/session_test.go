package main

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func generateTestData(ctx context.Context, t *testing.T) *Database {
	a := assert.New(t)

	database, err := NewMemoryDatabase()
	a.NoError(err)

	//定义数据
	session, err := database.CreateSessionOnDatabase(context.Background(), "sqlex")
	a.NoError(err)
	defer session.Close()

	err = session.ExecuteScript(ctx, `
			create table person(
		    	id integer auto_increment primary key,
		    	name varchar(255) not null,
				departmentId int not null,
				foreign key (departmentId) references department(id)
			);

			create table department(
		    	id int auto_increment primary key,
		    	name varchar(255) not null
			);
	`)
	a.NoError(err)
	return database
}

func TestSessionBasic(t *testing.T) {
	a := assert.New(t)

	database := generateTestData(context.Background(), t)

	//分析计划
	session, err := database.CreateSessionOnDatabase(context.Background(), "sqlex")
	a.NoError(err)
	defer session.Close()
	plan, err := session.GetPlan(context.Background(), "select * from department")
	a.NoError(err)
	fmt.Println(plan)
}

func TestSession_GetAllTables(t *testing.T) {
	a := assert.New(t)

	database := generateTestData(context.Background(), t)

	session, err := database.CreateSessionOnDatabase(context.Background(), "sqlex")
	a.NoError(err)
	//获取所有的表
	tables, err := session.GetAllTables()
	a.NoError(err)

	a.Equal([]string{"department", "person"}, tables)
}

func TestSession_GetTableDDL(t *testing.T) {
	a := assert.New(t)

	database := generateTestData(context.Background(), t)

	session, err := database.CreateSessionOnDatabase(context.Background(), "sqlex")
	a.NoError(err)

	tables, err := session.GetAllTables()
	a.NoError(err)

	for _, table := range tables {
		ddl, err := session.GetTableDDL(context.Background(), table)
		a.NoError(err)

		fmt.Println(ddl)
	}
}

func TestSession_GetPlan(t *testing.T) {
	a := assert.New(t)

	database := generateTestData(context.Background(), t)

	session, err := database.CreateSessionOnDatabase(context.Background(), "sqlex")
	a.NoError(err)

	_, err = session.GetPlan(context.Background(), "select * from person")
	a.NoError(err)

	_, err = session.GetPlan(context.Background(), "select * from person where id=?")
	a.NoError(err)

	_, err = session.GetPlan(context.Background(), "insert into person values(?,?,?)")
	a.NoError(err)

	_, err = session.GetPlan(context.Background(), "insert into person values(?,?)")
	a.Error(err)

	_, err = session.GetPlan(context.Background(), "delete from person where id=?")
	a.NoError(err)

	_, err = session.GetPlan(context.Background(), "update person set name=? where id=?")
	a.NoError(err)
}
