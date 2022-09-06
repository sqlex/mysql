package main

import (
	"encoding/json"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

type Test struct {
	name string
}

func (t *Test) GetName() string {
	return t.name
}

func (t *Test) GetError() error {
	return errors.New("error")
}

func (t *Test) GetNilError() error {
	return nil
}

func (t *Test) GetHello(hello string) (string, error) {
	return hello + t.name, nil
}

type Test2 struct {
}

func (t *Test2) Add(a, b int) int {
	return a + b
}

func TestFFIContainer(t *testing.T) {
	a := assert.New(t)

	ffi := NewFFIContainer()

	a.NoError(ffi.Inject(&Test{name: "sqlex"}))
	a.NoError(ffi.Inject(&Test2{}))

	resp := ffi.Invoke(&Request{
		ModuleName: "Test",
		MethodName: "GetName",
	})
	a.Equal(true, resp.Success)
	a.Equal("", resp.Message)
	a.Equal("sqlex", resp.ReturnValue)

	resp = ffi.Invoke(&Request{
		ModuleName: "Test",
		MethodName: "GetError",
	})
	a.Equal(false, resp.Success)
	a.Equal("error", resp.Message)
	a.Equal(nil, resp.ReturnValue)

	resp = ffi.Invoke(&Request{
		ModuleName: "Test",
		MethodName: "GetNilError",
	})
	a.Equal(true, resp.Success)
	a.Equal("", resp.Message)
	a.Equal(nil, resp.ReturnValue)

	resp = ffi.Invoke(&Request{
		ModuleName: "Test",
		MethodName: "GetHello",
		Parameters: []json.RawMessage{[]byte(`"你好"`)},
	})
	a.Equal(true, resp.Success)
	a.Equal("", resp.Message)
	a.Equal("你好sqlex", resp.ReturnValue)
}
