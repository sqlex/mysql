package main

import (
	"encoding/json"
	"fmt"
	"github.com/pingcap/errors"
	"reflect"
)

type Request struct {
	ModuleName string   `json:"moduleName"`
	MethodName string   `json:"methodName"`
	Params     []string `json:"params"`
}

type Response struct {
	Success     bool   `json:"success"`
	Message     string `json:"message"`
	ReturnValue string `json:"returnValue"`
}

type FFIContainer struct {
	modules map[string]interface{} //模块
}

func NewFFIContainer() *FFIContainer {
	return &FFIContainer{
		modules: map[string]interface{}{},
	}
}

func (f *FFIContainer) Inject(module interface{}) error {
	t := reflect.TypeOf(module)
	//检查类型
	if t.Kind() != reflect.Ptr {
		return errors.New("module必须为引用类型(指针)")
	}
	//检查方法
	for i := 0; i < t.NumMethod(); i++ {
		method := t.Method(i).Type
		//判断返回值个数
		if method.NumOut() > 2 {
			//大于2个
			return errors.Errorf("方法[%s]的返回值个数不能超过一个(Error除外)", t.Method(i).Name)
		} else if method.NumOut() == 2 {
			//等于2个,判断后面一个是不是error
			if !isError(method.Out(1)) {
				return errors.Errorf("方法[%s]的返回值个数不能超过一个(Error除外)", t.Method(i).Name)
			}
		}
	}
	//检查通过
	f.modules[t.Elem().Name()] = module
	return nil
}

func (f *FFIContainer) Invoke(req *Request) (resp *Response) {
	//开始构造返回
	resp = &Response{}
	//处理panic
	defer func() {
		err := recover()
		if err != nil {
			resp.Success = false
			resp.Message = fmt.Sprintf("PANIC: %s", err)
		}
	}()
	//检查模块或者方法是否存在
	if req.ModuleName == "" || req.MethodName == "" {
		resp.Success = false
		resp.Message = "模块/方法不能为空"
		return resp
	}
	//开始内层处理
	returnValue, err := func() (interface{}, error) {
		//查找对应的模块
		module, exist := f.modules[req.ModuleName]
		if !exist {
			return nil, errors.New("找不到模块: " + req.ModuleName)
		}
		//查找对应的方法
		method, exist := reflect.TypeOf(module).MethodByName(req.MethodName)
		if !exist {
			return nil, errors.New("找不到方法: " + req.MethodName)
		}
		v := method.Func
		t := method.Type

		//获取参数集合
		params := make([]reflect.Value, t.NumIn())
		//第一个参数为this
		params[0] = reflect.ValueOf(module)

		//挨个反序列和参数
		for index := 1; index < t.NumIn(); index++ {
			paramType := t.In(index)
			var paramValue reflect.Value

			//判断参数是引用还是值,取到正确的类型
			if paramType.Kind() == reflect.Ptr {
				paramValue = reflect.New(paramType.Elem())
			} else {
				paramValue = reflect.New(paramType)
			}

			//判断参数个数不要错误
			paramIndex := index - 1
			if paramIndex >= len(req.Params) {
				return nil, errors.Errorf("错误的参数个数,需要 %d 个,只提供了 %d 个", t.NumIn()-1, len(req.Params))
			}

			//拿到参数对应的二进制数据
			paramJson := req.Params[paramIndex]
			//然后反射出对应的数据
			err := json.Unmarshal([]byte(paramJson), paramValue.Interface())
			if err != nil {
				return nil, errors.Errorf("无法反序列化第 %d 个参数", paramIndex)
			}

			//判断参数是引用还是值
			if paramType.Kind() == reflect.Ptr {
				params[index] = paramValue
			} else {
				params[index] = paramValue.Elem()
			}
		}

		//调用函数,并获取结果
		invokeResults := v.Call(params)

		//返回结果
		if len(invokeResults) == 0 {
			return nil, nil
		} else if len(invokeResults) == 1 {
			//判断是不是一个Error
			if isError(invokeResults[0].Type()) {
				if invokeResults[0].IsNil() {
					return nil, nil
				} else {
					return nil, invokeResults[0].Interface().(error)
				}
			} else {
				return invokeResults[0].Interface(), nil
			}
		} else {
			if invokeResults[1].IsNil() {
				return invokeResults[0].Interface(), nil
			} else {
				return invokeResults[0].Interface(), invokeResults[1].Interface().(error)
			}
		}
	}()
	if err != nil {
		resp.Success = false
		resp.Message = err.Error()
		return resp
	}
	jsonData, err := json.Marshal(returnValue)
	if err != nil {
		resp.Success = false
		resp.Message = fmt.Sprintf("序列化返回值错误: %s", err.Error())
		return resp
	}
	resp.Success = true
	resp.ReturnValue = string(jsonData)
	return resp
}

func (f *FFIContainer) InvokeByData(reqData string) (respData string) {
	//解析请求
	req := &Request{}
	err := json.Unmarshal([]byte(reqData), req)
	if err != nil {
		return ""
	}
	//发起调用
	resp := f.Invoke(req)
	//反序列化
	respJson, err := json.Marshal(resp)
	if err != nil {
		return ""
	}
	return string(respJson)
}

//判断是否为Error
func isError(t reflect.Type) bool {
	//Error方法存不存在
	if m, exist := t.MethodByName("Error"); exist {
		//返回值是不是一个
		if m.Type.NumOut() == 1 {
			//返回值类型是不是String
			if m.Type.Out(0).Kind() == reflect.String {
				//是Error
				return true
			}
		}
	}
	return false
}
