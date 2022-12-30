package main

// #include <jni.h>
// #include <stdlib.h>
//
// static inline jstring NewString(JNIEnv * env, jchar * unicode, jsize len) {
//     return (*env)->NewString(env, unicode, len);
// }
//
// static inline jsize GetStringLength(JNIEnv * env, jstring str) {
//     return (*env)->GetStringLength(env, str);
// }
//
// static inline jsize GetStringUTFLength(JNIEnv * env, jstring str) {
//     return (*env)->GetStringUTFLength(env, str);
// }
//
// static inline void GetStringUTFRegion(JNIEnv * env, jstring str, jsize start, jsize len, char * buf) {
//     (*env)->GetStringUTFRegion(env, str, start, len, buf);
// }
import "C"
import (
	"unicode/utf16"
	"unsafe"
)

type Jstring = uintptr

func cmem(b []byte) *C.char {
	return (*C.char)(unsafe.Pointer(*(*uintptr)(unsafe.Pointer(&b))))
}

type Env uintptr

func (env Env) NewString(s string) Jstring {
	codes := utf16.Encode([]rune(s))
	size := len(codes)
	if size <= 0 {
		return 0
	} else {
		return Jstring(C.NewString((*C.JNIEnv)(unsafe.Pointer(env)), (*C.jchar)(unsafe.Pointer(&codes[0])), C.jsize(size)))
	}
}

func (env Env) GetStringUTF(ptr Jstring) []byte {
	jstr := C.jstring(ptr)
	size := C.GetStringUTFLength((*C.JNIEnv)(unsafe.Pointer(env)), jstr)
	ret := make([]byte, int(size))
	C.GetStringUTFRegion((*C.JNIEnv)(unsafe.Pointer(env)), jstr, C.jsize(0), C.GetStringLength((*C.JNIEnv)(unsafe.Pointer(env)), jstr), cmem(ret))
	return ret
}

//export Java_me_danwi_sqlex_parser_ffi_NativeFFI_request
func Java_me_danwi_sqlex_parser_ffi_NativeFFI_request(env uintptr, clazz uintptr, req uintptr) uintptr {
	reqData := Env(env).GetStringUTF(req)
	respData := container.InvokeByData(string(reqData))
	return Env(env).NewString(respData)
}
