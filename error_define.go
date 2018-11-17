// 错误定义

package apibus

import (
	"git.quyun.com/apibox/api"
)

// error type
const (
	errorWrongModulePath = iota
	errorModuleStartFailed
)

var ErrorDefines = map[api.ErrorType]*api.ErrorDefine{
	errorWrongModulePath: api.NewErrorDefine(
		"WrongModulePath",
		[]int{0},
		map[string]map[int]string{
			"en_us": {
				0: "Wrong module path!",
			},
			"zh_cn": {
				0: "模块路径错误！",
			},
		},
	),
	errorModuleStartFailed: api.NewErrorDefine(
		"ModuleStartFailed",
		[]int{0},
		map[string]map[int]string{
			"en_us": {
				0: "Module start failed!",
			},
			"zh_cn": {
				0: "模块启动失败！",
			},
		},
	),
}
