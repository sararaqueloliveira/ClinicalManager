package models2

import "github.com/golang/glog"

func debug(format string, a ...interface{}) {
	glog.V(20).Infof(format, a...)
}
