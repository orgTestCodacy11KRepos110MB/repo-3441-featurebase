// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
//go:build darwin
// +build darwin

package main

import (
	"syscall"
)

func CTimeNano(stat *syscall.Stat_t) int64 {
	NANOS := int64(1e9) // number of nanosecs in 1 sec
	ts := stat.Ctimespec
	return ts.Sec*NANOS + ts.Nsec
}
