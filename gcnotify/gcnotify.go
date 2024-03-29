// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package gcnotify

import (
	"github.com/CAFxX/gcnotifier"
	pilosa "github.com/featurebasedb/featurebase/v3"
)

// Ensure ActiveGCNotifier implements interface.
var _ pilosa.GCNotifier = &activeGCNotifier{}

type activeGCNotifier struct {
	gcn *gcnotifier.GCNotifier
}

// NewActiveGCNotifier creates an active GCNotifier.
func NewActiveGCNotifier() *activeGCNotifier {
	return &activeGCNotifier{
		gcn: gcnotifier.New(),
	}
}

// Close implements the GCNotifier interface.
func (n *activeGCNotifier) Close() {
	n.gcn.Close()
}

// AfterGC implements the GCNotifier interface.
func (n *activeGCNotifier) AfterGC() <-chan struct{} {
	return n.gcn.AfterGC()
}
