// Copyright 2021 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Package server contains the `pilosa server` subcommand which runs Pilosa
// itself. The purpose of this package is to define an easily tested Command
// object which handles interpreting configuration and setting up all the
// objects that Pilosa needs.

package server

import (
	"os"
	"time"

	"github.com/beevik/ntp"
	"github.com/pilosa/pilosa/v2"
)

func trialVersion(logger loggerLogger) {
	if pilosa.TrialDeadline != "" {
		startTime, err := ntpServerTime(4, logger)
		if err != nil {
			logger.Printf("reading ntp server time %v", err)
			os.Exit(1)
		}
		endTime, err := time.Parse("2006-01-02", pilosa.TrialDeadline)
		if err != nil {
			logger.Printf("parsing trial deadline: %v", err)
			os.Exit(1)
		}
		maxDuration := endTime.Sub(startTime)
		go expireAfter(maxDuration, logger)
		go dailyCheck(maxDuration, logger)
	}
}

const trialCheckInterval = 24 * time.Hour

// dailyCheck runs in the background while a trial version of Molecula is being run, displaying daily reminders of the remaining days
func dailyCheck(maxDuration time.Duration, logger loggerLogger) {
	startTime := time.Now() // we get a new start time here to ensure that it has a monotonic clock
	remaining := maxDuration - time.Since(startTime)
	logger.Printf("Current time remaining in trial: %d days %v", remaining/(time.Hour*24), remaining%(time.Hour*24))
	ticker := time.NewTicker(trialCheckInterval)
	for range ticker.C {
		remaining := maxDuration - time.Since(startTime)
		logger.Printf("Current time remaining in trial: %d days %v", remaining/(time.Hour*24), remaining%(time.Hour*24))
	}
}

const ntpURL = "0.beevik-ntp.pool.ntp.org"
const ntpRetryDelay = 100 * time.Millisecond

// ntpServerTime attempts to reach ntp servers with delays between each attempt, returning the time value of the first connected server
func ntpServerTime(retries int, logger loggerLogger) (time.Time, error) {
	t, err := ntp.Time(ntpURL)
	if err != nil && retries <= 0 {
		return t, err
	}
	if err != nil {
		time.Sleep(ntpRetryDelay)
		return ntpServerTime(retries-1, logger)
	}
	return t, nil
}

func expireAfter(maxDuration time.Duration, logger loggerLogger) {
	time.Sleep(maxDuration)
	logger.Printf("Trial edition of Molecula has expired, exiting now!")
	os.Exit(1)
}
