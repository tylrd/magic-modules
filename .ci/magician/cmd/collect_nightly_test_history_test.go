/*
* Copyright 2026 Google LLC. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */
package cmd

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"magician/provider"
	utils "magician/utility"

	"github.com/stretchr/testify/assert"
)

// writeDailyStatus stores a daily test-metadata file in the fake bucket for the given day.
func writeDailyStatus(t *testing.T, gcs *fakeGCS, pVersion provider.Version, day string, tests []TestInfo) {
	t.Helper()
	data, err := json.Marshal(tests)
	if err != nil {
		t.Fatal(err)
	}
	object := fmt.Sprintf("test-metadata/%s/%s-%s.json", pVersion.String(), day, pVersion.String())
	gcs.objects[nightlyDataBucket+"/"+object] = data
}

// readHistory reads back the history the command uploaded to the fake bucket.
func readHistory(t *testing.T, gcs *fakeGCS, pVersion provider.Version) NightlyTestHistoryReport {
	t.Helper()
	data, ok := gcs.objects[nightlyDataBucket+"/"+nightlyTestHistoryObjectName(pVersion)]
	if !ok {
		t.Fatal("history was not uploaded")
	}
	var report NightlyTestHistoryReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	return report
}

func TestCreateNightlyTestHistoryCapturesFailureEvidence(t *testing.T) {
	loc, err := time.LoadLocation("America/Los_Angeles")
	assert.NoError(t, err)

	gcs := &fakeGCS{objects: map[string][]byte{}}
	// 3 days: the test fails, passes, then fails again.
	writeDailyStatus(t, gcs, provider.Beta, "2026-09-26", []TestInfo{
		{Name: "TestAccA", Status: "FAILURE", Service: "pubsub", TestNameId: "111", LogLink: "https://logs/26.txt"},
	})
	writeDailyStatus(t, gcs, provider.Beta, "2026-09-27", []TestInfo{
		{Name: "TestAccA", Status: "SUCCESS", Service: "pubsub", TestNameId: "111"},
	})
	writeDailyStatus(t, gcs, provider.Beta, "2026-09-28", []TestInfo{
		{Name: "TestAccA", Status: "FAILURE", Service: "pubsub", TestNameId: "111", LogLink: "https://logs/28.txt"},
	})

	err = createNightlyTestHistory(provider.Beta, nil, gcs, loc, "2026-09-28", 3)
	assert.NoError(t, err)

	report := readHistory(t, gcs, provider.Beta)
	assert.Equal(t, 3, report.DaysFound)

	h := report.Tests["TestAccA"]
	if assert.NotNil(t, h) {
		assert.Equal(t, 1, h.Passes)
		assert.Equal(t, 2, h.Failures)
		assert.Equal(t, "FAILURE", h.LastStatus)
		assert.Equal(t, "111", h.TestNameId)
		// Failures are recorded oldest to newest, each with the log of that night's run.
		assert.Equal(t, []NightlyTestRun{
			{Date: "2026-09-26", LogLink: "https://logs/26.txt"},
			{Date: "2026-09-28", LogLink: "https://logs/28.txt"},
		}, h.FailureRuns)
	}
}

func TestCreateNightlyTestHistoryCapsFailureRuns(t *testing.T) {
	loc, err := time.LoadLocation("America/Los_Angeles")
	assert.NoError(t, err)

	days := maxNightlyFailureRuns + 5
	gcs := &fakeGCS{objects: map[string][]byte{}}
	end := time.Date(2026, 9, 28, 0, 0, 0, 0, loc)
	for i := days - 1; i >= 0; i-- {
		day := end.AddDate(0, 0, -i).Format("2006-01-02")
		writeDailyStatus(t, gcs, provider.Beta, day, []TestInfo{
			{Name: "TestAccA", Status: "FAILURE", Service: "pubsub", LogLink: "https://logs/" + day},
		})
	}

	err = createNightlyTestHistory(provider.Beta, nil, gcs, loc, "2026-09-28", days)
	assert.NoError(t, err)

	h := readHistory(t, gcs, provider.Beta).Tests["TestAccA"]
	if assert.NotNil(t, h) {
		// Every failure is counted, but only the most recent runs are retained.
		assert.Equal(t, days, h.Failures)
		assert.Len(t, h.FailureRuns, maxNightlyFailureRuns)
		assert.Equal(t, "2026-09-28", h.FailureRuns[len(h.FailureRuns)-1].Date)
		assert.Equal(t, "2026-09-28", h.LastFailureDate)
	}
}

// LastFailureDate must survive FailureRuns being trimmed, since consumers use it to tell a test
// fixed mid-window from a genuinely flakey one.
func TestCreateNightlyTestHistoryKeepsLastFailureDateAfterFix(t *testing.T) {
	loc, err := time.LoadLocation("America/Los_Angeles")
	assert.NoError(t, err)

	gcs := &fakeGCS{objects: map[string][]byte{}}
	end := time.Date(2026, 9, 28, 0, 0, 0, 0, loc)
	const days = 10
	for i := days - 1; i >= 0; i-- {
		day := end.AddDate(0, 0, -i).Format("2006-01-02")
		status := "FAILURE"
		// Fixed partway through: green for the last four nights.
		if i < 4 {
			status = "SUCCESS"
		}
		writeDailyStatus(t, gcs, provider.Beta, day, []TestInfo{
			{Name: "TestAccA", Status: status, Service: "pubsub"},
		})
	}

	err = createNightlyTestHistory(provider.Beta, nil, gcs, loc, "2026-09-28", days)
	assert.NoError(t, err)

	report := readHistory(t, gcs, provider.Beta)
	h := report.Tests["TestAccA"]
	if assert.NotNil(t, h) {
		assert.Equal(t, "SUCCESS", h.LastStatus)
		assert.Equal(t, "2026-09-24", h.LastFailureDate)
		// The consumer should read this as fixed rather than flakey.
		assert.Equal(t, NightlyStatusRecentlyFixed, classifyNightlyStatus(h, report.EndDate))
	}
}

// Histories collected before test ids were captured must still aggregate cleanly.
func TestCreateNightlyTestHistoryWithoutTestNameId(t *testing.T) {
	loc, err := time.LoadLocation("America/Los_Angeles")
	assert.NoError(t, err)

	gcs := &fakeGCS{objects: map[string][]byte{}}
	writeDailyStatus(t, gcs, provider.Beta, "2026-09-28", []TestInfo{
		{Name: "TestAccA", Status: "SUCCESS", Service: "pubsub"},
	})

	err = createNightlyTestHistory(provider.Beta, nil, gcs, loc, "2026-09-28", 1)
	assert.NoError(t, err)

	h := readHistory(t, gcs, provider.Beta).Tests["TestAccA"]
	if assert.NotNil(t, h) {
		assert.Equal(t, "", h.TestNameId)
		assert.Empty(t, h.FailureRuns)
	}
}

// The daily test-metadata file must round-trip the TeamCity test id.
func TestTestInfoTestNameIdRoundTrip(t *testing.T) {
	in := []TestInfo{{Name: "TestAccA", Status: "FAILURE", TestNameId: "6946391424746324317"}}
	path := t.TempDir() + "/daily.json"
	assert.NoError(t, utils.WriteToJson(in, path))

	var out []TestInfo
	assert.NoError(t, utils.ReadFromJson(&out, path))
	assert.Equal(t, "6946391424746324317", out[0].TestNameId)
}
