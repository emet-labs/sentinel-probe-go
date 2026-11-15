package conformance

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/emet-labs/sentinel-probe-go/emission"
	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
	int128codec "github.com/emet-labs/sentinel-probe-go/int128"
	"github.com/emet-labs/sentinel-probe-go/otlp"
	"google.golang.org/protobuf/proto"
)

// The shared sequence_completeness suite (#461), run through the real
// allocator, heartbeat and partial-success decoder. The suite is found
// through manifest-v1.json.

type sequenceSuite struct {
	FormatVersion string            `json:"format_version"`
	Kind          string            `json:"kind"`
	Cases         []json.RawMessage `json:"cases"`
}

type sequenceStart struct {
	ClockUnixMS   string  `json:"clock_unix_ms"`
	Epoch         *string `json:"epoch"`
	PreviousEpoch *string `json:"previous_epoch"`
}

type sequenceSignal struct {
	SequenceEpoch           string `json:"sequence_epoch"`
	LastEmitted             string `json:"last_emitted"`
	CompleteThrough         string `json:"complete_through"`
	AcknowledgedFilterEpoch string `json:"acknowledged_filter_epoch"`
	Activity                string `json:"activity"`
	ReportedAtNS            string `json:"reported_at_ns"`
	ActivitySinceNS         string `json:"activity_since_ns"`
	RestatesPrevious        bool   `json:"restates_previous"`
}

type sequenceRange struct {
	First       string `json:"first"`
	Last        string `json:"last"`
	FilterEpoch string `json:"filter_epoch"`
}

type sequenceStep struct {
	Op     string          `json:"op"`
	Expect json.RawMessage `json:"expect"`
	// allocate, export_ok, export_fail, filter_drop, set_filter_epoch
	Epoch       *string `json:"epoch"`
	Number      string  `json:"number"`
	FilterEpoch string  `json:"filter_epoch"`
	// clock
	UnixMS string `json:"unix_ms"`
	// tick, shutdown
	Send        []string        `json:"send"`
	Filtered    []sequenceRange `json:"filtered"`
	ExpectError string          `json:"expect_error"`
	// restart
	ClockUnixMS   string          `json:"clock_unix_ms"`
	PreviousEpoch json.RawMessage `json:"previous_epoch"`
}

type sequenceCase struct {
	ID    string         `json:"id"`
	Start *sequenceStart `json:"start"`
	Steps []sequenceStep `json:"steps"`
	Hex   *string        `json:"response_hex"`
	Count *string        `json:"rejected_spans"`
	Bad   *bool          `json:"undecodable"`
}

func loadSequenceSuite(t *testing.T) sequenceSuite {
	t.Helper()
	var manifest struct {
		FormatVersion string `json:"format_version"`
		Suites        []struct {
			Kind string `json:"kind"`
			Path string `json:"path"`
		} `json:"suites"`
		Malformed []json.RawMessage `json:"malformed"`
	}
	decodeStrict(t, "manifest-v1.json", &manifest)
	if manifest.FormatVersion != "1.4.0" {
		t.Fatalf("manifest version %s", manifest.FormatVersion)
	}
	var path string
	for _, entry := range manifest.Suites {
		if entry.Kind == "sequence_completeness" {
			if path != "" {
				t.Fatal("two sequence_completeness suites")
			}
			path = entry.Path
		}
	}
	if path == "" {
		t.Fatal("manifest lists no sequence_completeness suite")
	}
	var suite sequenceSuite
	decodeStrict(t, path, &suite)
	if suite.FormatVersion != "1.3.0" || suite.Kind != "sequence_completeness" {
		t.Fatalf("unsupported suite %s %s", suite.FormatVersion, suite.Kind)
	}
	return suite
}

func mustUint(t *testing.T, text string) uint64 {
	t.Helper()
	value, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		t.Fatalf("uint %q: %v", text, err)
	}
	return value
}

func optionalUint(t *testing.T, text *string) *uint64 {
	t.Helper()
	if text == nil {
		return nil
	}
	value := mustUint(t, *text)
	return &value
}

func TestSequenceCompletenessVectors(t *testing.T) {
	suite := loadSequenceSuite(t)
	seen := map[string]bool{}
	scenarios := 0
	for _, raw := range suite.Cases {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		var tc sequenceCase
		if err := decoder.Decode(&tc); err != nil {
			t.Fatalf("case: %v", err)
		}
		if seen[tc.ID] {
			t.Fatalf("duplicate id %s", tc.ID)
		}
		seen[tc.ID] = true
		if tc.Start != nil {
			scenarios++
			t.Run("scenario/"+tc.ID, func(t *testing.T) { runSequenceScenario(t, tc) })
			continue
		}
		t.Run("partial_success/"+tc.ID, func(t *testing.T) {
			if tc.Hex == nil {
				t.Fatal("case has neither steps nor response_hex")
			}
			body, err := hex.DecodeString(*tc.Hex)
			if err != nil {
				t.Fatal(err)
			}
			rejected, err := otlp.RejectedSpans(body)
			if tc.Bad != nil {
				if err == nil {
					t.Fatalf("undecodable body decoded to %d", rejected)
				}
				return
			}
			if err != nil || tc.Count == nil || uint64(rejected) != mustUint(t, *tc.Count) {
				t.Fatalf("rejected %d, %v", rejected, err)
			}
		})
	}
	if scenarios == 0 {
		t.Fatal("no scenario executed")
	}
}

type scenarioClock struct {
	mu sync.Mutex
	ms int64
}

func (c *scenarioClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.UnixMilli(c.ms)
}

func runSequenceScenario(t *testing.T, tc sequenceCase) {
	clock := &scenarioClock{ms: int64(mustUint(t, tc.Start.ClockUnixMS))}
	var filterEpoch *uint64
	var outcomes []string
	var sent []*modelv1.ProducerCompletenessSignal
	var errs []error
	send := func(_ context.Context, signal *modelv1.ProducerCompletenessSignal) error {
		sent = append(sent, signal)
		if len(outcomes) == 0 {
			t.Fatalf("%s: an unexpected signal was sent", tc.ID)
		}
		outcome := outcomes[0]
		outcomes = outcomes[1:]
		if outcome == "accepted" {
			return nil
		}
		return errors.New("409 refused")
	}
	pair := func(epoch, previous *uint64) (*emission.SequenceAllocator, *otlp.CompletenessHeartbeat, error) {
		allocator, err := emission.NewSequenceAllocator(emission.SequenceOptions{Now: clock.now, Epoch: epoch, PreviousEpoch: previous})
		if err != nil {
			return nil, nil, err
		}
		heartbeat, err := otlp.NewCompletenessHeartbeat(otlp.HeartbeatConfig{
			Source: allocator,
			FilterEpoch: func() *uint64 {
				return filterEpoch
			},
			Send:    send,
			Now:     clock.now,
			Manual:  true,
			OnError: func(err error) { errs = append(errs, err) },
		})
		return allocator, heartbeat, err
	}
	allocator, heartbeat, err := pair(optionalUint(t, tc.Start.Epoch), optionalUint(t, tc.Start.PreviousEpoch))
	if err != nil {
		t.Fatal(err)
	}
	var previous *modelv1.ProducerCompletenessSignal

	for index, step := range tc.Steps {
		label := fmt.Sprintf("step %d (%s)", index, step.Op)
		switch step.Op {
		case "allocate":
			slot, err := allocator.Allocate()
			if step.ExpectError != "" {
				var sequence *emission.SequenceError
				if !errors.As(err, &sequence) || sequence.Code != step.ExpectError {
					t.Fatalf("%s: err = %v", label, err)
				}
				continue
			}
			var want struct {
				Epoch  string `json:"epoch"`
				Number string `json:"number"`
			}
			if err := json.Unmarshal(step.Expect, &want); err != nil {
				t.Fatal(err)
			}
			if err != nil || slot != (emission.Slot{Epoch: mustUint(t, want.Epoch), Number: mustUint(t, want.Number)}) {
				t.Fatalf("%s: slot %+v, %v; want %+v", label, slot, err, want)
			}
		case "export_ok", "export_fail", "filter_drop":
			slot := emission.Slot{Epoch: mustUint(t, *step.Epoch), Number: mustUint(t, step.Number)}
			switch step.Op {
			case "export_ok":
				allocator.MarkExported(slot)
			case "export_fail":
				allocator.MarkFailed(slot)
			default:
				allocator.MarkFiltered(slot, mustUint(t, step.FilterEpoch))
			}
		case "set_filter_epoch":
			filterEpoch = optionalUint(t, step.Epoch)
		case "clock":
			clock.mu.Lock()
			clock.ms = int64(mustUint(t, step.UnixMS))
			clock.mu.Unlock()
		case "idle":
			heartbeat.DeclareIdle()
		case "active":
			heartbeat.DeclareActive()
		case "tick", "shutdown":
			outcomes = append([]string(nil), step.Send...)
			sent, errs = nil, nil
			var reports []otlp.CompletenessReport
			if step.Op == "tick" {
				reports = heartbeat.Report(context.Background())
			} else {
				reports = heartbeat.Shutdown(context.Background())
			}
			var want []sequenceSignal
			if err := json.Unmarshal(step.Expect, &want); err != nil {
				t.Fatal(err)
			}
			if len(sent) != len(want) || len(reports) != len(sent) {
				t.Fatalf("%s: sent %d signals, want %d", label, len(sent), len(want))
			}
			for i, expected := range want {
				if reports[i].Accepted != (step.Send[i] == "accepted") || reports[i].Signal != sent[i] {
					t.Fatalf("%s: report %d = %+v", label, i, reports[i])
				}
				expectSequenceSignal(t, fmt.Sprintf("%s signal %d", label, i), sent[i], expected)
				if got := restatesPrevious(t, sent[i], previous); got != expected.RestatesPrevious {
					t.Fatalf("%s signal %d: restates_previous = %v", label, i, got)
				}
				previous = sent[i]
			}
			got := allocator.Declaration().Filtered
			if len(got) != len(step.Filtered) {
				t.Fatalf("%s: filtered %+v, want %+v", label, got, step.Filtered)
			}
			for i, expected := range step.Filtered {
				if got[i] != (otlp.FilteredRange{First: mustUint(t, expected.First), Last: mustUint(t, expected.Last), FilterEpoch: mustUint(t, expected.FilterEpoch)}) {
					t.Fatalf("%s: filtered %+v, want %+v", label, got, step.Filtered)
				}
			}
			regressed := false
			for _, err := range errs {
				regressed = regressed || otlp.IsHeartbeatError(err, otlp.HeartbeatFilterEpochRegressed)
			}
			if regressed != (step.ExpectError == otlp.HeartbeatFilterEpochRegressed) {
				t.Fatalf("%s: errors %v, expect_error %q", label, errs, step.ExpectError)
			}
		case "restart":
			clock.mu.Lock()
			clock.ms = int64(mustUint(t, step.ClockUnixMS))
			clock.mu.Unlock()
			var previousEpoch *uint64
			var text *string
			if err := json.Unmarshal(step.PreviousEpoch, &text); err != nil {
				t.Fatal(err)
			}
			if text != nil && *text == "inherit" {
				epoch := allocator.Epoch()
				previousEpoch = &epoch
			} else {
				previousEpoch = optionalUint(t, text)
			}
			nextAllocator, nextHeartbeat, err := pair(optionalUint(t, step.Epoch), previousEpoch)
			if step.ExpectError != "" {
				var sequence *emission.SequenceError
				if !errors.As(err, &sequence) || sequence.Code != step.ExpectError {
					t.Fatalf("%s: err = %v", label, err)
				}
				continue
			}
			if err != nil {
				t.Fatalf("%s: %v", label, err)
			}
			allocator, heartbeat = nextAllocator, nextHeartbeat
		default:
			t.Fatalf("%s: unknown op", label)
		}
	}
}

func expectSequenceSignal(t *testing.T, label string, sent *modelv1.ProducerCompletenessSignal, want sequenceSignal) {
	t.Helper()
	encoded, err := proto.Marshal(sent)
	if err != nil {
		t.Fatal(err)
	}
	var decoded modelv1.ProducerCompletenessSignal
	if err := proto.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	// Presence must survive the wire, including zero.
	if decoded.LastEmitted == nil || decoded.CompleteThrough == nil || decoded.AcknowledgedFilterEpoch == nil {
		t.Fatalf("%s: optional field absent: %v", label, &decoded)
	}
	activity := modelv1.SourceActivityState_SOURCE_ACTIVITY_STATE_ACTIVE
	if want.Activity == "idle" {
		activity = modelv1.SourceActivityState_SOURCE_ACTIVITY_STATE_INTENTIONALLY_IDLE
	}
	if decoded.GetSequenceEpoch() != mustUint(t, want.SequenceEpoch) ||
		decoded.GetLastEmitted() != mustUint(t, want.LastEmitted) ||
		decoded.GetCompleteThrough() != mustUint(t, want.CompleteThrough) ||
		decoded.GetAcknowledgedFilterEpoch() != mustUint(t, want.AcknowledgedFilterEpoch) ||
		decoded.GetActivity() != activity {
		t.Fatalf("%s: signal %v, want %+v", label, &decoded, want)
	}
	for _, check := range []struct {
		time *modelv1.OccurrenceTime
		want string
	}{{decoded.GetProducerReportedAt(), want.ReportedAtNS}, {decoded.GetActivitySince(), want.ActivitySinceNS}} {
		if check.time.GetClockDomainId() != "unix" || check.time.GetUncertaintyNanoseconds() != 0 {
			t.Fatalf("%s: time %v", label, check.time)
		}
		if got := int128codec.ToBigInt(check.time.GetNanoseconds()).String(); got != check.want {
			t.Fatalf("%s: nanoseconds %s, want %s", label, got, check.want)
		}
	}
}

// restatesPrevious re-encodes signal with the earlier signal's report
// instant and compares bytes.
func restatesPrevious(t *testing.T, signal, earlier *modelv1.ProducerCompletenessSignal) bool {
	t.Helper()
	if earlier == nil {
		return false
	}
	normalized, ok := proto.Clone(signal).(*modelv1.ProducerCompletenessSignal)
	if !ok {
		t.Fatal("clone")
	}
	normalized.ProducerReportedAt.Nanoseconds, ok = proto.Clone(earlier.GetProducerReportedAt().GetNanoseconds()).(*modelv1.Int128)
	if !ok {
		t.Fatal("clone")
	}
	deterministic := proto.MarshalOptions{Deterministic: true}
	left, err := deterministic.Marshal(normalized)
	if err != nil {
		t.Fatal(err)
	}
	right, err := deterministic.Marshal(earlier)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Equal(left, right)
}
