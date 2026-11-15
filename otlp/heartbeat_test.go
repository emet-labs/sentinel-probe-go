package otlp_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/emet-labs/sentinel-probe-go/emission"
	modelv1 "github.com/emet-labs/sentinel-probe-go/gen/sentinel/model/v1"
	"github.com/emet-labs/sentinel-probe-go/otlp"
	"google.golang.org/protobuf/proto"
)

const heartbeatEpoch uint64 = 1_794_000_000_000

type heartbeatClock struct {
	mu sync.Mutex
	ms int64
}

func (c *heartbeatClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.UnixMilli(c.ms)
}

// gatedSend records each signal and blocks until the test releases it.
type gatedSend struct {
	mu      sync.Mutex
	sent    []*modelv1.ProducerCompletenessSignal
	arrived chan struct{}
	release chan error
}

func newGatedSend() *gatedSend {
	return &gatedSend{arrived: make(chan struct{}, 16), release: make(chan error, 16)}
}

func (g *gatedSend) send(_ context.Context, signal *modelv1.ProducerCompletenessSignal) error {
	g.mu.Lock()
	g.sent = append(g.sent, signal)
	g.mu.Unlock()
	g.arrived <- struct{}{}
	return <-g.release
}

func (g *gatedSend) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.sent)
}

func newHeartbeatFixture(t *testing.T, config otlp.HeartbeatConfig) (*emission.SequenceAllocator, *otlp.CompletenessHeartbeat) {
	t.Helper()
	clock := &heartbeatClock{ms: int64(heartbeatEpoch)}
	allocator, err := emission.NewSequenceAllocator(emission.SequenceOptions{Now: clock.now})
	if err != nil {
		t.Fatal(err)
	}
	config.Source = allocator
	config.Now = clock.now
	if config.Interval == 0 {
		config.Manual = true
	}
	heartbeat, err := otlp.NewCompletenessHeartbeat(config)
	if err != nil {
		t.Fatal(err)
	}
	return allocator, heartbeat
}

func TestHeartbeatCoalescesReportsDuringASend(t *testing.T) {
	gate := newGatedSend()
	allocator, heartbeat := newHeartbeatFixture(t, otlp.HeartbeatConfig{Send: gate.send})
	one, _ := allocator.Allocate()
	first := make(chan []otlp.CompletenessReport)
	go func() { first <- heartbeat.Report(context.Background()) }()
	<-gate.arrived
	allocator.MarkExported(one)
	var joined sync.WaitGroup
	followUps := make([][]otlp.CompletenessReport, 2)
	for i := range followUps {
		joined.Add(1)
		go func() {
			defer joined.Done()
			followUps[i] = heartbeat.Report(context.Background())
		}()
	}
	// Both callers are waiting on one follow-up; nothing else is sent yet.
	waitForWaiters(heartbeat, 2)
	if gate.count() != 1 {
		t.Fatalf("sent %d before the first send finished", gate.count())
	}
	gate.release <- nil
	if reports := <-first; len(reports) != 1 || reports[0].CompleteThrough != 0 {
		t.Fatalf("first = %+v", reports)
	}
	<-gate.arrived
	gate.release <- nil
	joined.Wait()
	for _, reports := range followUps {
		if len(reports) != 1 || reports[0].CompleteThrough != 1 {
			t.Fatalf("follow-up = %+v", reports)
		}
	}
	if gate.count() != 2 {
		t.Fatalf("sent %d, want exactly one follow-up", gate.count())
	}
}

// waitForWaiters yields until n callers wait on the queued report. It arms
// no timer.
func waitForWaiters(heartbeat *otlp.CompletenessHeartbeat, n int) {
	for otlp.QueuedReportWaiters(heartbeat) < n {
		runtime.Gosched()
	}
}

func TestHeartbeatStopSendsNothing(t *testing.T) {
	sent := 0
	ticks := make(chan time.Time)
	stopped := make(chan struct{})
	_, heartbeat := newHeartbeatFixture(t, otlp.HeartbeatConfig{
		Send:     func(context.Context, *modelv1.ProducerCompletenessSignal) error { sent++; return nil },
		Interval: 15 * time.Second,
		Ticks: func(time.Duration) (<-chan time.Time, func()) {
			return ticks, func() { close(stopped) }
		},
	})
	done := make(chan error)
	go func() { done <- heartbeat.Run(context.Background()) }()
	heartbeat.Stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	<-stopped
	if reports := heartbeat.Report(context.Background()); reports != nil {
		t.Fatalf("report after stop = %+v", reports)
	}
	if reports := heartbeat.Shutdown(context.Background()); reports != nil {
		t.Fatalf("shutdown after stop = %+v", reports)
	}
	if sent != 0 {
		t.Fatalf("sent %d", sent)
	}
}

func TestHeartbeatShutdownSendsOneFinalReportAfterARunningOne(t *testing.T) {
	gate := newGatedSend()
	allocator, heartbeat := newHeartbeatFixture(t, otlp.HeartbeatConfig{Send: gate.send})
	one, _ := allocator.Allocate()
	running := make(chan []otlp.CompletenessReport)
	go func() { running <- heartbeat.Report(context.Background()) }()
	<-gate.arrived
	allocator.MarkExported(one)
	final := make(chan []otlp.CompletenessReport)
	go func() { final <- heartbeat.Shutdown(context.Background()) }()
	waitForWaiters(heartbeat, 1)
	gate.release <- nil
	<-running
	<-gate.arrived
	gate.release <- nil
	reports := <-final
	if len(reports) != 1 || reports[0].LastEmitted != 1 || reports[0].CompleteThrough != 1 {
		t.Fatalf("final = %+v", reports)
	}
	if again := heartbeat.Report(context.Background()); again != nil || gate.count() != 2 {
		t.Fatalf("after shutdown: %+v, sent %d", again, gate.count())
	}
}

func TestHeartbeatRunReportsOncePerTickAndNeverAfterStop(t *testing.T) {
	var mu sync.Mutex
	sent := 0
	reported := make(chan struct{}, 4)
	ticks := make(chan time.Time)
	var interval time.Duration
	allocator, heartbeat := newHeartbeatFixture(t, otlp.HeartbeatConfig{
		Send: func(context.Context, *modelv1.ProducerCompletenessSignal) error {
			mu.Lock()
			sent++
			mu.Unlock()
			reported <- struct{}{}
			return nil
		},
		Interval: otlp.DefaultHeartbeatInterval,
		Ticks: func(every time.Duration) (<-chan time.Time, func()) {
			interval = every
			return ticks, func() {}
		},
	})
	if _, err := allocator.Allocate(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- heartbeat.Run(ctx) }()
	ticks <- time.Time{}
	<-reported
	ticks <- time.Time{}
	<-reported
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
	if interval != 15*time.Second {
		t.Fatalf("interval = %v", interval)
	}
	mu.Lock()
	defer mu.Unlock()
	if sent != 2 {
		t.Fatalf("sent %d", sent)
	}
}

func TestHeartbeatValidatesItsConfig(t *testing.T) {
	allocator, err := emission.NewSequenceAllocator(emission.SequenceOptions{Now: (&heartbeatClock{}).now})
	if err != nil {
		t.Fatal(err)
	}
	send := func(context.Context, *modelv1.ProducerCompletenessSignal) error { return nil }
	for _, interval := range []time.Duration{999 * time.Millisecond, time.Minute, -time.Second} {
		if _, err := otlp.NewCompletenessHeartbeat(otlp.HeartbeatConfig{Source: allocator, Send: send, Interval: interval}); err == nil {
			t.Fatalf("interval %v accepted", interval)
		}
	}
	for _, interval := range []time.Duration{0, time.Second, 59999 * time.Millisecond} {
		if _, err := otlp.NewCompletenessHeartbeat(otlp.HeartbeatConfig{Source: allocator, Send: send, Interval: interval}); err != nil {
			t.Fatalf("interval %v: %v", interval, err)
		}
	}
	if _, err := otlp.NewCompletenessHeartbeat(otlp.HeartbeatConfig{Source: allocator}); err == nil {
		t.Fatal("no Send and no SourceHandle accepted")
	}
	if _, err := otlp.NewCompletenessHeartbeat(otlp.HeartbeatConfig{Send: send}); err == nil {
		t.Fatal("no Source accepted")
	}
}

func TestHeartbeatReportsARefusedSendThroughOnError(t *testing.T) {
	var errs []error
	refused := errors.New("409 ReopenedClosedEpoch")
	allocator, heartbeat := newHeartbeatFixture(t, otlp.HeartbeatConfig{
		Send:    func(context.Context, *modelv1.ProducerCompletenessSignal) error { return refused },
		OnError: func(err error) { errs = append(errs, err) },
	})
	if _, err := allocator.Allocate(); err != nil {
		t.Fatal(err)
	}
	reports := heartbeat.Report(context.Background())
	if len(reports) != 1 || reports[0].Accepted || !errors.Is(reports[0].Err, refused) {
		t.Fatalf("reports = %+v", reports)
	}
	if len(errs) != 1 || !otlp.IsHeartbeatError(errs[0], otlp.HeartbeatNotAccepted) || !errors.Is(errs[0], refused) {
		t.Fatalf("errors = %v", errs)
	}
}

func TestBuildCompletenessSignalSetsPresenceOnZero(t *testing.T) {
	signal := otlp.BuildCompletenessSignal(otlp.CompletenessFields{SequenceEpoch: 1})
	signal.ProducerReportedAt, signal.ActivitySince = nil, nil
	bytes, err := proto.Marshal(signal)
	if err != nil {
		t.Fatal(err)
	}
	// Fields 2, 3 and 5 are present with value 0.
	if got := string(bytes); got != "\x08\x01\x10\x00\x18\x00\x28\x00" {
		t.Fatalf("encoded = %x", bytes)
	}
}

func TestHeartbeatStopDuringPreviousEpochSuppressesCurrentSignal(t *testing.T) {
	var heartbeat *otlp.CompletenessHeartbeat
	var sent []uint64
	allocator, beat := newHeartbeatFixture(t, otlp.HeartbeatConfig{
		Send: func(_ context.Context, signal *modelv1.ProducerCompletenessSignal) error {
			sent = append(sent, signal.GetSequenceEpoch())
			heartbeat.Stop()
			return nil
		},
	})
	heartbeat = beat
	one, err := allocator.Allocate()
	if err != nil {
		t.Fatal(err)
	}
	allocator.MarkExported(one)
	allocator.Declare()
	if _, err := allocator.Allocate(); err != nil {
		t.Fatal(err)
	}
	reports := heartbeat.Report(context.Background())
	if len(reports) != 1 || len(sent) != 1 || sent[0] != heartbeatEpoch {
		t.Fatalf("reports=%+v, sent=%v; want previous epoch only", reports, sent)
	}
	if reports := heartbeat.Report(context.Background()); reports != nil {
		t.Fatalf("report after stop = %+v", reports)
	}
}
