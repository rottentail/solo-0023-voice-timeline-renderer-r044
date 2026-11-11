package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"time"

	"voice-timeline-renderer/internal/domain"
)

type Repository interface {
	SaveSource(domain.SourceAsset, []byte) error
	GetSourceMeta(string) (domain.SourceAsset, error)
	GetSource(string) (domain.SourceAsset, []byte, error)
	SaveTimeline(domain.Timeline) error
	GetTimeline(string) (domain.Timeline, error)
	SaveJob(domain.RenderJob) error
	GetJob(string) (domain.RenderJob, error)
	ListRenderJobs() ([]domain.RenderJob, error)
	CommitRenderArtifacts(string, []byte, domain.RenderReceipt) error
	GetRenderOutput(string) ([]byte, domain.RenderReceipt, error)
	LoadOwnedRenderArtifacts(string, string) ([]byte, domain.RenderReceipt, error)
	QuarantineRenderArtifacts(string) error
	CleanStaging() error
	// AcquireRenderLease returns a release function when the caller becomes
	// the executor for the render job; the release must be called once the
	// job reaches a terminal state.
	AcquireRenderLease(string, string, time.Duration, bool) (func() error, bool, error)
}

type Config struct {
	MaxSourceBytes  int64
	MaxSpans        int
	MaxOutputFrames int64
	// RenderLeaseTTL bounds how long an executor may hold the publish
	// qualification for one render before a crashed executor's lease becomes
	// reclaimable.
	RenderLeaseTTL time.Duration
}

func DefaultConfig() Config {
	return Config{
		MaxSourceBytes:  32 << 20,
		MaxSpans:        64,
		MaxOutputFrames: 48000 * 180,
		RenderLeaseTTL:  2 * time.Minute,
	}
}

type Service struct {
	repo     Repository
	config   Config
	executor string
	mu       sync.Mutex
}

func New(repo Repository, config Config) *Service {
	defaults := DefaultConfig()
	if config.MaxSourceBytes <= 0 {
		config.MaxSourceBytes = defaults.MaxSourceBytes
	}
	if config.MaxSpans <= 0 {
		config.MaxSpans = defaults.MaxSpans
	}
	if config.MaxOutputFrames <= 0 {
		config.MaxOutputFrames = defaults.MaxOutputFrames
	}
	if config.RenderLeaseTTL <= 0 {
		config.RenderLeaseTTL = defaults.RenderLeaseTTL
	}
	return &Service{repo: repo, config: config, executor: newExecutorToken()}
}

// newExecutorToken distinguishes this service instance from any other
// executor that shares the same data directory.
func newExecutorToken() string {
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return fmt.Sprintf("pid-%d", os.Getpid())
	}
	return fmt.Sprintf("pid-%d-%s", os.Getpid(), hex.EncodeToString(random[:]))
}

type SourceWriteRequest struct {
	Name      string `json:"name"`
	WAVBase64 string `json:"wav_base64"`
}

type TimelineWriteRequest struct {
	Name           string                `json:"name"`
	SampleRate     int                   `json:"sample_rate"`
	OutputChannels int                   `json:"output_channels"`
	TargetPeakDBFS float64               `json:"target_peak_dbfs"`
	Spans          []domain.TimelineSpan `json:"spans"`
}

type RenderWriteRequest struct {
	TimelineID string `json:"timeline_id"`
}

type RenderResult struct {
	Job domain.RenderJob
	WAV []byte
}

func (s *Service) GetSource(ctx context.Context, id string) (domain.SourceAsset, error) {
	if err := ctx.Err(); err != nil {
		return domain.SourceAsset{}, err
	}
	return s.repo.GetSourceMeta(id)
}

func (s *Service) GetTimeline(ctx context.Context, id string) (domain.Timeline, error) {
	if err := ctx.Err(); err != nil {
		return domain.Timeline{}, err
	}
	return s.repo.GetTimeline(id)
}

func (s *Service) GetRender(ctx context.Context, id string) (domain.RenderJob, error) {
	if err := ctx.Err(); err != nil {
		return domain.RenderJob{}, err
	}
	return s.repo.GetJob(id)
}

func (s *Service) GetReceipt(ctx context.Context, id string) (domain.RenderReceipt, error) {
	job, err := s.GetRender(ctx, id)
	if err != nil {
		return domain.RenderReceipt{}, err
	}
	if job.State != domain.RenderCompleted {
		return domain.RenderReceipt{}, domain.E(domain.KindUnready, "render_not_completed", "render_id", "render receipt is available only after completion")
	}
	_, receipt, err := s.repo.GetRenderOutput(id)
	return receipt, err
}

func (s *Service) GetOutput(ctx context.Context, id string) ([]byte, domain.RenderReceipt, error) {
	job, err := s.GetRender(ctx, id)
	if err != nil {
		return nil, domain.RenderReceipt{}, err
	}
	if job.State != domain.RenderCompleted {
		return nil, domain.RenderReceipt{}, domain.E(domain.KindUnready, "render_not_completed", "render_id", "render output is available only after completion")
	}
	return s.repo.GetRenderOutput(id)
}
