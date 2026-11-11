package checkflow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"voice-timeline-renderer/internal/audiocodec"
	"voice-timeline-renderer/internal/domain"
	"voice-timeline-renderer/internal/service"
	"voice-timeline-renderer/internal/storage"
)

func Run(ctx context.Context, workflow string) error {
	harness, err := NewHarness()
	if err != nil {
		return err
	}
	defer harness.Close()

	switch workflow {
	case "ingest-audio-source":
		return runIngest(ctx, harness)
	case "compose-timeline":
		return runCompose(ctx, harness)
	case "render-mixdown":
		return runRender(ctx, harness)
	case "recover-interrupted-render":
		return runRecover(ctx, harness)
	default:
		return fmt.Errorf("unknown workflow %q", workflow)
	}
}

func runIngest(ctx context.Context, harness *Harness) error {
	wav, err := sineWAV(48000, 1, 250, 440, 0.4)
	if err != nil {
		return err
	}
	asset, status, err := harness.createSource(ctx, "source_ingest_01", "narration take", wav)
	if err != nil {
		return err
	}
	if status != 201 {
		return fmt.Errorf("source write returned status %d, want 201", status)
	}
	if asset.FrameCount != 12000 || asset.BitsPerSample != 16 {
		return fmt.Errorf("source metadata is inconsistent: %+v", asset)
	}
	readBack, status, err := harness.getSource(ctx, asset.ID)
	if err != nil {
		return err
	}
	if status != 200 || readBack.Digest != asset.Digest {
		return fmt.Errorf("source read-back mismatch: status=%d digest=%s", status, readBack.Digest)
	}
	badStatus, body, err := harness.requestJSON(ctx, "PUT", "/v1/sources/source_ingest_bad", service.SourceWriteRequest{
		Name:      "broken take",
		WAVBase64: base64.StdEncoding.EncodeToString([]byte("not a wave")),
	})
	if err != nil {
		return err
	}
	if badStatus != 422 || failureCode(body) != "invalid_wav" {
		return fmt.Errorf("malformed media returned status=%d code=%s", badStatus, failureCode(body))
	}
	missing, status, err := harness.getSource(ctx, "source_ingest_bad")
	if err != nil {
		return err
	}
	if status != 404 || missing.ID != "" {
		return fmt.Errorf("failed source became visible: status=%d id=%s", status, missing.ID)
	}
	return nil
}

func runCompose(ctx context.Context, harness *Harness) error {
	first, err := sineWAV(48000, 1, 300, 220, 0.35)
	if err != nil {
		return err
	}
	second, err := sineWAV(44100, 2, 250, 330, 0.25)
	if err != nil {
		return err
	}
	if _, status, err := harness.createSource(ctx, "source_compose_a", "first span", first); err != nil || status != 201 {
		return fmt.Errorf("first source setup failed: status=%d err=%v", status, err)
	}
	if _, status, err := harness.createSource(ctx, "source_compose_b", "second span", second); err != nil || status != 201 {
		return fmt.Errorf("second source setup failed: status=%d err=%v", status, err)
	}
	validSpans := []domain.TimelineSpan{
		{SourceID: "source_compose_a", SourceStartFrame: 0, SourceEndFrame: 14400, GainDB: -3, CrossfadeFrames: 0},
		{SourceID: "source_compose_b", SourceStartFrame: 0, SourceEndFrame: 11025, GainDB: 1, CrossfadeFrames: 2400},
	}
	plan, status, err := harness.createTimeline(ctx, "timeline_compose_01", "two span assembly", 48000, 2, -1, validSpans)
	if err != nil {
		return err
	}
	if status != 201 || plan.State != "ready" {
		return fmt.Errorf("timeline write returned status=%d state=%s", status, plan.State)
	}
	readBack, status, err := harness.getTimeline(ctx, plan.ID)
	if err != nil {
		return err
	}
	if status != 200 || readBack.OutputFrames != plan.OutputFrames || len(readBack.Spans) != 2 {
		return fmt.Errorf("timeline read-back mismatch: status=%d frames=%d", status, readBack.OutputFrames)
	}
	invalidSpans := append([]domain.TimelineSpan(nil), validSpans...)
	invalidSpans[1].CrossfadeFrames = 999999
	_, status, err = harness.createTimeline(ctx, "timeline_compose_bad", "invalid crossfade", 48000, 2, -1, invalidSpans)
	if err != nil {
		return err
	}
	if status != 422 {
		return fmt.Errorf("invalid crossfade returned status %d, want 422", status)
	}
	return nil
}

func runRender(ctx context.Context, harness *Harness) error {
	first, err := sineWAV(48000, 1, 400, 220, 0.25)
	if err != nil {
		return err
	}
	second, err := sineWAV(44100, 2, 350, 330, 0.2)
	if err != nil {
		return err
	}
	if _, status, err := harness.createSource(ctx, "source_render_a", "opening take", first); err != nil || status != 201 {
		return fmt.Errorf("opening source setup failed: status=%d err=%v", status, err)
	}
	if _, status, err := harness.createSource(ctx, "source_render_b", "closing take", second); err != nil || status != 201 {
		return fmt.Errorf("closing source setup failed: status=%d err=%v", status, err)
	}
	spans := []domain.TimelineSpan{
		{SourceID: "source_render_a", SourceStartFrame: 0, SourceEndFrame: 19200, GainDB: -2, CrossfadeFrames: 0},
		{SourceID: "source_render_b", SourceStartFrame: 0, SourceEndFrame: 15435, GainDB: 0, CrossfadeFrames: 4800},
	}
	plan, status, err := harness.createTimeline(ctx, "timeline_render_01", "crossfaded narration", 48000, 2, -1, spans)
	if err != nil {
		return err
	}
	if status != 201 {
		return fmt.Errorf("timeline setup returned status %d", status)
	}
	job, status, err := harness.createRender(ctx, "render_mixdown_01", plan.ID)
	if err != nil {
		return err
	}
	if status != 201 || job.State != domain.RenderCompleted || job.Output == nil {
		return fmt.Errorf("render returned status=%d state=%s output=%v", status, job.State, job.Output)
	}
	fetched, status, err := harness.getRender(ctx, job.ID)
	if err != nil {
		return err
	}
	if status != 200 || fetched.State != domain.RenderCompleted {
		return fmt.Errorf("render read-back returned status=%d state=%s", status, fetched.State)
	}
	receipt, status, err := harness.getReceipt(ctx, job.ID)
	if err != nil {
		return err
	}
	if status != 200 || receipt.Output.FrameCount != plan.OutputFrames {
		return fmt.Errorf("receipt frame mismatch: status=%d receipt=%d plan=%d", status, receipt.Output.FrameCount, plan.OutputFrames)
	}
	audioBytes, status, err := harness.getOutput(ctx, job.ID)
	if err != nil {
		return err
	}
	if status != 200 {
		return fmt.Errorf("output returned status %d", status)
	}
	digest := sha256.Sum256(audioBytes)
	if hex.EncodeToString(digest[:]) != receipt.Output.Digest {
		return fmt.Errorf("output digest does not match receipt")
	}
	clip, info, err := audiocodec.DecodeWAV(audioBytes)
	if err != nil {
		return fmt.Errorf("decode rendered output: %w", err)
	}
	measured := audiocodec.Measure(clip.Samples)
	if info.FrameCount != receipt.Output.FrameCount {
		return fmt.Errorf("decoded frame count %d != receipt %d", info.FrameCount, receipt.Output.FrameCount)
	}
	if math.Abs(measured.PeakDBFS-(-1)) > 0.25 {
		return fmt.Errorf("output peak %.3f dBFS does not meet -1 dBFS target", measured.PeakDBFS)
	}
	return nil
}

// runRecover verifies that an interrupted render reaches an explainable
// terminal state after a service restart while the caller keeps using the
// original render identifier:
//   - committed artifacts that prove ownership are adopted without re-mixing,
//   - disk files that cannot prove ownership are never served as output,
//   - a queued job resumes through the pipeline,
//   - only one executor holds the publish qualification at a time.
func runRecover(ctx context.Context, harness *Harness) error {
	first, err := sineWAV(48000, 1, 300, 260, 0.3)
	if err != nil {
		return err
	}
	second, err := sineWAV(44100, 2, 250, 330, 0.25)
	if err != nil {
		return err
	}
	if _, status, err := harness.createSource(ctx, "source_recover_a", "adopted take", first); err != nil || status != 201 {
		return fmt.Errorf("adopt source setup failed: status=%d err=%v", status, err)
	}
	if _, status, err := harness.createSource(ctx, "source_recover_b", "rebuilt take", second); err != nil || status != 201 {
		return fmt.Errorf("rebuild source setup failed: status=%d err=%v", status, err)
	}
	adoptSpans := []domain.TimelineSpan{
		{SourceID: "source_recover_a", SourceStartFrame: 0, SourceEndFrame: 14400, GainDB: -2, CrossfadeFrames: 0},
	}
	adoptPlan, status, err := harness.createTimeline(ctx, "timeline_recover_adopt", "adopt assembly", 48000, 1, -1, adoptSpans)
	if err != nil || status != 201 {
		return fmt.Errorf("adopt timeline setup failed: status=%d err=%v", status, err)
	}
	rebuildSpans := []domain.TimelineSpan{
		{SourceID: "source_recover_b", SourceStartFrame: 0, SourceEndFrame: 11025, GainDB: 0, CrossfadeFrames: 0},
	}
	rebuildPlan, status, err := harness.createTimeline(ctx, "timeline_recover_rebuild", "rebuild assembly", 48000, 2, -1, rebuildSpans)
	if err != nil || status != 201 {
		return fmt.Errorf("rebuild timeline setup failed: status=%d err=%v", status, err)
	}

	// Scenario A: the completion confirmation is lost after the artifacts
	// were committed; the restart must adopt them without re-mixing.
	adoptJob, status, err := harness.createRender(ctx, "render_recover_adopt", adoptPlan.ID)
	if err != nil || status != 201 || adoptJob.State != domain.RenderCompleted {
		return fmt.Errorf("adopt render setup failed: status=%d state=%s err=%v", status, adoptJob.State, err)
	}
	adoptReceipt, status, err := harness.getReceipt(ctx, adoptJob.ID)
	if err != nil || status != 200 {
		return fmt.Errorf("adopt receipt setup failed: status=%d err=%v", status, err)
	}
	repo, err := harness.repository()
	if err != nil {
		return err
	}
	if err := forceJobState(repo, adoptJob.ID, domain.RenderFinalizing); err != nil {
		return fmt.Errorf("simulate lost completion: %w", err)
	}
	// Re-mixing is impossible once the source media is gone; only adoption
	// can complete the job.
	if err := os.Remove(filepath.Join(harness.tempDir, "sources", "source_recover_a", "source.wav")); err != nil {
		return err
	}
	if err := harness.restart(ctx); err != nil {
		return err
	}
	adopted, status, err := harness.getRender(ctx, adoptJob.ID)
	if err != nil || status != 200 {
		return fmt.Errorf("adopt read-back failed: status=%d err=%v", status, err)
	}
	if adopted.State != domain.RenderCompleted || adopted.Output == nil {
		return fmt.Errorf("interrupted render was not adopted: state=%s output=%v", adopted.State, adopted.Output)
	}
	if adopted.Output.Digest != adoptReceipt.Output.Digest {
		return fmt.Errorf("adopted output digest %s != original receipt %s", adopted.Output.Digest, adoptReceipt.Output.Digest)
	}
	adoptedAudio, status, err := harness.getOutput(ctx, adoptJob.ID)
	if err != nil || status != 200 {
		return fmt.Errorf("adopted output unavailable: status=%d err=%v", status, err)
	}
	adoptedDigest := sha256.Sum256(adoptedAudio)
	if hex.EncodeToString(adoptedDigest[:]) != adoptReceipt.Output.Digest {
		return fmt.Errorf("adopted output bytes do not match the original receipt")
	}
	adoptedReceipt, status, err := harness.getReceipt(ctx, adoptJob.ID)
	if err != nil || status != 200 {
		return fmt.Errorf("adopted receipt unavailable: status=%d err=%v", status, err)
	}
	if !adoptedReceipt.CreatedAt.Equal(adoptReceipt.CreatedAt) || adoptedReceipt.Output.Digest != adoptReceipt.Output.Digest {
		return fmt.Errorf("original receipt was rewritten during adoption")
	}

	// Scenario B: files that cannot prove ownership must not become the
	// successful output; the job is executed again instead.
	rebuildJob, status, err := harness.createRender(ctx, "render_recover_rebuild", rebuildPlan.ID)
	if err != nil || status != 201 || rebuildJob.State != domain.RenderCompleted {
		return fmt.Errorf("rebuild render setup failed: status=%d state=%s err=%v", status, rebuildJob.State, err)
	}
	if err := forceJobState(repo, rebuildJob.ID, domain.RenderRendering); err != nil {
		return fmt.Errorf("simulate interrupted render: %w", err)
	}
	foreign := []byte("foreign bytes that cannot prove ownership")
	if err := os.WriteFile(filepath.Join(harness.tempDir, "renders", rebuildJob.ID, "output.wav"), foreign, 0o640); err != nil {
		return err
	}
	if err := harness.restart(ctx); err != nil {
		return err
	}
	rebuilt, status, err := harness.getRender(ctx, rebuildJob.ID)
	if err != nil || status != 200 || rebuilt.State != domain.RenderCompleted {
		return fmt.Errorf("rebuild render did not complete: status=%d state=%s err=%v", status, rebuilt.State, err)
	}
	rebuiltAudio, status, err := harness.getOutput(ctx, rebuildJob.ID)
	if err != nil || status != 200 {
		return fmt.Errorf("rebuilt output unavailable: status=%d err=%v", status, err)
	}
	if bytes.Equal(rebuiltAudio, foreign) {
		return fmt.Errorf("unowned disk files were served as the render output")
	}
	rebuiltReceipt, status, err := harness.getReceipt(ctx, rebuildJob.ID)
	if err != nil || status != 200 {
		return fmt.Errorf("rebuilt receipt unavailable: status=%d err=%v", status, err)
	}
	rebuiltDigest := sha256.Sum256(rebuiltAudio)
	if hex.EncodeToString(rebuiltDigest[:]) != rebuiltReceipt.Output.Digest {
		return fmt.Errorf("rebuilt output bytes do not match the rebuilt receipt")
	}
	quarantined, err := filepath.Glob(filepath.Join(harness.tempDir, "tmp", "quarantine-"+rebuildJob.ID+"-*"))
	if err != nil || len(quarantined) != 1 {
		return fmt.Errorf("unowned artifacts were not quarantined: matches=%d err=%v", len(quarantined), err)
	}

	// Scenario C: a job that never started resumes through the pipeline
	// under its original identifier.
	repo, err = harness.repository()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	queuedJob := domain.RenderJob{
		ID:         "render_recover_queued",
		TimelineID: rebuildPlan.ID,
		State:      domain.RenderQueued,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := repo.SaveJob(queuedJob); err != nil {
		return fmt.Errorf("inject queued job: %w", err)
	}
	if err := harness.restart(ctx); err != nil {
		return err
	}
	resumed, status, err := harness.getRender(ctx, queuedJob.ID)
	if err != nil || status != 200 || resumed.State != domain.RenderCompleted {
		return fmt.Errorf("queued job did not resume: status=%d state=%s err=%v", status, resumed.State, err)
	}
	if _, status, err := harness.getOutput(ctx, queuedJob.ID); err != nil || status != 200 {
		return fmt.Errorf("resumed output unavailable: status=%d err=%v", status, err)
	}

	// Scenario D: while another executor holds the lease, a same-identifier
	// request only observes the job; once released, the request resumes it.
	if err := forceJobState(repo, queuedJob.ID, domain.RenderRendering); err != nil {
		return fmt.Errorf("simulate in-flight render: %w", err)
	}
	release, acquired, err := repo.AcquireRenderLease(queuedJob.ID, "competing-executor", time.Minute, false)
	if err != nil || !acquired {
		return fmt.Errorf("competing executor could not acquire the lease: acquired=%v err=%v", acquired, err)
	}
	blocked, status, err := harness.createRender(ctx, queuedJob.ID, rebuildPlan.ID)
	if err != nil || status != 200 {
		return fmt.Errorf("same-identifier request failed: status=%d err=%v", status, err)
	}
	if blocked.State != domain.RenderRendering {
		return fmt.Errorf("same-identifier request did not observe the in-flight job: state=%s", blocked.State)
	}
	if err := release(); err != nil {
		return fmt.Errorf("release competing lease: %w", err)
	}
	finished, status, err := harness.createRender(ctx, queuedJob.ID, rebuildPlan.ID)
	if err != nil || status != 200 || finished.State != domain.RenderCompleted {
		return fmt.Errorf("released render did not resume: status=%d state=%s err=%v", status, finished.State, err)
	}
	return nil
}

// forceJobState rewrites the persisted state of a render job to simulate an
// interrupted process. Forcing an active state drops the completion metadata
// that a real interruption would not have persisted yet.
func forceJobState(repo *storage.Repository, id string, state domain.RenderState) error {
	job, err := repo.GetJob(id)
	if err != nil {
		return err
	}
	job.State = state
	if state != domain.RenderCompleted && state != domain.RenderFailed {
		job.Output = nil
		job.ReceiptDigest = ""
	}
	return repo.SaveJob(job)
}
