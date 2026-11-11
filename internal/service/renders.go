package service

import (
	"context"
	"errors"
	"time"

	"voice-timeline-renderer/internal/domain"
	"voice-timeline-renderer/internal/render"
)

func (s *Service) CreateRender(
	ctx context.Context,
	id string,
	request RenderWriteRequest,
) (domain.RenderJob, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return domain.RenderJob{}, false, err
	}
	if err := domain.ValidateResourceID("render_id", id); err != nil {
		return domain.RenderJob{}, false, err
	}
	if err := domain.ValidateResourceID("timeline_id", request.TimelineID); err != nil {
		return domain.RenderJob{}, false, err
	}

	existing, getErr := s.repo.GetJob(id)
	if getErr == nil {
		if existing.TimelineID != request.TimelineID {
			return domain.RenderJob{}, false, domain.E(domain.KindConflict, "render_conflict", "render_id", "identifier is already bound to a different timeline")
		}
		if terminalRenderState(existing.State) {
			return existing, false, nil
		}
		// The identifier is bound to a job that never reached a terminal
		// state (for example after an interruption). Resume it under the
		// original identifier instead of returning the stale record.
		job, err := s.executeRender(ctx, id, false)
		return job, false, err
	}
	if appErr := domain.AsAppError(getErr); appErr.Kind != domain.KindNotFound {
		return domain.RenderJob{}, false, getErr
	}

	plan, err := s.repo.GetTimeline(request.TimelineID)
	if err != nil {
		return domain.RenderJob{}, false, err
	}
	now := time.Now().UTC()
	job := domain.RenderJob{
		ID:         id,
		TimelineID: plan.ID,
		State:      domain.RenderQueued,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := s.repo.SaveJob(job); err != nil {
		return domain.RenderJob{}, false, err
	}
	job, err = s.executeRender(ctx, id, false)
	return job, true, err
}

func terminalRenderState(state domain.RenderState) bool {
	return state == domain.RenderCompleted || state == domain.RenderFailed
}

// executeRender drives one non-terminal job to an explainable terminal state.
// Exactly one executor holds the render lease at a time; a caller that cannot
// acquire it simply observes the current job state. Committed artifacts that
// prove they belong to this job are adopted into a completed state without
// re-mixing; files that cannot prove ownership are quarantined and the job is
// executed again.
func (s *Service) executeRender(ctx context.Context, id string, reclaimLiveLease bool) (domain.RenderJob, error) {
	release, acquired, err := s.repo.AcquireRenderLease(id, s.executor, s.config.RenderLeaseTTL, reclaimLiveLease)
	if err != nil {
		return domain.RenderJob{}, err
	}
	if !acquired {
		job, getErr := s.repo.GetJob(id)
		if getErr != nil {
			return domain.RenderJob{}, getErr
		}
		return job, nil
	}
	defer func() {
		_ = release()
	}()

	job, err := s.repo.GetJob(id)
	if err != nil {
		return domain.RenderJob{}, err
	}
	if terminalRenderState(job.State) {
		return job, nil
	}
	if err := ctx.Err(); err != nil {
		return job, err
	}

	_, receipt, adoptErr := s.repo.LoadOwnedRenderArtifacts(id, job.TimelineID)
	switch {
	case adoptErr == nil:
		// The output was fully committed and proves ownership; only the
		// completion confirmation was lost. Adopt it instead of re-mixing.
		if err := s.advanceTo(&job, domain.RenderRendering); err != nil {
			return job, err
		}
		if err := s.advanceTo(&job, domain.RenderFinalizing); err != nil {
			return job, err
		}
		output := receipt.Output
		job.Output = &output
		job.ReceiptDigest = receipt.Output.Digest
		if err := s.advanceTo(&job, domain.RenderCompleted); err != nil {
			return job, err
		}
		return job, nil
	case errors.Is(adoptErr, domain.ErrRenderArtifactsAbsent):
		// Nothing was committed; execute the job from its current state.
	case errors.Is(adoptErr, domain.ErrRenderArtifactsUnowned):
		if err := s.repo.QuarantineRenderArtifacts(id); err != nil {
			return s.failRender(job, err)
		}
	default:
		return s.failRender(job, adoptErr)
	}

	plan, err := s.repo.GetTimeline(job.TimelineID)
	if err != nil {
		return s.failRender(job, err)
	}
	if err := s.advanceTo(&job, domain.RenderRendering); err != nil {
		return job, err
	}
	sources := make(map[string]render.SourceInput)
	for _, span := range plan.Spans {
		if _, loaded := sources[span.SourceID]; loaded {
			continue
		}
		asset, payload, loadErr := s.repo.GetSource(span.SourceID)
		if loadErr != nil {
			return s.failRender(job, loadErr)
		}
		sources[span.SourceID] = render.SourceInput{Asset: asset, WAV: payload}
	}
	result, renderErr := render.Render(
		ctx,
		id,
		plan,
		sources,
		render.Limits{MaxOutputFrames: s.config.MaxOutputFrames},
		time.Now().UTC(),
	)
	if renderErr != nil {
		return s.failRender(job, domain.Wrap(domain.KindInternal, "render_failed", "", "timeline rendering failed", renderErr))
	}
	if err := s.advanceTo(&job, domain.RenderFinalizing); err != nil {
		return job, err
	}
	if err := s.repo.CommitRenderArtifacts(id, result.WAV, result.Receipt); err != nil {
		if appErr := domain.AsAppError(err); appErr.Kind == domain.KindConflict {
			// Another executor published first; deliver the stored
			// terminal state instead of overwriting it.
			if stored, getErr := s.repo.GetJob(id); getErr == nil && terminalRenderState(stored.State) {
				return stored, nil
			}
		}
		return s.failRender(job, err)
	}
	output := result.Output
	job.Output = &output
	job.ReceiptDigest = result.Output.Digest
	if err := s.advanceTo(&job, domain.RenderCompleted); err != nil {
		return job, err
	}
	return job, nil
}

// renderStateRank orders the forward path of the render state machine.
func renderStateRank(state domain.RenderState) int {
	switch state {
	case domain.RenderQueued:
		return 0
	case domain.RenderRendering:
		return 1
	case domain.RenderFinalizing:
		return 2
	case domain.RenderCompleted:
		return 3
	}
	return -1
}

// advanceTo moves a job forward to the requested state, skipping states the
// job already passed so a resumed job never regresses along the chain.
func (s *Service) advanceTo(job *domain.RenderJob, state domain.RenderState) error {
	if renderStateRank(job.State) >= renderStateRank(state) {
		return nil
	}
	return s.advanceJob(job, state, nil)
}

func (s *Service) advanceJob(job *domain.RenderJob, state domain.RenderState, detail *domain.ErrorDetail) error {
	if err := job.Transition(state, detail, time.Now().UTC()); err != nil {
		return err
	}
	if err := s.repo.SaveJob(*job); err != nil {
		return err
	}
	return nil
}

func (s *Service) failRender(job domain.RenderJob, cause error) (domain.RenderJob, error) {
	appErr := domain.AsAppError(cause)
	detail := &domain.ErrorDetail{
		Code:    appErr.Detail.Code,
		Message: appErr.Detail.Message,
		Field:   appErr.Detail.Field,
	}
	transitionErr := job.Transition(domain.RenderFailed, detail, time.Now().UTC())
	if transitionErr != nil {
		return job, transitionErr
	}
	job.Output = nil
	job.ReceiptDigest = ""
	if saveErr := s.repo.SaveJob(job); saveErr != nil {
		return job, saveErr
	}
	return job, appErr
}
