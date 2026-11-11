package service

import (
	"context"
	"log/slog"
)

// RecoverRenders resumes every render job that was left in a non-terminal
// state, for example by an interrupted process, and drives it to an
// explainable terminal state. It is meant to run once at service startup
// before requests are served: any lease found on disk then belongs to a dead
// executor and is reclaimed. Per-job outcomes are recorded on the jobs
// themselves; only systemic failures are returned.
func (s *Service) RecoverRenders(ctx context.Context) error {
	if err := s.repo.CleanStaging(); err != nil {
		return err
	}
	jobs, listErr := s.repo.ListRenderJobs()
	if listErr != nil && jobs == nil {
		return listErr
	}
	if listErr != nil {
		slog.Warn("render recovery skipped undecodable job records", "error", listErr)
	}
	resumed := 0
	for _, job := range jobs {
		if terminalRenderState(job.State) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.Lock()
		resumedJob, resumeErr := s.executeRender(ctx, job.ID, true)
		s.mu.Unlock()
		resumed++
		if resumeErr != nil {
			slog.Warn(
				"render recovery moved job to a terminal failure",
				"render_id", job.ID,
				"state", resumedJob.State,
				"error", resumeErr,
			)
			continue
		}
		slog.Info("render recovery resumed job", "render_id", job.ID, "state", resumedJob.State)
	}
	if resumed > 0 {
		slog.Info("render recovery completed", "resumed", resumed)
	}
	return nil
}
