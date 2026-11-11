package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"voice-timeline-renderer/internal/domain"
)

// ListRenderJobs returns every decodable render job record. Jobs whose files
// cannot be decoded are skipped and reported through the returned error so
// that recovery can continue with the remaining records.
func (repo *Repository) ListRenderJobs() ([]domain.RenderJob, error) {
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	entries, err := os.ReadDir(repo.path("jobs"))
	if err != nil {
		return nil, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot list render jobs", err)
	}
	jobs := make([]domain.RenderJob, 0, len(entries))
	var firstErr error
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		var job domain.RenderJob
		if err := readJSON(repo.path("jobs", entry.Name()), &job); err != nil {
			if firstErr == nil {
				firstErr = domain.Wrap(domain.KindInternal, "storage_error", "", "cannot read render job", err)
			}
			continue
		}
		jobs = append(jobs, job)
	}
	return jobs, firstErr
}

// LoadOwnedRenderArtifacts reads a committed render artifact directory and
// proves that it belongs to the given render job and timeline. It returns
// domain.ErrRenderArtifactsAbsent when nothing was committed and
// domain.ErrRenderArtifactsUnowned when the files on disk cannot establish
// ownership: a missing or unreadable receipt, a receipt naming a different
// render or timeline, missing output media, or output bytes whose digest
// disagrees with the receipt.
func (repo *Repository) LoadOwnedRenderArtifacts(
	id string,
	timelineID string,
) ([]byte, domain.RenderReceipt, error) {
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	final := repo.path("renders", id)
	if _, err := os.Stat(final); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, domain.RenderReceipt{}, domain.ErrRenderArtifactsAbsent
		}
		return nil, domain.RenderReceipt{}, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot inspect render artifacts", err)
	}
	unowned := func(reason string) error {
		return fmt.Errorf("render %s artifacts: %s: %w", id, reason, domain.ErrRenderArtifactsUnowned)
	}
	var receipt domain.RenderReceipt
	if err := readJSON(filepath.Join(final, "receipt.json"), &receipt); err != nil {
		return nil, domain.RenderReceipt{}, unowned("receipt is missing or unreadable")
	}
	if receipt.RenderID != id || receipt.TimelineID != timelineID {
		return nil, domain.RenderReceipt{}, unowned("receipt identity does not match the render job")
	}
	payload, err := os.ReadFile(filepath.Join(final, "output.wav"))
	if err != nil {
		return nil, domain.RenderReceipt{}, unowned("output media is missing or unreadable")
	}
	digest := sha256.Sum256(payload)
	if receipt.Output.Digest == "" || hex.EncodeToString(digest[:]) != receipt.Output.Digest {
		return nil, domain.RenderReceipt{}, unowned("output digest does not match the receipt")
	}
	return payload, receipt, nil
}

// QuarantineRenderArtifacts moves a render artifact directory that cannot
// prove ownership out of the published location so the job can be executed
// again. The directory is kept under tmp/ for diagnosis instead of being
// deleted.
func (repo *Repository) QuarantineRenderArtifacts(id string) error {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	final := repo.path("renders", id)
	if _, err := os.Stat(final); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot inspect render artifacts", err)
	}
	target := repo.path("tmp", fmt.Sprintf("quarantine-%s-%d", id, time.Now().UnixNano()))
	if err := os.Rename(final, target); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot quarantine render artifacts", err)
	}
	if err := syncDirectory(repo.path("renders")); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot sync render index", err)
	}
	if err := syncDirectory(repo.path("tmp")); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot sync staging index", err)
	}
	return nil
}

// CleanStaging removes staging directories left behind by interrupted source
// and render commits. Quarantined artifacts are preserved.
func (repo *Repository) CleanStaging() error {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	entries, err := os.ReadDir(repo.path("tmp"))
	if err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot list staging directory", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "render-") && !strings.HasPrefix(name, "source-") {
			continue
		}
		if err := os.RemoveAll(filepath.Join(repo.path("tmp"), name)); err != nil {
			return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot remove staging directory", err)
		}
	}
	return syncDirectory(repo.path("tmp"))
}
