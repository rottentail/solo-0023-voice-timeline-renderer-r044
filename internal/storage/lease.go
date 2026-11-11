package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"voice-timeline-renderer/internal/domain"
)

// renderLeaseRecord is the on-disk content of a render executor lease.
type renderLeaseRecord struct {
	RenderID  string    `json:"render_id"`
	Owner     string    `json:"owner"`
	ExpiresAt time.Time `json:"expires_at"`
}

// RenderLease is the exclusive qualification to execute and publish one render
// job. The holder must call Release once the job reaches a terminal state.
type RenderLease struct {
	repo *Repository
	path string
	info os.FileInfo
	once sync.Once
}

// AcquireRenderLease grants the caller the executor qualification for one
// render job. It returns (release, true, nil) when the caller holds the
// lease, (nil, false, nil) when another live executor holds it, and a non-nil
// error for storage failures. The release function must be called once the
// job reaches a terminal state. An expired or unreadable stale lease is
// reclaimed atomically; reclaimLive additionally takes over leases that have
// not expired, which is reserved for startup recovery where no other executor
// of this data directory can be alive.
func (repo *Repository) AcquireRenderLease(
	id string,
	owner string,
	ttl time.Duration,
	reclaimLive bool,
) (func() error, bool, error) {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if ttl <= 0 {
		ttl = time.Minute
	}
	path := repo.path("jobs", id+".lock")
	record := renderLeaseRecord{
		RenderID:  id,
		Owner:     owner,
		ExpiresAt: time.Now().UTC().Add(ttl),
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return nil, false, err
	}
	payload = append(payload, '\n')
	for attempt := 0; attempt < 3; attempt++ {
		handle, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
		if err == nil {
			lease, err := repo.finishLeaseAcquisition(handle, path, payload)
			if err != nil {
				return nil, false, err
			}
			return lease.Release, true, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, false, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot create render lease", err)
		}
		reclaimable, err := repo.leaseReclaimable(path, ttl, reclaimLive)
		if err != nil {
			return nil, false, err
		}
		if !reclaimable {
			return nil, false, nil
		}
		// Reclaim atomically: exactly one competitor can rename the stale
		// lease away; the others observe its disappearance and retry.
		claim := fmt.Sprintf("%s.reclaim-%d", path, time.Now().UnixNano())
		if err := os.Rename(path, claim); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, false, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot reclaim render lease", err)
		}
		_ = os.Remove(claim)
	}
	return nil, false, nil
}

func (repo *Repository) finishLeaseAcquisition(
	handle *os.File,
	path string,
	payload []byte,
) (*RenderLease, error) {
	fail := func(err error) (*RenderLease, error) {
		_ = handle.Close()
		_ = os.Remove(path)
		return nil, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot write render lease", err)
	}
	if _, err := handle.Write(payload); err != nil {
		return fail(err)
	}
	if err := handle.Sync(); err != nil {
		return fail(err)
	}
	info, err := handle.Stat()
	if err != nil {
		return fail(err)
	}
	if err := handle.Close(); err != nil {
		_ = os.Remove(path)
		return nil, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot write render lease", err)
	}
	if err := syncDirectory(repo.path("jobs")); err != nil {
		_ = os.Remove(path)
		return nil, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot sync render lease", err)
	}
	return &RenderLease{repo: repo, path: path, info: info}, nil
}

// leaseReclaimable reports whether the lease file at path may be taken over.
// A lease written by a live executor is never reclaimed unless reclaimLive is
// set. An unreadable lease falls back to its modification time so that a
// crashed writer cannot block a render forever.
func (repo *Repository) leaseReclaimable(path string, ttl time.Duration, reclaimLive bool) (bool, error) {
	if reclaimLive {
		return true, nil
	}
	var record renderLeaseRecord
	if err := readJSON(path, &record); err == nil {
		return !record.ExpiresAt.After(time.Now().UTC()), nil
	}
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		return false, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot inspect render lease", err)
	}
	return time.Since(info.ModTime()) > ttl, nil
}

// Release drops the executor qualification. The lease file is removed only
// when it is still the file this lease created; a lease that was reclaimed in
// the meantime belongs to another executor and is left untouched.
func (lease *RenderLease) Release() error {
	var err error
	lease.once.Do(func() {
		lease.repo.mu.Lock()
		defer lease.repo.mu.Unlock()
		info, statErr := os.Stat(lease.path)
		if statErr != nil {
			return
		}
		if lease.info != nil && !os.SameFile(lease.info, info) {
			return
		}
		err = os.Remove(lease.path)
	})
	return err
}
