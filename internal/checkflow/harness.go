package checkflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"voice-timeline-renderer/internal/httpapi"
	"voice-timeline-renderer/internal/service"
	"voice-timeline-renderer/internal/storage"
)

type Harness struct {
	baseURL string
	client  *http.Client
	tempDir string
	server  *httptest.Server
}

func NewHarness() (*Harness, error) {
	tempDir, err := os.MkdirTemp("", "voice-timeline-check-*")
	if err != nil {
		return nil, err
	}
	harness := &Harness{
		client:  &http.Client{Timeout: 45 * time.Second},
		tempDir: tempDir,
	}
	if err := harness.start(context.Background()); err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, err
	}
	return harness, nil
}

// start builds the repository, service, and HTTP stack on the harness data
// directory. Startup render recovery runs before the server accepts requests,
// mirroring the production entry point.
func (harness *Harness) start(ctx context.Context) error {
	repo, err := storage.NewRepository(harness.tempDir)
	if err != nil {
		return err
	}
	serviceLayer := service.New(repo, service.DefaultConfig())
	if err := serviceLayer.RecoverRenders(ctx); err != nil {
		return err
	}
	httpServer := httpapi.New(serviceLayer, httpapi.Options{
		MaxSourceRequestBytes: 48 << 20,
		MaxJSONBytes:          2 << 20,
	})
	harness.server = httptest.NewServer(httpServer.Handler())
	harness.baseURL = harness.server.URL
	return nil
}

// restart simulates a service restart over the same data directory.
func (harness *Harness) restart(ctx context.Context) error {
	harness.server.Close()
	return harness.start(ctx)
}

// repository opens an independent repository handle on the harness data
// directory for controlled fault injection.
func (harness *Harness) repository() (*storage.Repository, error) {
	return storage.NewRepository(harness.tempDir)
}

func (harness *Harness) Close() {
	harness.server.Close()
	_ = os.RemoveAll(harness.tempDir)
}

func (harness *Harness) requestJSON(
	ctx context.Context,
	method string,
	path string,
	payload any,
) (int, []byte, error) {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, err
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, harness.baseURL+path, body)
	if err != nil {
		return 0, nil, err
	}
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := harness.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	result, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, nil, err
	}
	return response.StatusCode, result, nil
}

func decodeBody(body []byte, target any) error {
	if err := json.Unmarshal(body, target); err != nil {
		return fmt.Errorf("decode response %q: %w", string(body), err)
	}
	return nil
}
