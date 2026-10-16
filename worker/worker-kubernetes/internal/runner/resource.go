package runner

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

func (m *sessionManager) resource(ctx context.Context, r terminalResourceRequest) (terminalResourceRunResult, error) {
	s, _, err := m.claim(ctx, r.SessionID, false, nil, false)
	if err != nil {
		return terminalResourceRunResult{}, err
	}
	defer m.release(s)
	action := strings.ToLower(strings.TrimSpace(r.Action))
	if action == "" {
		action = terminalResourceActionValidate
	}
	if action != "validate" && action != "read" && action != "export" {
		return terminalResourceRunResult{}, newTerminalExecError(terminalExecCodeInvalidPayload, "invalid resource action")
	}
	probeAction := action
	limit := m.cfg.TerminalOutputLimitBytes
	if action == "export" {
		probeAction = "validate"
	}
	out := &boundedBuffer{limit: limit*2 + 4096}
	stderr := &boundedBuffer{limit: 4096}
	status, err := m.backend.exec(ctx, s.name, []string{"python3", "-c", terminalResourceProbeScript, "--action", probeAction, "--file-path", r.FilePath, "--max-read-bytes", strconv.Itoa(limit)}, nil, out, stderr)
	if err != nil {
		return terminalResourceRunResult{}, err
	}
	if out.truncated {
		return terminalResourceRunResult{}, newTerminalExecError(terminalResourceCodeFileTooLarge, "resource exceeds limit")
	}
	var probe terminalResourceProbeResult
	if err = json.Unmarshal(out.Bytes(), &probe); err != nil {
		return terminalResourceRunResult{}, errors.New("invalid resource response")
	}
	if probe.Error != "" {
		return terminalResourceRunResult{}, newTerminalExecError(probe.Error, probe.Message)
	}
	if status != 0 {
		return terminalResourceRunResult{}, errors.New("resource command failed")
	}
	blob, err := base64.StdEncoding.DecodeString(probe.Blob)
	if err != nil {
		return terminalResourceRunResult{}, errors.New("invalid resource encoding")
	}
	result := terminalResourceRunResult{SessionID: s.id, FilePath: r.FilePath, MIMEType: probe.MIMEType, SizeBytes: probe.Size}
	if action == "read" {
		result.Blob = blob
	}
	if action == "export" {
		if m.cfg.TerminalExportMaxBytes > 0 && probe.Size > int64(m.cfg.TerminalExportMaxBytes) {
			return result, newTerminalExecError(terminalResourceCodeFileTooLarge, "file exceeds export limit")
		}
		exportCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		reader, writer := io.Pipe()
		defer reader.Close()
		completed := make(chan error, 1)
		go func() {
			_, execErr := m.backend.exec(exportCtx, s.name, []string{"python3", "-c", "import sys, shutil; f=open(sys.argv[1], 'rb'); shutil.copyfileobj(f, sys.stdout.buffer)", r.FilePath}, nil, writer, io.Discard)
			_ = writer.CloseWithError(execErr)
			completed <- execErr
		}()
		request, err := http.NewRequestWithContext(exportCtx, http.MethodPut, r.SignedURL, reader)
		if err != nil {
			return result, errors.New("invalid upload URL")
		}
		request.ContentLength = probe.Size
		for k, v := range r.Headers {
			key := strings.ToLower(strings.TrimSpace(k))
			if key == "content-type" || key == "content-md5" || strings.HasPrefix(key, "x-amz-") {
				request.Header.Set(k, v)
			}
		}
		client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		resp, err := client.Do(request)
		if err != nil {
			return result, errors.New("export upload failed")
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return result, fmt.Errorf("export upload HTTP %d", resp.StatusCode)
		}
		if err := <-completed; err != nil {
			return result, errors.New("export file transfer failed")
		}
	}
	return result, nil
}
