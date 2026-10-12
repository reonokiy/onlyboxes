package runner

import (
	"strings"
)

const (
	terminalExecCapabilityName           = "terminalexec"
	terminalExecCapabilityDeclared       = "terminalExec"
	terminalLeaseRenewCapabilityName     = "terminalleaserenew"
	terminalLeaseRenewCapabilityDeclared = "terminalLeaseRenew"
	terminalExecNoSessionMessage         = "session not found"
	terminalExecBusyMessage              = "session is busy"
	terminalExecCapacityMessage          = "terminal session capacity exceeded"
	terminalExecNotReadyMessage          = "terminal executor is unavailable"

	// defaultTerminalSessionMaxInflight keeps one command per session, matching
	// the behaviour before per-session concurrency became configurable.
	defaultTerminalSessionMaxInflight = 1
)

const (
	terminalExecCodeSessionNotFound         = "session_not_found"
	terminalExecCodeSessionBusy             = "session_busy"
	terminalExecCodeSessionCapacityExceeded = "session_capacity_exceeded"
	terminalExecCodeInvalidPayload          = "invalid_payload"
)

type terminalExecPayload struct {
	Command         string `json:"command"`
	SessionID       string `json:"session_id,omitempty"`
	CreateIfMissing bool   `json:"create_if_missing,omitempty"`
	LeaseTTLSec     *int   `json:"lease_ttl_sec,omitempty"`
}

type terminalExecRequest struct {
	Command         string
	SessionID       string
	CreateIfMissing bool
	LeaseTTLSec     *int
}

type terminalExecRunResult struct {
	SessionID          string `json:"session_id"`
	Created            bool   `json:"created"`
	Stdout             string `json:"stdout"`
	Stderr             string `json:"stderr"`
	ExitCode           int    `json:"exit_code"`
	StdoutTruncated    bool   `json:"stdout_truncated"`
	StderrTruncated    bool   `json:"stderr_truncated"`
	LeaseExpiresUnixMS int64  `json:"lease_expires_unix_ms"`
}

type terminalLeaseRenewPayload struct {
	SessionID   string `json:"session_id"`
	LeaseTTLSec int    `json:"lease_ttl_sec"`
}

type terminalLeaseRenewResult struct {
	SessionID          string `json:"session_id"`
	LeaseExpiresUnixMS int64  `json:"lease_expires_unix_ms"`
}

type terminalExecError struct {
	code    string
	message string
}

func (e *terminalExecError) Error() string {
	if e == nil {
		return "terminal execution failed"
	}
	return e.message
}

func (e *terminalExecError) Code() string {
	if e == nil {
		return ""
	}
	return e.code
}

func newTerminalExecError(code string, message string) *terminalExecError {
	return &terminalExecError{
		code:    strings.TrimSpace(code),
		message: strings.TrimSpace(message),
	}
}

const (
	terminalResourceCapabilityName     = "terminalresource"
	terminalResourceCapabilityDeclared = "terminalResource"
	terminalResourceActionValidate     = "validate"
	terminalResourceActionRead         = "read"
	terminalResourceActionExport       = "export"
	terminalResourceCodeFileNotFound   = "file_not_found"
	terminalResourceCodePathIsDir      = "path_is_directory"
	terminalResourceCodeFileTooLarge   = "file_too_large"
)

type terminalResourcePayload struct {
	SessionID string            `json:"session_id"`
	FilePath  string            `json:"file_path"`
	Action    string            `json:"action,omitempty"`
	SignedURL string            `json:"signed_url,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
}

type terminalResourceRequest struct {
	SessionID string
	FilePath  string
	Action    string
	SignedURL string
	Headers   map[string]string
}

type terminalResourceRunResult struct {
	SessionID string `json:"session_id"`
	FilePath  string `json:"file_path"`
	MIMEType  string `json:"mime_type"`
	SizeBytes int64  `json:"size_bytes"`
	Blob      []byte `json:"blob,omitempty"`
}

type terminalResourceProbeResult struct {
	Error    string `json:"error,omitempty"`
	Message  string `json:"message,omitempty"`
	MIMEType string `json:"mime_type,omitempty"`
	Size     int64  `json:"size_bytes"`
	Blob     string `json:"blob,omitempty"`
}

const terminalResourceProbeScript = `
import argparse
import base64
import json
import mimetypes
import os
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--action", choices=["validate", "read"], default="validate")
parser.add_argument("--file-path", required=True)
parser.add_argument("--max-read-bytes", type=int, required=True)
args = parser.parse_args()

target = args.file_path
if not os.path.exists(target):
    print(json.dumps({"error": "file_not_found", "message": "file not found"}))
    sys.exit(10)
if os.path.isdir(target):
    print(json.dumps({"error": "path_is_directory", "message": "path is directory"}))
    sys.exit(11)

size_bytes = os.path.getsize(target)
mime_type, _ = mimetypes.guess_type(target)
if not mime_type:
    mime_type = "application/octet-stream"

if args.action == "validate":
    print(json.dumps({"mime_type": mime_type, "size_bytes": size_bytes}))
    sys.exit(0)

limit = args.max_read_bytes
if size_bytes > limit:
    print(json.dumps({
        "error": "file_too_large",
        "message": "file exceeds read limit",
        "mime_type": mime_type,
        "size_bytes": size_bytes,
    }))
    sys.exit(12)

with open(target, "rb") as fh:
    content = fh.read(limit + 1)
if len(content) > limit:
    print(json.dumps({
        "error": "file_too_large",
        "message": "file exceeds read limit",
        "mime_type": mime_type,
        "size_bytes": len(content),
    }))
    sys.exit(12)

print(json.dumps({
    "mime_type": mime_type,
    "size_bytes": len(content),
    "blob": base64.b64encode(content).decode("ascii"),
}))
`
