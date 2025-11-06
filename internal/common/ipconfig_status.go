package common

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// FileOpsReader defines minimal file operations needed to read status
type FileOpsReader interface {
	ReadFile(string) ([]byte, error)
	IsNotExist(error) bool
}

// WriteIPConfigStatus writes the given status struct to the provided file path.
func WriteIPConfigStatus(filePath string, st IPConfigRunStatus) error {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	data, err := json.Marshal(st)
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, data, 0o600)
}

// FinalizeIPConfigStatus sets final phase, message and finishedAt in the given file.
// If a previous status exists, StartedAt is preserved.
func FinalizeIPConfigStatus(filePath string, phase IPConfigRunStatusPhase, msg string) error {
	st := IPConfigRunStatus{Phase: phase, Message: msg, FinishedAt: time.Now().UTC().Format(time.RFC3339)}
	if data, err := os.ReadFile(filePath); err == nil && len(data) > 0 {
		var prev IPConfigRunStatus
		if jsonErr := json.Unmarshal(data, &prev); jsonErr == nil {
			st.StartedAt = prev.StartedAt
		}
	}
	return WriteIPConfigStatus(filePath, st)
}

// ReadIPConfigStatus reads and parses the status file, returning phase and message.
// Returns Unknown when the file is not found.
func ReadIPConfigStatus(filePath string, fops FileOpsReader) (IPConfigRunStatusPhase, string, error) {
	data, err := fops.ReadFile(filePath)
	if err != nil {
		if fops.IsNotExist(err) {
			return IPConfigPhaseUnknown, "", nil
		}
		return IPConfigPhaseUnknown, "", fmt.Errorf("failed to read status file %s: %w", filePath, err)
	}

	var st IPConfigRunStatus
	if err := json.Unmarshal(data, &st); err != nil {
		return IPConfigPhaseUnknown, "", fmt.Errorf("failed to parse status file %s: %w", filePath, err)
	}

	switch st.Phase {
	case IPConfigPhaseRunning, IPConfigPhaseSucceeded, IPConfigPhaseFailed:
		return st.Phase, st.Message, nil
	default:
		return IPConfigPhaseUnknown, st.Message, nil
	}
}
