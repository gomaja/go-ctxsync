// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIRejectsInvalidVariantsBeforeSearch(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "modelcheck")
	if output, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	for _, tc := range []struct{ name, design, scenario, variant, diagnostic string }{
		{"sendlock-unknown-liveness", "sendlock", "liveness", "not-a-variant", "unknown variant"},
		{"sendlock-unsupported-liveness", "sendlock", "liveness", string(reReadMode), "unsupported"},
		{"sendlock-unknown-bounded", "sendlock", "bounded", "not-a-variant", "unknown variant"},
		{"queue-unknown-liveness", "queue", "liveness", "not-a-variant", "unknown queue variant"},
		{"rw-unknown-liveness", "rw", "liveness", "not-a-variant", "unknown RW variant"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outputPath := filepath.Join(t.TempDir(), "result.json")
			cmd := exec.Command(binary, "-design", tc.design, "-scenario", tc.scenario, "-variant", tc.variant, "-limit", "1", "-output", outputPath)
			output, err := cmd.CombinedOutput()
			if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != 2 || !strings.Contains(string(output), tc.diagnostic) {
				t.Fatalf("invalid invocation: err=%v output=%s", err, output)
			}
			if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
				t.Fatalf("invalid invocation must not produce search results: %v", err)
			}
		})
	}
}
