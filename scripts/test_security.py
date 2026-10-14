#!/usr/bin/env python3
# Copyright 2026 gomaja. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Exercise the scheduled alert-count step with synthetic API responses."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class AlertCountTest(unittest.TestCase):
    def test_alert_query_outcomes(self):
        workflow = (ROOT / '.github/workflows/security.yml').read_text()
        step = workflow.split('      - name: Count scheduled CodeQL alerts\n', 1)[1]
        block = step.split('        run: |\n', 1)[1].split('      - name:', 1)[0]
        script = '\n'.join(line[10:] for line in block.splitlines())
        for name, response, status, expected in [
            ('failed', 'synthetic API failure', 1, False),
            ('empty', '', 0, False),
            ('invalid', 'invalid', 0, False),
            ('negative', '-1', 0, False),
            ('fractional', '0.5', 0, False),
            ('zero', '0', 0, True),
            ('positive', '7', 0, True),
        ]:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                tmp = Path(directory)
                gh = tmp / 'gh'
                gh.write_text('#!/bin/sh\nif [ "$MOCK_STATUS" -ne 0 ]; then\n'
                              '  printf "%s\\n" "$MOCK_RESPONSE" >&2\n'
                              'else\n  printf "%s" "$MOCK_RESPONSE"\nfi\nexit "$MOCK_STATUS"\n')
                gh.chmod(0o755)
                output = tmp / 'output'
                env = dict(os.environ, PATH=f'{tmp}:{os.environ["PATH"]}',
                           MOCK_RESPONSE=response, MOCK_STATUS=str(status),
                           GITHUB_REPOSITORY='example/locks', GITHUB_OUTPUT=str(output))
                result = subprocess.run(['bash', '-c', script], env=env,
                                        capture_output=True, text=True, check=False)
                self.assertEqual(result.returncode == 0, expected, result.stderr)
                emitted = output.read_text() if output.exists() else ''
                self.assertEqual(emitted, f'count={response}\n' if expected else '')
                if status:
                    self.assertIn(response, result.stderr)


if __name__ == '__main__':
    unittest.main()
