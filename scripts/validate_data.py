"""Run DATA tests and persist observed results; strict mode rejects skips."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import unittest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--require-spark', action='store_true', help='Fail if any integration check is skipped')
    parser.add_argument('--output', default='reports/results/data-validation.json')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    suite = unittest.defaultTestLoader.discover(str(root / 'tests'))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    report = {'recorded_at': datetime.now(timezone.utc).isoformat(),
              'python_version': platform.python_version(), 'tests_run': result.testsRun,
              'passed': result.testsRun - len(result.skipped) - len(result.errors) - len(result.failures),
              'skipped': [{'test': str(test), 'reason': reason} for test, reason in result.skipped],
              'failures': [{'test': str(test), 'traceback': trace} for test, trace in result.failures],
              'errors': [{'test': str(test), 'traceback': trace} for test, trace in result.errors],
              'strict': args.require_spark,
              'acceptance_passed': result.wasSuccessful() and not result.skipped}
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    if not result.wasSuccessful() or (args.require_spark and result.skipped):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
