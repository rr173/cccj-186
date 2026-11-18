#!/usr/bin/env bash
# Run the whole extsort test suite (plain python, no pytest needed).
set -euo pipefail
cd "$(dirname "$0")/.."
for t in test_barrier_reclaim test_cancel_paths test_io_failure test_edges test_large \
         test_interval_correctness test_interval_validation \
         test_interval_lifecycle test_interval_scale \
         test_resumable test_resume_kill \
         test_gq_correctness test_gq_validation test_gq_lifecycle test_gq_scale \
         test_repartition_correctness test_repartition_lifecycle test_repartition_scale; do
    echo "=== $t ==="
    timeout 600 python3 "tests/$t.py"
done
echo "ALL SUITES PASSED"
