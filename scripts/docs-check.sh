#!/usr/bin/env bash
# Every check of the documents (docs/*_test.go; foundation B14 and E14): dead links
# and anchors in every markdown file outside plan/, the getting-started commands run
# as written and their output blocks compared with what the binary printed, the
# documents' kafka-*.sh lines run in apache/kafka (Docker), and what the documents
# say the program prints, logs or answers held to its source.
#
#   scripts/docs-check.sh                  kcat and Docker missing are skips
#   CI=1 DOCS_KAFKA_CLI=1 scripts/docs-check.sh   ... and failures
set -euo pipefail
cd "$(dirname "$0")/.."
exec env GOWORK=off go test -count=1 ./docs/ "$@"
