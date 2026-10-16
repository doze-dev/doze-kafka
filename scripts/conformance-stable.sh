#!/bin/sh
# Records every scripted scenario against apache/kafka:4.3.1 three times and
# fails unless each run rewrites conformance/recordings byte for byte as
# committed: a step whose answer depends on timing inside Kafka shows up here as
# a recording that changes (conformance/scenarios.go says every script is
# deterministic; this is what holds it to that). Needs Docker. Run it from a
# clean tree: the committed recordings are what each run is compared with.
set -eu
cd "$(dirname "$0")/.."
if ! git diff --quiet -- conformance/recordings || [ -n "$(git ls-files --others --exclude-standard conformance/recordings)" ]; then
	echo "conformance/recordings has uncommitted changes: commit or discard them first" >&2
	exit 2
fi
for run in 1 2 3; do
	echo "== run $run of 3"
	go test ./conformance -count=1 -run 'TestRecord$' -record -timeout 60m
	if ! git diff --quiet -- conformance/recordings || [ -n "$(git ls-files --others --exclude-standard conformance/recordings)" ]; then
		echo "run $run changed these recordings:" >&2
		git status --short -- conformance/recordings >&2
		exit 1
	fi
done
echo "three recordings of every scenario, identical to the committed ones"
