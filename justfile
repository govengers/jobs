set shell := ["bash", "-euo", "pipefail", "-c"]

# govengers/jobs, from the library template of govengers/standards (templates/library).
# A library pins its tools here with go run, not with tool directives in go.mod (TCH-002).

gotestsum := "go run gotest.tools/gotestsum@v1.13.0"
govulncheck := "go run golang.org/x/vuln/cmd/govulncheck@v1.9.0"
coverage_threshold := "80"
coverage_excluded := "/examples/|/test/|/internal/postgrestest/"
e2e_inputs := "test/e2e"

default:
    @just --list

# Vet and lint with the govengers baseline (.golangci.yml, copied from govengers/standards, LIB-005 checks enabled)
lint:
    go vet ./...
    go vet -tags e2e ./test/e2e/...
    golangci-lint run ./...

# Unit tests of changed and affected packages; the Go test cache replays the rest (TST-014). Fails below the coverage threshold (TST-010)
test *flags:
    mkdir -p sandbox
    {{gotestsum}} --format testname --jsonfile sandbox/unit.json -- -race -coverprofile=sandbox/unit.raw.cover {{flags}} ./...
    grep -v -E '{{coverage_excluded}}' sandbox/unit.raw.cover > sandbox/unit.cover
    go tool cover -func=sandbox/unit.cover > sandbox/unit.func
    awk -v threshold={{coverage_threshold}} '/^total:/ { found = 1; sub("%", "", $3); print "coverage " $3 "%"; if ($3 + 0 < threshold) { print "coverage is below " threshold "%"; exit 1 } } END { if (!found) { print "no coverage total"; exit 1 } }' sandbox/unit.func

# Every unit test, uncached and shuffled: the nightly run and before a release (TST-014, TST-005)
test-all: (test "-count=1" "-shuffle=on")

# Reproducible worker binary and compiled suite: their hash says whether the suite has anything new to test (TST-015)
e2e-build:
    mkdir -p sandbox/e2e-build
    go build -race -tags e2e -trimpath -buildvcs=false -o sandbox/e2e-build/worker ./test/e2e/worker
    go test -c -race -tags e2e -trimpath -buildvcs=false -o sandbox/e2e-build/ ./test/e2e/

# The end-to-end suite: two worker processes on one database (TST-011), always
test-e2e-all: e2e-build
    mkdir -p sandbox
    E2E_WORKER={{justfile_directory()}}/sandbox/e2e-build/worker {{gotestsum}} --format testname --jsonfile sandbox/e2e.json -- -race -tags e2e -trimpath -buildvcs=false -count=1 ./test/e2e/

# The end-to-end suite, skipped when worker, suite and inputs equal a green run (TST-015); keeps sandbox/e2e.json for CI badges
test-e2e: e2e-build
    #!/usr/bin/env bash
    set -euo pipefail
    key=$(find sandbox/e2e-build {{e2e_inputs}} -type f -print0 | sort -z | xargs -0 shasum -a 256 | shasum -a 256 | cut -c1-32)
    passed="sandbox/e2e-passed/$key"
    if [ -f "$passed/e2e.json" ]; then
        echo "e2e skipped: worker, suite and inputs equal the green run $key"
        cp "$passed/e2e.json" sandbox/e2e.json
        exit 0
    fi
    "{{just_executable()}}" test-e2e-all
    mkdir -p "$passed"
    cp sandbox/e2e.json "$passed/e2e.json"

# Scan for known vulnerabilities in reachable code (TCH-008)
vulncheck:
    {{govulncheck}} ./...

# Everything CI runs on a change
check: lint test test-e2e vulncheck
