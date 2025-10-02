#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Script to build and run all active message examples
# Handles both standalone examples and multi-process coordination tests

set -euo pipefail

# ============================================================================
# Configuration
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_MODE="release"
TIMEOUT_SECONDS=30
VERBOSE=false

# Process tracking
declare -a BACKGROUND_PIDS=()
declare -a FAILED_TESTS=()
declare -a PASSED_TESTS=()

# ============================================================================
# Color Output
# ============================================================================

if [[ -t 1 ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    CYAN='\033[0;36m'
    BOLD='\033[1m'
    NC='\033[0m' # No Color
else
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    CYAN=''
    BOLD=''
    NC=''
fi

# ============================================================================
# Helper Functions
# ============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[PASS]${NC} $*"
}

log_error() {
    echo -e "${RED}[FAIL]${NC} $*"
}

log_warning() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

print_separator() {
    echo -e "${CYAN}========================================${NC}"
}

print_header() {
    echo ""
    print_separator
    echo -e "${BOLD}$*${NC}"
    print_separator
}

# ============================================================================
# Cleanup Function
# ============================================================================

cleanup() {
    local exit_code=$?

    if [[ ${#BACKGROUND_PIDS[@]} -gt 0 ]]; then
        log_info "Cleaning up background processes..."
        for pid in "${BACKGROUND_PIDS[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                log_info "Killing process $pid"
                kill -TERM "$pid" 2>/dev/null || true
                # Give it a moment to terminate gracefully
                sleep 0.5
                # Force kill if still running
                if kill -0 "$pid" 2>/dev/null; then
                    kill -KILL "$pid" 2>/dev/null || true
                fi
            fi
        done
    fi

    # Wait for all background jobs to finish
    wait 2>/dev/null || true

    exit $exit_code
}

trap cleanup EXIT INT TERM

# ============================================================================
# Build Functions
# ============================================================================

build_examples() {
    print_header "Building Examples"

    cd "$SCRIPT_DIR"

    local build_flags=()
    if [[ "$BUILD_MODE" == "release" ]]; then
        build_flags+=(--release)
        log_info "Building in RELEASE mode..."
    else
        log_info "Building in DEBUG mode..."
    fi

    if cargo build --bins "${build_flags[@]}"; then
        log_success "Build completed successfully"
        return 0
    else
        log_error "Build failed"
        return 1
    fi
}

# ============================================================================
# Example Runner Functions
# ============================================================================

get_binary_path() {
    local example_name=$1
    if [[ "$BUILD_MODE" == "release" ]]; then
        echo "$SCRIPT_DIR/target/release/$example_name"
    else
        echo "$SCRIPT_DIR/target/debug/$example_name"
    fi
}

run_with_timeout() {
    local timeout_sec=$1
    local name=$2
    local binary=$3

    log_info "Running: $binary"

    # Run command in background
    "$binary" &
    local pid=$!

    # Wait with timeout
    local count=0
    while kill -0 $pid 2>/dev/null; do
        if [[ $count -ge $timeout_sec ]]; then
            log_error "$name timed out after ${timeout_sec}s"
            kill -TERM $pid 2>/dev/null || true
            sleep 0.5
            kill -KILL $pid 2>/dev/null || true
            return 124 # Timeout exit code
        fi
        sleep 1
        ((count++))
    done

    # Get exit code
    wait $pid
    return $?
}

run_standalone_example() {
    local example_name=$1
    local description=$2

    print_header "Test: $example_name - $description"

    local binary
    binary=$(get_binary_path "$example_name")

    if [[ ! -x "$binary" ]]; then
        log_error "Binary not found or not executable: $binary"
        FAILED_TESTS+=("$example_name")
        return 1
    fi

    # Set up environment
    if [[ "$VERBOSE" == true ]]; then
        export RUST_LOG=debug
    else
        # Don't set RUST_LOG - let examples run without logging overhead
        # This is especially important for performance benchmarks like ping_pong
        unset RUST_LOG
    fi

    # Run with timeout
    if run_with_timeout "$TIMEOUT_SECONDS" "$example_name" "$binary"; then
        log_success "$example_name completed successfully"
        PASSED_TESTS+=("$example_name")
        return 0
    else
        local exit_code=$?
        log_error "$example_name failed with exit code $exit_code"
        FAILED_TESTS+=("$example_name")
        return 1
    fi
}

# ============================================================================
# Multi-Process Coordination Test
# ============================================================================

run_leader_worker_test() {
    print_header "Test: Leader + Workers Coordination"

    local leader_binary
    local worker_binary
    leader_binary=$(get_binary_path "leader")
    worker_binary=$(get_binary_path "worker")

    if [[ ! -x "$leader_binary" ]] || [[ ! -x "$worker_binary" ]]; then
        log_error "Leader or worker binary not found"
        FAILED_TESTS+=("leader_worker")
        return 1
    fi

    # Set up environment
    if [[ "$VERBOSE" == true ]]; then
        export RUST_LOG=debug
    else
        # Don't set RUST_LOG - let examples run without logging overhead
        # This is especially important for performance benchmarks like ping_pong
        unset RUST_LOG
    fi

    local leader_endpoint="tcp://127.0.0.1:5555"

    # Start leader
    log_info "Starting leader on $leader_endpoint..."
    "$leader_binary" &
    local leader_pid=$!
    BACKGROUND_PIDS+=("$leader_pid")

    # Wait for leader to start
    log_info "Waiting for leader to initialize..."
    sleep 2

    # Check if leader is still running
    if ! kill -0 $leader_pid 2>/dev/null; then
        log_error "Leader process died during startup"
        FAILED_TESTS+=("leader_worker")
        return 1
    fi

    # Start worker 1
    log_info "Starting worker 1 (RANK=0)..."
    LEADER_ENDPOINT="$leader_endpoint" RANK=0 "$worker_binary" &
    local worker1_pid=$!
    BACKGROUND_PIDS+=("$worker1_pid")

    # Small delay between workers
    sleep 0.5

    # Start worker 2
    log_info "Starting worker 2 (RANK=1)..."
    LEADER_ENDPOINT="$leader_endpoint" RANK=1 "$worker_binary" &
    local worker2_pid=$!
    BACKGROUND_PIDS+=("$worker2_pid")

    log_info "All processes started. Waiting for all to complete gracefully..."
    log_info "Leader PID: $leader_pid, Worker PIDs: $worker1_pid, $worker2_pid"

    # Wait for all processes to complete with timeout
    local count=0
    local max_wait=$TIMEOUT_SECONDS
    while kill -0 $leader_pid 2>/dev/null || kill -0 $worker1_pid 2>/dev/null || kill -0 $worker2_pid 2>/dev/null; do
        if [[ $count -ge $max_wait ]]; then
            log_error "Leader/worker test timed out after ${max_wait}s"
            kill -TERM $leader_pid $worker1_pid $worker2_pid 2>/dev/null || true
            sleep 1
            kill -KILL $leader_pid $worker1_pid $worker2_pid 2>/dev/null || true
            FAILED_TESTS+=("leader_worker")
            return 1
        fi
        sleep 1
        ((count++))
    done

    # Get exit codes
    wait $leader_pid 2>/dev/null
    local leader_exit=$?
    wait $worker1_pid 2>/dev/null
    local worker1_exit=$?
    wait $worker2_pid 2>/dev/null
    local worker2_exit=$?

    # Remove from background tracking since we handled cleanup
    BACKGROUND_PIDS=()

    if [[ $leader_exit -eq 0 ]] && [[ $worker1_exit -eq 0 ]] && [[ $worker2_exit -eq 0 ]]; then
        log_success "Leader/worker coordination test completed successfully"
        log_info "Leader exited with code $leader_exit"
        log_info "Worker 1 exited with code $worker1_exit"
        log_info "Worker 2 exited with code $worker2_exit"
        PASSED_TESTS+=("leader_worker")
        return 0
    else
        log_error "Leader/worker test failed"
        log_error "Leader exit code: $leader_exit"
        log_error "Worker 1 exit code: $worker1_exit"
        log_error "Worker 2 exit code: $worker2_exit"
        FAILED_TESTS+=("leader_worker")
        return 1
    fi
}

# ============================================================================
# Summary Report
# ============================================================================

print_summary() {
    print_header "Test Summary"

    local total=$((${#PASSED_TESTS[@]} + ${#FAILED_TESTS[@]}))

    echo -e "${BOLD}Total Tests:${NC} $total"
    echo -e "${GREEN}Passed:${NC} ${#PASSED_TESTS[@]}"
    echo -e "${RED}Failed:${NC} ${#FAILED_TESTS[@]}"
    echo ""

    if [[ ${#PASSED_TESTS[@]} -gt 0 ]]; then
        echo -e "${GREEN}Passed Tests:${NC}"
        for test in "${PASSED_TESTS[@]}"; do
            echo -e "  ${GREEN}✓${NC} $test"
        done
        echo ""
    fi

    if [[ ${#FAILED_TESTS[@]} -gt 0 ]]; then
        echo -e "${RED}Failed Tests:${NC}"
        for test in "${FAILED_TESTS[@]}"; do
            echo -e "  ${RED}✗${NC} $test"
        done
        echo ""
    fi

    print_separator

    if [[ ${#FAILED_TESTS[@]} -eq 0 ]]; then
        echo -e "${GREEN}${BOLD}All tests passed!${NC}"
        return 0
    else
        echo -e "${RED}${BOLD}Some tests failed.${NC}"
        return 1
    fi
}

# ============================================================================
# Main Function
# ============================================================================

show_usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Build and run all active message examples, including multi-process coordination tests.

OPTIONS:
    -v, --verbose       Enable verbose logging (RUST_LOG=debug)
    -d, --debug         Build and run in debug mode (default is release)
    -t, --timeout N     Set timeout for tests in seconds (default: 30)
    -h, --help          Show this help message

EXAMPLES:
    $0                  # Run all tests with release builds (default)
    $0 --verbose        # Run with debug logging
    $0 --debug          # Run debug builds instead of release
    $0 --timeout 60     # Use 60 second timeout

TESTS INCLUDED:
    Standalone Examples:
        - hello_world: Basic request-response pattern
        - ping_pong: Latency measurement
        - zmq_ping_pong: ZMQ pub-sub benchmark
        - cohort_parallel: Cohort-based parallel operations

    Multi-Process Coordination:
        - leader + 2 workers: Distributed cohort coordination

EOF
}

main() {
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -d|--debug)
                BUILD_MODE="debug"
                shift
                ;;
            -t|--timeout)
                TIMEOUT_SECONDS="$2"
                shift 2
                ;;
            -h|--help)
                show_usage
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done

    print_header "Active Message Examples Test Suite"
    echo "Build Mode: $BUILD_MODE"
    echo "Timeout: ${TIMEOUT_SECONDS}s"
    echo "Verbose: $VERBOSE"

    # Build examples
    if ! build_examples; then
        log_error "Build failed. Exiting."
        exit 1
    fi

    # Run standalone examples (continue even if tests fail)
    run_standalone_example "hello_world" "Basic request-response pattern" || true
    run_standalone_example "ping_pong" "Latency measurement with ACK-only responses" || true
    run_standalone_example "zmq_ping_pong" "Raw ZMQ pub-sub benchmark" || true
    run_standalone_example "cohort_parallel" "Cohort-based parallel operations" || true

    # Run multi-process coordination test
    run_leader_worker_test || true

    # Print summary and exit with appropriate code
    if print_summary; then
        exit 0
    else
        exit 1
    fi
}

# ============================================================================
# Execute
# ============================================================================

main "$@"
