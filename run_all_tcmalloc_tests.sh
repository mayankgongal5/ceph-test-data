#!/bin/bash
###############################################################################
# RGW tcmalloc Profiling — Full Automated Test Suite
# Tests: 3 sizes × 7 cache values × 3 workloads = 63 combinations
#
# Usage:  cd /path/to/ceph/build && bash /root/run_all_tcmalloc_tests.sh
# Assumes: You're in the ceph build/ directory with vstart.sh available
###############################################################################

set -euo pipefail

# ─── Configuration ───────────────────────────────────────────────────────────
BASE="/root/ceph-test-result"
TESTDATA="/root/rgw-testdata"
ENDPOINT="http://localhost:8000"
S5CMD="s5cmd --endpoint-url $ENDPOINT --no-verify-ssl"
NUM_OBJECTS=50
MIXED_START=$((NUM_OBJECTS + 1))
MIXED_END=$((NUM_OBJECTS * 2))

declare -A SIZE_MAP
SIZE_MAP[small]="64K"
SIZE_MAP[medium]="2M"
SIZE_MAP[large]="24M"

declare -A BUCKET_MAP
BUCKET_MAP[small]="small-objects"
BUCKET_MAP[medium]="medium-objects"
BUCKET_MAP[large]="large-objects"

declare -A FILE_MAP
FILE_MAP[small]="obj_64k"
FILE_MAP[medium]="obj_2m"
FILE_MAP[large]="obj_24m"

CACHES=(8 16 32 64 128 256 512)
WORKLOADS=(writeonly readonly mixed)
SIZES=(small medium large)

# ─── Helper Functions ────────────────────────────────────────────────────────

log() {
    echo ""
    echo "========================================================================"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "========================================================================"
}

find_asok() {
    # Find the RGW admin socket
    ASOK=$(ls /var/run/ceph/radosgw.*.asok 2>/dev/null | head -1)
    if [[ -z "$ASOK" ]]; then
        ASOK=$(ls ./out/radosgw.*.asok 2>/dev/null | head -1)
    fi
    if [[ -z "$ASOK" ]]; then
        echo "ERROR: Cannot find radosgw admin socket!" >&2
        return 1
    fi
    echo "$ASOK"
}

collect_heap_stats() {
    local output_file="$1"
    local label="$2"
    local asok
    asok=$(find_asok)

    echo "--- $label ---" >> "$output_file"
    echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')" >> "$output_file"
    echo "" >> "$output_file"

    # Full heap stats
    echo "=== Full heap stats ===" >> "$output_file"
    ./bin/ceph --admin-daemon "$asok" heap stats 2>/dev/null >> "$output_file" || echo "heap stats failed" >> "$output_file"
    echo "" >> "$output_file"

    # Summary line for quick parsing
    echo "=== Key metrics ===" >> "$output_file"
    ./bin/ceph --admin-daemon "$asok" heap stats 2>/dev/null | \
        grep -iE "thread.cache|central.cache|transfer.cache|page.heap|in.use|free.in|unmapped|total" >> "$output_file" 2>/dev/null || true
    echo "" >> "$output_file"
    echo "────────────────────────────────────────" >> "$output_file"
    echo "" >> "$output_file"
}

wait_for_rgw() {
    log "Waiting for RGW to be ready..."
    local retries=0
    while ! curl -s -o /dev/null "$ENDPOINT" 2>/dev/null; do
        sleep 2
        retries=$((retries + 1))
        if [[ $retries -ge 30 ]]; then
            echo "ERROR: RGW did not start within 60 seconds"
            return 1
        fi
    done
    echo "RGW is up."
    sleep 2
}

stop_cluster() {
    log "Stopping cluster..."
    ../src/stop.sh 2>/dev/null || true
    sleep 3
    # Kill any lingering processes
    pkill -f radosgw 2>/dev/null || true
    pkill -f ceph-mon 2>/dev/null || true
    pkill -f ceph-osd 2>/dev/null || true
    sleep 2
}

start_cluster() {
    local cache_bytes="$1"
    log "Starting cluster with TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES=$cache_bytes"

    export TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES="$cache_bytes"

    # Clean stale data to avoid mon crashes

    MON=1 OSD=1 MDS=0 MGR=0 RGW=1 ../src/vstart.sh -n -d 2>&1 | tail -5

    wait_for_rgw
}

# ─── Prepare Test Data ───────────────────────────────────────────────────────

prepare_test_files() {
    log "Preparing test data files in $TESTDATA"
    mkdir -p "$TESTDATA"

    if [[ ! -f "$TESTDATA/obj_64k" ]]; then
        dd if=/dev/urandom of="$TESTDATA/obj_64k" bs=64K count=1 2>/dev/null
        echo "Created obj_64k (64 KiB)"
    fi
    if [[ ! -f "$TESTDATA/obj_2m" ]]; then
        dd if=/dev/urandom of="$TESTDATA/obj_2m" bs=1M count=2 2>/dev/null
        echo "Created obj_2m (2 MiB)"
    fi
    if [[ ! -f "$TESTDATA/obj_24m" ]]; then
        dd if=/dev/urandom of="$TESTDATA/obj_24m" bs=1M count=24 2>/dev/null
        echo "Created obj_24m (24 MiB)"
    fi
}

# ─── Workload Functions ──────────────────────────────────────────────────────

run_writeonly() {
    local size="$1"
    local result_dir="$2"
    local bucket="${BUCKET_MAP[$size]}"
    local file="${FILE_MAP[$size]}"
    local output="$result_dir/results.txt"

    log "WRITE-ONLY: size=$size bucket=$bucket"

    collect_heap_stats "$output" "BEFORE write-only"

    echo "Uploading $NUM_OBJECTS objects..."
    for i in $(seq 1 $NUM_OBJECTS); do
        echo "cp $TESTDATA/$file s3://$bucket/obj_$i"
    done | $S5CMD run 2>&1 | tail -3

    collect_heap_stats "$output" "AFTER write-only ($NUM_OBJECTS objects uploaded)"

    echo "Write-only complete. Results → $output"
}

run_readonly() {
    local size="$1"
    local result_dir="$2"
    local bucket="${BUCKET_MAP[$size]}"
    local output="$result_dir/results.txt"

    log "READ-ONLY: size=$size bucket=$bucket"

    # Objects should already exist from writeonly phase
    collect_heap_stats "$output" "BEFORE read-only"

    echo "Downloading $NUM_OBJECTS objects..."
    for i in $(seq 1 $NUM_OBJECTS); do
        echo "cp s3://$bucket/obj_$i /dev/null"
    done | $S5CMD run 2>&1 | tail -3

    collect_heap_stats "$output" "AFTER read-only ($NUM_OBJECTS objects downloaded)"

    echo "Read-only complete. Results → $output"
}

run_mixed() {
    local size="$1"
    local result_dir="$2"
    local bucket="${BUCKET_MAP[$size]}"
    local file="${FILE_MAP[$size]}"
    local output="$result_dir/results.txt"

    log "MIXED (write+read+delete): size=$size bucket=$bucket"

    collect_heap_stats "$output" "BEFORE mixed"

    echo "Mixed WRITE: uploading objects ${MIXED_START}–${MIXED_END}..."
    for i in $(seq $MIXED_START $MIXED_END); do
        echo "cp $TESTDATA/$file s3://$bucket/obj_$i"
    done | $S5CMD run 2>&1 | tail -3

    collect_heap_stats "$output" "AFTER mixed-write"

    echo "Mixed READ: downloading objects ${MIXED_START}–${MIXED_END}..."
    for i in $(seq $MIXED_START $MIXED_END); do
        echo "cp s3://$bucket/obj_$i /dev/null"
    done | $S5CMD run 2>&1 | tail -3

    collect_heap_stats "$output" "AFTER mixed-read"

    echo "Mixed DELETE: removing objects ${MIXED_START}–${MIXED_END}..."
    for i in $(seq $MIXED_START $MIXED_END); do
        echo "rm s3://$bucket/obj_$i"
    done | $S5CMD run 2>&1 | tail -3

    collect_heap_stats "$output" "AFTER mixed-delete (final)"

    echo "Mixed complete. Results → $output"
}

# ─── Main Test Loop ──────────────────────────────────────────────────────────

main() {
    local start_time
    start_time=$(date +%s)

    log "RGW tcmalloc Profiling — Starting Full Test Suite"
    echo "Results directory: $BASE"
    echo "Sizes: ${SIZES[*]}"
    echo "Cache values (MiB): ${CACHES[*]}"
    echo "Workloads: ${WORKLOADS[*]}"
    echo "Objects per workload: $NUM_OBJECTS"

    prepare_test_files

    for cache in "${CACHES[@]}"; do
        local cache_bytes=$((cache * 1024 * 1024))

        log "═══ CACHE SIZE: ${cache} MiB ($cache_bytes bytes) ═══"

        # Fresh cluster for each cache value
        stop_cluster
        start_cluster "$cache_bytes"

        for size in "${SIZES[@]}"; do
            local bucket="${BUCKET_MAP[$size]}"

            log "── Size: $size | Cache: ${cache}mb ──"

            # Create bucket for this size
            $S5CMD mb "s3://$bucket" 2>/dev/null || true

            # Record test metadata
            local meta_file="$BASE/$size/${cache}mb/test_info.txt"
            cat > "$meta_file" <<EOF
Test Metadata
─────────────
Date:       $(date)
Size:       $size (${SIZE_MAP[$size]})
Cache:      ${cache} MiB ($cache_bytes bytes)
Objects:    $NUM_OBJECTS per workload
Endpoint:   $ENDPOINT
EOF

            # Run all three workloads in order
            # writeonly first (creates objects), readonly second (reads them), mixed last
            run_writeonly "$size" "$BASE/$size/${cache}mb/writeonly"
            run_readonly  "$size" "$BASE/$size/${cache}mb/readonly"
            run_mixed     "$size" "$BASE/$size/${cache}mb/mixed"

            # Cleanup bucket for next iteration
            log "Cleaning up bucket $bucket..."
            $S5CMD rm "s3://$bucket/*" 2>/dev/null || true
            $S5CMD rb "s3://$bucket" 2>/dev/null || true
        done
    done

    stop_cluster

    local end_time
    end_time=$(date +%s)
    local duration=$(( end_time - start_time ))
    local minutes=$(( duration / 60 ))
    local seconds=$(( duration % 60 ))

    log "ALL TESTS COMPLETE in ${minutes}m ${seconds}s"
    echo ""
    echo "Results saved under: $BASE"
    echo ""
    echo "Quick view of all result files:"
    find "$BASE" -name "results.txt" | sort
}

# ─── Run ─────────────────────────────────────────────────────────────────────
main "$@"
