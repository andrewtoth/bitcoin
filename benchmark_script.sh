#!/bin/bash

set -ex

# Function to print usage
usage() {
  echo "Usage: $0 --storage-path /path/to/storage --bench-name bench_name --runs runs --stopatheight stopatheight --dbcache dbcache --printtoconsole printtoconsole --preseed preseed --crash-interval-seconds crash_interval_seconds --commit-list commit_list"
  exit 1
}

while [[ "$#" -gt 0 ]]; do
  case $1 in
    --storage-path) STORAGE_PATH="$2"; shift ;;
    --bench-name) BASE_NAME="$2"; shift ;;
    --runs) RUNS="$2"; shift ;;
    --stopatheight) STOP_AT_HEIGHT="$2"; shift ;;
    --dbcache) DBCACHE="$2"; shift ;;
    --printtoconsole) PRINT_TO_CONSOLE="$2"; shift ;;
    --preseed) PRESEED="$2"; shift ;;
    --crash-interval-seconds) CRASH_INTERVAL_SECONDS="$2"; shift ;;
    --commit-list) COMMIT_LIST="$2"; shift ;;
    *) echo "Unknown parameter passed: $1"; usage ;;
  esac
  shift
done

# Ensure all required arguments are provided
if [ -z "$STORAGE_PATH" ] || [ -z "$BASE_NAME" ] || [ -z "$RUNS" ] || [ -z "$STOP_AT_HEIGHT" ] || [ -z "$DBCACHE" ] || [ -z "$PRINT_TO_CONSOLE" ] || [ -z "$PRESEED" ] || [ -z "$CRASH_INTERVAL_SECONDS" ] || [ -z "$COMMIT_LIST" ]; then
  usage
fi

# Setup environment variables
START_DATE=$(date +%Y%m%d%H%M%S)
export DATA_DIR="$STORAGE_PATH/BitcoinData"
mkdir -p "$DATA_DIR"
export PROJECT_DIR="$STORAGE_PATH/${BASE_NAME}_${START_DATE}"
mkdir -p "$PROJECT_DIR"

export LOG_FILE="$PROJECT_DIR/benchmark.log"
export JSON_FILE="$PROJECT_DIR/benchmark.json"

prepare_function() {
  echo "Starting prepare step at commit $COMMIT at $(date)" | tee -a "$LOG_FILE"

  killall bitcoind vmstat || true

  git checkout "$COMMIT" || { echo "Failed to checkout commit $COMMIT" | tee -a "$LOG_FILE"; exit 1; }
  COMMIT_MSG=$(git log --format=%B -n 1)
  echo "Preparing commit: $COMMIT: $COMMIT_MSG" | tee -a "$LOG_FILE"

  # Build Bitcoin Core
  cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_UTIL=OFF -DBUILD_TX=OFF -DBUILD_TESTS=OFF -DENABLE_WALLET=OFF -DINSTALL_MAN=OFF
  cmake --build build -j$(nproc) || { echo "Build failed at commit $COMMIT" | tee -a "$LOG_FILE"; exit 1; }

  # Cleanup data directory and caches
  rm -rf "$DATA_DIR"/*
  sync
  echo 3 > /proc/sys/vm/drop_caches
  echo "Cleared data directory and dropped caches at commit $COMMIT at $(date)" | tee -a "$LOG_FILE"

  # Preseed bitcoind if option is enabled
  if [ "$PRESEED" = true ]; then
    echo "Starting bitcoind with large dbcache for preseed at commit: $COMMIT: '$COMMIT_MSG'  at $(date)" | tee -a "$LOG_FILE"
    ./build/src/bitcoind -datadir="$DATA_DIR" -dbcache=10000 -stopatheight=1 -printtoconsole=1
    echo "Preseed complete at $(date)" | tee -a "$LOG_FILE"
  fi

  echo "Finished prepare step at commit $COMMIT at $(date)" | tee -a "$LOG_FILE"
}
export -f prepare_function

# Run and rash bitcoind periodically and restart it until it exits successfully
run_and_crash_bitcoind_periodically() {
  local BITCOIND_CMD="./build/src/bitcoind -datadir=$DATA_DIR -stopatheight=$STOP_AT_HEIGHT -dbcache=$DBCACHE -printtoconsole=$PRINT_TO_CONSOLE -maxmempool=5 -blocksonly"

  while true; do
    echo "Starting bitcoind process with commit $COMMIT at $(date)" | tee -a "$LOG_FILE"
    $BITCOIND_CMD &
    BITCOIND_PID=$!

    # Crash bitcoind at intervals if CRASH_INTERVAL_SECONDS is set
    if [[ "$CRASH_INTERVAL_SECONDS" -gt 0 ]]; then
      echo "Crashing bitcoind every $CRASH_INTERVAL_SECONDS seconds." | tee -a "$LOG_FILE"
      sleep "$CRASH_INTERVAL_SECONDS"
      echo "Killing bitcoind (PID: $BITCOIND_PID) with SIGKILL." | tee -a "$LOG_FILE"
      kill -9 "$BITCOIND_PID"
    fi

    wait "$BITCOIND_PID"
    EXIT_CODE=$?

    if [ "$EXIT_CODE" -eq 0 ]; then
      echo "bitcoind finished successfully with exit code 0" | tee -a "$LOG_FILE"
      break
    else
      echo "bitcoind crashed with exit code $EXIT_CODE, restarting..." | tee -a "$LOG_FILE"
    fi
  done
}
export -f run_and_crash_bitcoind_periodically

run_bitcoind_with_monitoring() {
  echo "run_bitcoind_with_monitoring:" | tee -a "$LOG_FILE"

  COMMIT_MSG=$(git log --format=%B -n 1)
  echo "Measuring commit: $COMMIT: '$COMMIT_MSG'" | tee -a "$LOG_FILE"

  # Start vmstat monitoring
  vmstat 1 > "$PROJECT_DIR/vmstat_${COMMIT}_$(date +%Y%m%d%H%M%S).log" &
  VMSTAT_PID=$!

  run_and_crash_bitcoind_periodically

  echo "VMSTAT monitoring at commit $COMMIT at $(date)" | tee -a "$LOG_FILE"
  vmstat -s | tee -a "$LOG_FILE"
  kill $VMSTAT_PID
}
export -f run_bitcoind_with_monitoring

cleanup_function() {
  echo "cleanup_function:" | tee -a "$LOG_FILE"

  {
    # Log data directory stats
    echo "Data directory size after benchmark at commit $COMMIT: $(du -sh "$DATA_DIR" | cut -f1)"
    echo "Number of files in data directory: $(find "$DATA_DIR" -type f | wc -l)"
  } | tee -a "$LOG_FILE"

  echo "Starting bitcoind for $COMMIT at $(date)" | tee -a "$LOG_FILE"
  ./build/src/bitcoind -datadir="$DATA_DIR" -daemon -dbcache="$DBCACHE" -printtoconsole=0 && sleep 10

  {
    echo "Benchmarking gettxoutsetinfo at $(date)"
    time ./build/src/bitcoin-cli -datadir="$DATA_DIR" gettxoutsetinfo
  } 2>&1 | tee -a "$LOG_FILE"

  echo "Stopping bitcoind for $COMMIT at $(date)" | tee -a "$LOG_FILE"
  ./build/src/bitcoin-cli -datadir="$DATA_DIR" stop && sleep 10
  killall bitcoind vmstat || true

  echo "Ended $COMMIT: $COMMIT_MSG at $(date)" | tee -a "$LOG_FILE"
}
export -f cleanup_function

run_benchmarks() {
  hyperfine \
    --shell=bash \
    --runs "$RUNS" \
    --show-output \
    --export-json "$JSON_FILE" \
    --parameter-list COMMIT "$COMMIT_LIST" \
    --prepare 'COMMIT={COMMIT} prepare_function' \
    --cleanup 'COMMIT={COMMIT} cleanup_function' \
    "COMMIT={COMMIT} run_bitcoind_with_monitoring"
}

# Example usage of the benchmark script with parameter names
#./benchmark_script.sh \
#  --storage-path /mnt/my_storage \
#  --bench-name rocksdb-bench \
#  --runs 1 \
#  --stopatheight 100 \
#  --dbcache 1450 \
#  --printtoconsole 1 \
#  --preseed true \
#  --crash-interval-seconds 100 \
#  --commit-list "5f66faa959f38f25c7d93dc9d784f39cf47dd7ec,7aab3c3de38c1d824ee3ecb8d1fcd0cbffab810a,997e4012f94ef396c727686e4b163338fdbbbc77"

run_benchmarks
