#!/usr/bin/env bash
set -euo pipefail

# Resolve the directory where this script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Configuration
DIRECTORY="${1:-$SCRIPT_DIR}"
PORT=8089
APP_FILE="Home.py"
REQUIREMENTS_FILE="requirements.txt"
APP_NAME="dea-session-4"

# Derived paths
APP_PATH="${DIRECTORY}/${APP_FILE}"
VENV_PATH="${DIRECTORY}/.venv"
REQUIREMENTS_PATH="${DIRECTORY}/${REQUIREMENTS_FILE}"
PID_FILE="${DIRECTORY}/${APP_NAME}.pid"
LOG_DIR="${DIRECTORY}/log"
LOG_FILE="${LOG_DIR}/${APP_NAME}.log"

# Ensure log directory and file exist
mkdir -p "$LOG_DIR"
touch "$LOG_FILE"

# Logging
log() {
    local level=$1; shift
    printf '[%s] [%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$level" "$*" | tee -a "$LOG_FILE"
}

# Check if a port is in use — works across distros
port_in_use() {
    if command -v ss &>/dev/null; then
        ss -tln 2>/dev/null | grep -q ":${PORT}\b"
    elif command -v netstat &>/dev/null; then
        netstat -tln 2>/dev/null | grep -q ":${PORT}\b"
    elif command -v lsof &>/dev/null; then
        lsof -iTCP:"$PORT" -sTCP:LISTEN &>/dev/null
    elif [ -d /proc/net ]; then
        local hex_port
        hex_port=$(printf '%04X' "$PORT")
        grep -qi ":${hex_port} " /proc/net/tcp 2>/dev/null
    else
        return 1
    fi
}

# Get PIDs listening on our port
get_port_pids() {
    if command -v ss &>/dev/null; then
        ss -tlnp 2>/dev/null | grep ":${PORT}\b" | grep -oP 'pid=\K[0-9]+' || true
    elif command -v lsof &>/dev/null; then
        lsof -iTCP:"$PORT" -sTCP:LISTEN -t 2>/dev/null || true
    elif command -v fuser &>/dev/null; then
        fuser "$PORT/tcp" 2>/dev/null | tr -s ' ' '\n' || true
    fi
}

# Stop any existing instance
stop_existing() {
    if [ -f "$PID_FILE" ]; then
        local old_pid
        old_pid=$(cat "$PID_FILE")
        if [ -n "$old_pid" ] && kill -0 "$old_pid" 2>/dev/null; then
            log "INFO" "Stopping existing process (PID $old_pid)"
            kill "$old_pid" 2>/dev/null || true
            sleep 2
            kill -9 "$old_pid" 2>/dev/null || true
        fi
        rm -f "$PID_FILE"
    fi

    local pids
    pids=$(get_port_pids)
    if [ -n "$pids" ]; then
        log "INFO" "Killing process(es) on port $PORT: $pids"
        echo "$pids" | xargs -r kill -9 2>/dev/null || true
        sleep 1
    fi
}

# Find python3 binary
find_python() {
    if command -v python3 &>/dev/null; then
        echo "python3"
    elif command -v python &>/dev/null; then
        echo "python"
    else
        log "ERROR" "No python3 or python found in PATH"
        return 1
    fi
}

# Setup virtual environment
setup_venv() {
    local python_bin
    python_bin=$(find_python) || return 1

    if [ -d "$VENV_PATH" ] && [ -f "${VENV_PATH}/bin/pip" ]; then
        log "INFO" "Updating existing virtual environment..."
        if "${VENV_PATH}/bin/pip" install -q -r "$REQUIREMENTS_PATH" >> "$LOG_FILE" 2>&1; then
            return 0
        fi
        log "WARNING" "Update failed, rebuilding venv..."
    fi

    log "INFO" "Creating fresh virtual environment..."
    rm -rf "$VENV_PATH"
    "$python_bin" -m venv "$VENV_PATH" || { log "ERROR" "Failed to create venv"; return 1; }

    log "INFO" "Upgrading pip..."
    "${VENV_PATH}/bin/pip" install -q -U pip >> "$LOG_FILE" 2>&1

    log "INFO" "Installing dependencies..."
    if ! "${VENV_PATH}/bin/pip" install -q -r "$REQUIREMENTS_PATH" >> "$LOG_FILE" 2>&1; then
        log "ERROR" "Failed to install requirements. Last output:"
        "${VENV_PATH}/bin/pip" install -r "$REQUIREMENTS_PATH" 2>&1 | tail -20 | tee -a "$LOG_FILE"
        return 1
    fi

    log "INFO" "Virtual environment ready"
    return 0
}

# Start the streamlit application
start_app() {
    log "INFO" "Starting streamlit on port $PORT..."

    nohup "${VENV_PATH}/bin/streamlit" run "$APP_PATH" \
        --server.port "$PORT" \
        --server.headless true \
        --server.address 0.0.0.0 \
        >> "$LOG_FILE" 2>&1 &

    local app_pid=$!
    echo "$app_pid" > "$PID_FILE"
    log "INFO" "Launched with PID $app_pid (pid file: $PID_FILE)"

    local waited=0
    while [ $waited -lt 15 ]; do
        if port_in_use; then
            log "INFO" "Port $PORT is active — application running"
            return 0
        fi
        if ! kill -0 "$app_pid" 2>/dev/null; then
            log "ERROR" "Process $app_pid exited prematurely. Last log lines:"
            tail -10 "$LOG_FILE" | while IFS= read -r line; do log "ERROR" "  $line"; done
            rm -f "$PID_FILE"
            return 1
        fi
        sleep 1
        waited=$((waited + 1))
    done

    if kill -0 "$app_pid" 2>/dev/null; then
        log "WARNING" "Port $PORT not confirmed after 15s but process $app_pid is alive"
        return 0
    fi

    log "ERROR" "Application failed to start"
    rm -f "$PID_FILE"
    return 1
}

# ============================================================
# Main
# ============================================================
log "INFO" "=== Setup starting ==="
log "INFO" "Directory: $DIRECTORY"
log "INFO" "Port: $PORT"
log "INFO" "App: $APP_FILE"
log "INFO" "Log: $LOG_FILE"
log "INFO" "PID file: $PID_FILE"

cd "$DIRECTORY" || { log "ERROR" "Cannot cd to $DIRECTORY"; exit 1; }

stop_existing
setup_venv || exit 1
start_app || exit 1

log "INFO" "=== Setup complete ==="
