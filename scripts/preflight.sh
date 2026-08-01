#!/usr/bin/env bash
# Check the host can actually run the stack before anything is pulled or built.
# Every failure here has cost someone an hour at some point.
set -uo pipefail

FAILURES=0

pass() { printf '  \033[32m[ok]\033[0m   %s\n' "$1"; }
warn() { printf '  \033[33m[warn]\033[0m %s\n' "$1"; }
fail() { printf '  \033[31m[fail]\033[0m %s\n' "$1"; FAILURES=$((FAILURES + 1)); }

echo "smart-agri-platform preflight"
echo

# --- architecture ------------------------------------------------------------
echo "architecture"
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64)
    pass "$ARCH — the Hadoop images are amd64-only and will run natively"
    ;;
  *)
    fail "$ARCH — apache/hadoop publishes amd64 only; HDFS would run under
         emulation. Run this stack on the x86_64 Ubuntu VM instead."
    ;;
esac
echo

# --- docker ------------------------------------------------------------------
echo "docker"
if command -v docker >/dev/null 2>&1; then
  pass "docker $(docker --version | awk '{print $3}' | tr -d ,)"
else
  fail "docker not found"
fi

if docker compose version >/dev/null 2>&1; then
  pass "compose $(docker compose version --short)"
else
  fail "docker compose v2 not found"
fi

if docker info >/dev/null 2>&1; then
  pass "docker daemon reachable"
else
  fail "cannot reach the docker daemon — is it running, and is your user in the docker group?"
fi

# DockerOperator spawns sibling containers through this socket.
if [ -S /var/run/docker.sock ]; then
  if [ -r /var/run/docker.sock ] && [ -w /var/run/docker.sock ]; then
    pass "/var/run/docker.sock is readable and writable (DockerOperator will work)"
  else
    fail "/var/run/docker.sock exists but is not writable by $(id -un).
         Fix with: sudo usermod -aG docker $(id -un) && newgrp docker"
  fi
else
  fail "/var/run/docker.sock missing — DockerOperator cannot launch task containers"
fi
echo

# --- resources ---------------------------------------------------------------
echo "resources"
if [ -r /proc/meminfo ]; then
  MEM_GB=$(awk '/MemTotal/ {printf "%d", $2/1024/1024}' /proc/meminfo)
  if [ "$MEM_GB" -ge 24 ]; then
    pass "${MEM_GB} GB RAM"
  elif [ "$MEM_GB" -ge 16 ]; then
    warn "${MEM_GB} GB RAM — start profiles selectively rather than using make up-all"
  else
    fail "${MEM_GB} GB RAM — the full stack needs roughly 16 GB free"
  fi
else
  warn "cannot read /proc/meminfo — skipping the memory check"
fi

CPUS=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 0)
[ "$CPUS" -ge 4 ] && pass "${CPUS} CPUs" || warn "${CPUS} CPUs — expect slow Hadoop startup"

DISK_GB=$(df -BG --output=avail . 2>/dev/null | tail -1 | tr -dc '0-9')
if [ -n "${DISK_GB:-}" ]; then
  [ "$DISK_GB" -ge 30 ] \
    && pass "${DISK_GB} GB free disk" \
    || fail "${DISK_GB} GB free disk — images alone need roughly 20 GB"
else
  warn "cannot determine free disk space"
fi
echo

# --- ports -------------------------------------------------------------------
echo "host ports"
check_port() {
  local port=$1 label=$2
  if command -v ss >/dev/null 2>&1 && ss -ltn "sport = :$port" 2>/dev/null | grep -q LISTEN; then
    fail "$port already in use (needed for $label)"
  else
    pass "$port free ($label)"
  fi
}
check_port 5432 "postgres"
check_port 8123 "clickhouse http"
check_port 9000 "clickhouse native"
check_port 9870 "hdfs namenode ui"
check_port 9864 "hdfs datanode ui"
check_port 9083 "hive metastore"
check_port 8080 "airflow"
check_port 8088 "superset"
echo

# --- result ------------------------------------------------------------------
if [ "$FAILURES" -eq 0 ]; then
  echo "preflight passed — run: make env && make build && make up-all"
  exit 0
fi

echo "preflight found $FAILURES problem(s); resolve them before starting the stack."
exit 1
