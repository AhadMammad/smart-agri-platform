#!/usr/bin/env bash
# One entry point for the x86_64 Ubuntu VM the platform is verified on.
#
# The Hadoop images are amd64-only, so a phase is only "done" once it has run
# there. This wraps the SSH options, the remote repo path and the output
# filtering so none of it has to be retyped or rediscovered.
#
# Connection details come from .env.vm (gitignored — this repo is public).
# Copy .env.vm.example and fill it in.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_VM="$ROOT/.env.vm"

REMOTE_DIR="${VM_REPO_DIR:-~/smart-agri-platform}"
CONTAINER_PREFIX="${VM_CONTAINER_PREFIX:-smart-agri}"

die() { printf '\033[31merror\033[0m %s\n' "$1" >&2; exit 1; }

usage() {
  cat <<'USAGE'
usage: scripts/vm.sh <command> [args]

  status              container status across all three profiles
  sync                git pull --ff-only, then rebuild the ETL image
  exec '<sh>'         run a shell command in the repo directory
  make <target> [...] sync, then run a make target
  logs <service> [n]  last n lines (default 40) of a container's logs
  ch '<sql>'          run a query against the ClickHouse analytics database
  psql '<sql>'        run a query against the Postgres source database
  tunnel [service]    SSH-forward a VM service to localhost (hive|postgres|clickhouse|superset; default hive)
                       foreground — Ctrl-C to close

Connection details are read from .env.vm — see .env.vm.example.
USAGE
}

# --- connection --------------------------------------------------------------

load_connection() {
  if [ -f "$ENV_VM" ]; then
    set -a
    # The path is computed and the file is gitignored, so it cannot be followed.
    # shellcheck source=/dev/null
    . "$ENV_VM"
    set +a
  fi

  [ -n "${VM_HOST:-}" ] || die "VM_HOST is not set. Copy .env.vm.example to .env.vm and fill it in."
  [ -n "${VM_USER:-}" ] || die "VM_USER is not set (in $ENV_VM or the environment)."

  SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new)
  if [ -n "${VM_SSH_KEY:-}" ]; then
    # Expand a leading ~ that .env.vm quoting would otherwise keep literal.
    VM_SSH_KEY="${VM_SSH_KEY/#\~/$HOME}"
    [ -f "$VM_SSH_KEY" ] || die "VM_SSH_KEY points at $VM_SSH_KEY, which does not exist."
    SSH_OPTS+=(-i "$VM_SSH_KEY")
  fi
}

# Run a command on the VM, already inside the repo directory.
# $REMOTE_DIR is expanded here on purpose: the caller's command is not, because
# it is passed through as a single word for the remote shell to interpret.
# shellcheck disable=SC2029
remote() {
  ssh "${SSH_OPTS[@]}" "${VM_USER}@${VM_HOST}" "cd $REMOTE_DIR && $1"
}

# Strip what buries the actual result: the echoed `docker run` line, the
# structured JSON logs, timestamped container output, and the remote
# /etc/bash.bashrc complaining about PS1 in a non-interactive shell.
denoise() {
  grep -vE '^docker run|^\{|^[0-9]{4}-[0-9]{2}-[0-9]{2}|bash.bashrc.*PS1' || true
}

# --- commands ----------------------------------------------------------------

cmd_status() {
  remote "docker compose --env-file .env -f docker/docker-compose.yml \
            --profile core --profile orchestration --profile bi ps \
            --format 'table {{.Service}}\t{{.Status}}'"
}

cmd_sync() {
  echo "--- pulling and rebuilding the ETL image on $VM_HOST ---"
  remote "git pull --ff-only && make build-etl" 2>&1 | tail -5
}

cmd_exec() {
  [ $# -ge 1 ] || die "exec needs a command: scripts/vm.sh exec 'make ps'"
  remote "$1"
}

cmd_make() {
  [ $# -ge 1 ] || die "make needs a target: scripts/vm.sh make doctor"
  case "$1" in
    logs|psql|ch)
      die "'make $1' blocks on stdin or never exits. Use 'scripts/vm.sh $1' instead."
      ;;
    clean)
      die "'make clean' waits on a typed confirmation and deletes the lake and both databases. Run it on the VM by hand if that is really what you want."
      ;;
  esac
  cmd_sync
  echo "--- make $* ---"
  remote "make $*" 2>&1 | denoise
}

cmd_logs() {
  [ $# -ge 1 ] || die "logs needs a service: scripts/vm.sh logs namenode"
  remote "docker logs --tail ${2:-40} ${CONTAINER_PREFIX}-${1}-1" 2>&1
}

cmd_ch() {
  [ $# -ge 1 ] || die "ch needs a query: scripts/vm.sh ch 'SELECT 1'"
  remote "docker exec ${CONTAINER_PREFIX}-clickhouse-1 clickhouse-client \
            --user \"\$(grep -E '^CLICKHOUSE_USER=' .env | cut -d= -f2)\" \
            --password \"\$(grep -E '^CLICKHOUSE_PASSWORD=' .env | cut -d= -f2)\" \
            --database \"\$(grep -E '^CLICKHOUSE_DB=' .env | cut -d= -f2)\" \
            -q \"$1\""
}

cmd_psql() {
  [ $# -ge 1 ] || die "psql needs a query: scripts/vm.sh psql 'SELECT 1'"
  remote "docker exec ${CONTAINER_PREFIX}-postgres-1 psql \
            -U \"\$(grep -E '^POSTGRES_USER=' .env | cut -d= -f2)\" \
            -d \"\$(grep -E '^POSTGRES_DB=' .env | cut -d= -f2)\" \
            -qAt -c \"$1\""
}

# Forwards a single VM-local port to the same port on 127.0.0.1 here, over
# SSH. Bound to loopback only, so nothing on the LAN can ride the tunnel.
# Runs in the foreground (-N: no remote command) so an accidental orphaned
# background ssh process can't outlive the terminal it was started from —
# Ctrl-C is the only way to close it, which is deliberate.
cmd_tunnel() {
  local service="${1:-hive}" port
  case "$service" in
    hive)       port=10000 ;;
    postgres)   port=5432  ;;
    clickhouse) port=8123  ;;
    superset)   port=8088  ;;
    *) die "unknown tunnel service: $service (expected hive|postgres|clickhouse|superset)" ;;
  esac
  echo "tunnelling 127.0.0.1:${port} -> ${VM_HOST}:${port} (${service} on the VM) — Ctrl-C to close"
  exec ssh "${SSH_OPTS[@]}" \
    -N -T \
    -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
    -L "127.0.0.1:${port}:localhost:${port}" \
    "${VM_USER}@${VM_HOST}"
}

# --- dispatch ----------------------------------------------------------------

[ $# -ge 1 ] || { usage; exit 2; }

COMMAND="$1"; shift

case "$COMMAND" in
  -h|--help|help) usage; exit 0 ;;
  status|sync|exec|make|logs|ch|psql|tunnel)
    load_connection
    "cmd_$COMMAND" "$@"
    ;;
  *)
    printf '\033[31merror\033[0m unknown command: %s\n\n' "$COMMAND" >&2
    usage >&2
    exit 2
    ;;
esac
