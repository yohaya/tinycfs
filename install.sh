#!/usr/bin/env bash
# install.sh — Install tinycfs as a systemd service
#
# Usage:
#   sudo ./install.sh                              # install with defaults
#   sudo ./install.sh --mountpoint /srv/tinycfs    # custom mount point
#   sudo ./install.sh --config /path/to/my.conf    # use existing config
#   sudo ./install.sh --data-dir /data/tinycfs      # custom data directory
#   sudo ./install.sh --uninstall                  # remove tinycfs

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
INSTALL_BIN="/usr/local/bin/tinycfs"
CONFIG_FILE="/etc/tinycfs/tinycfs.conf"
DATA_DIR="/var/lib/tinycfs"
MOUNT_POINT="/mnt/tinycfs"
SYSTEMD_UNIT="/etc/systemd/system/tinycfs.service"
BINARY_SRC=""
UNINSTALL=false

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mountpoint)  MOUNT_POINT="$2";  shift 2 ;;
    --config)      CONFIG_FILE="$2";  shift 2 ;;
    --data-dir)    DATA_DIR="$2";     shift 2 ;;
    --binary)      BINARY_SRC="$2";   shift 2 ;;
    --uninstall)   UNINSTALL=true;    shift   ;;
    -h|--help)
      cat <<'EOF'
Usage: sudo ./install.sh [OPTIONS]

Options:
  --mountpoint DIR   FUSE mount point            (default: /mnt/tinycfs)
  --config FILE      Config file path            (default: /etc/tinycfs/tinycfs.conf)
  --data-dir DIR     SQLite persistence directory (default: /var/lib/tinycfs)
  --binary FILE      Path to tinycfs binary      (default: auto-detected from bin/)
  --uninstall        Stop, disable, and remove tinycfs (config/data preserved)
  -h, --help         Show this help

Examples:
  sudo ./install.sh
  sudo ./install.sh --mountpoint /srv/cluster --data-dir /data/tinycfs
  sudo ./install.sh --uninstall
EOF
      exit 0
      ;;
    *)
      echo "ERROR: Unknown option: $1" >&2
      echo "       Run '$0 --help' for usage." >&2
      exit 1
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Require root ──────────────────────────────────────────────────────────────
if [[ $EUID -ne 0 ]]; then
  echo "ERROR: This script must be run as root." >&2
  echo "       Try: sudo $0 $*" >&2
  exit 1
fi

# ── Uninstall ─────────────────────────────────────────────────────────────────
if $UNINSTALL; then
  echo "==> Stopping tinycfs service..."
  systemctl stop tinycfs 2>/dev/null && echo "    Stopped." || echo "    (was not running)"

  echo "==> Disabling tinycfs service..."
  systemctl disable tinycfs 2>/dev/null && echo "    Disabled." || true

  echo "==> Unmounting $MOUNT_POINT..."
  fusermount3 -u "$MOUNT_POINT" 2>/dev/null \
    || fusermount -u "$MOUNT_POINT" 2>/dev/null \
    || umount "$MOUNT_POINT" 2>/dev/null \
    || echo "    (was not mounted)"

  echo "==> Removing files..."
  rm -f "$SYSTEMD_UNIT"
  rm -f "$INSTALL_BIN"
  systemctl daemon-reload

  echo ""
  echo "tinycfs has been uninstalled."
  echo "Config ($CONFIG_FILE) and data ($DATA_DIR) were NOT removed."
  echo "Remove them manually if no longer needed."
  exit 0
fi

# ── Detect architecture ───────────────────────────────────────────────────────
ARCH="$(uname -m)"
if [[ "$ARCH" != "x86_64" ]]; then
  echo "WARNING: Pre-built binaries in bin/ are x86_64 only." >&2
  echo "         Use --binary /path/to/your/tinycfs for $ARCH." >&2
fi

# ── Auto-detect binary ────────────────────────────────────────────────────────
if [[ -z "$BINARY_SRC" ]]; then
  # Prefer the static musl binary (no glibc dependency, works everywhere)
  STATIC_BIN="$SCRIPT_DIR/bin/tinycfs-linux-x86_64"
  if [[ -f "$STATIC_BIN" ]]; then
    BINARY_SRC="$STATIC_BIN"
  elif [[ -f "$SCRIPT_DIR/target/release/tinycfs" ]]; then
    # Fall back to a locally compiled binary
    BINARY_SRC="$SCRIPT_DIR/target/release/tinycfs"
    echo "INFO: Using locally compiled binary ($BINARY_SRC)."
    echo "      For a statically linked binary, run: cargo build --release"
  else
    echo "ERROR: No binary found." >&2
    echo "       Expected: $STATIC_BIN" >&2
    echo "       Build one: cargo build --release" >&2
    echo "       Or specify: --binary /path/to/tinycfs" >&2
    exit 1
  fi
fi

if [[ ! -x "$BINARY_SRC" ]]; then
  chmod +x "$BINARY_SRC"
fi

# ── Print plan ────────────────────────────────────────────────────────────────
echo "==> Installing tinycfs"
echo ""
echo "    Binary source : $BINARY_SRC"
echo "    Binary target : $INSTALL_BIN"
echo "    Config file   : $CONFIG_FILE"
echo "    Data directory: $DATA_DIR"
echo "    Mount point   : $MOUNT_POINT"
echo "    Systemd unit  : $SYSTEMD_UNIT"
echo ""

# ── Install binary ────────────────────────────────────────────────────────────
echo "==> Installing binary..."
install -Dm755 "$BINARY_SRC" "$INSTALL_BIN"
echo "    $INSTALL_BIN"

# ── Create directories ────────────────────────────────────────────────────────
echo "==> Creating directories..."
mkdir -p "$(dirname "$CONFIG_FILE")" "$DATA_DIR" "$MOUNT_POINT"
echo "    $(dirname "$CONFIG_FILE")/"
echo "    $DATA_DIR/"
echo "    $MOUNT_POINT/"

# ── Install config (only if missing) ─────────────────────────────────────────
echo "==> Installing config..."
if [[ -f "$CONFIG_FILE" ]]; then
  echo "    $CONFIG_FILE already exists — not overwritten."
else
  if [[ -f "$SCRIPT_DIR/tinycfs.conf.example" ]]; then
    cp "$SCRIPT_DIR/tinycfs.conf.example" "$CONFIG_FILE"
    echo "    Template installed to $CONFIG_FILE"
  else
    # Write a minimal config if the example is not present
    cat > "$CONFIG_FILE" <<'CONF'
// tinycfs.conf — edit before starting the service
{
  cluster_name: "mycluster",
  local_node: "node1",           // change to this machine's node name
  data_dir: "/var/lib/tinycfs",
  algorithm: "raft",
  nodes: [
    { name: "node1", ip: "192.168.1.10", port: 7788 },
    { name: "node2", ip: "192.168.1.11", port: 7788 },
    { name: "node3", ip: "192.168.1.12", port: 7788 },
  ],
}
CONF
    echo "    Minimal config written to $CONFIG_FILE"
  fi
  echo ""
  echo "    *** IMPORTANT: Edit $CONFIG_FILE before starting the service ***"
  echo "        - Set cluster_name (same on every node)"
  echo "        - Set local_node to THIS machine's name"
  echo "        - Set nodes[] with IPs of all cluster members"
  echo ""
fi

# ── Enable user_allow_other in /etc/fuse.conf ─────────────────────────────────
echo "==> Configuring FUSE..."
FUSE_CONF="/etc/fuse.conf"
if [[ ! -f "$FUSE_CONF" ]]; then
  echo "user_allow_other" > "$FUSE_CONF"
  echo "    Created $FUSE_CONF with user_allow_other"
elif ! grep -q "^user_allow_other" "$FUSE_CONF"; then
  echo "user_allow_other" >> "$FUSE_CONF"
  echo "    Added user_allow_other to $FUSE_CONF"
else
  echo "    $FUSE_CONF already has user_allow_other"
fi

# ── Write systemd unit ────────────────────────────────────────────────────────
echo "==> Installing systemd unit..."
cat > "$SYSTEMD_UNIT" <<UNIT
[Unit]
Description=TinyCFS — Tiny Cluster File System
Documentation=https://github.com/yohaya/tinycfs
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
# Ensure the mount point exists before starting
ExecStartPre=/bin/mkdir -p ${MOUNT_POINT}
ExecStart=${INSTALL_BIN} ${MOUNT_POINT} \\
    --config ${CONFIG_FILE} \\
    --allow-other \\
    --no-auto-unmount
# Unmount on stop; '-' prefix means failure is not fatal (already unmounted)
ExecStop=-/bin/fusermount3 -u ${MOUNT_POINT}
ExecStop=-/bin/fusermount -u ${MOUNT_POINT}
Restart=on-failure
RestartSec=10s
TimeoutStartSec=30s
TimeoutStopSec=15s
StandardOutput=journal
StandardError=journal
SyslogIdentifier=tinycfs

[Install]
WantedBy=multi-user.target
UNIT
echo "    $SYSTEMD_UNIT"

# ── Enable service ────────────────────────────────────────────────────────────
echo "==> Enabling tinycfs service..."
systemctl daemon-reload
systemctl enable tinycfs
echo "    Service enabled (will start automatically on boot)"

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "================================================================"
echo "  tinycfs installed successfully"
echo "================================================================"
echo ""
echo "  Next steps:"
echo ""
echo "  1. Edit the config (if not already done):"
echo "       \$EDITOR $CONFIG_FILE"
echo ""
echo "  2. Start the service:"
echo "       systemctl start tinycfs"
echo ""
echo "  3. Check status:"
echo "       systemctl status tinycfs"
echo ""
echo "  4. View logs:"
echo "       journalctl -u tinycfs -f"
echo ""
echo "  To uninstall:"
echo "       sudo $0 --uninstall"
echo ""
