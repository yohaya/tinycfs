#!/usr/bin/env bash
# scripts/post-change.sh
# Triggered automatically by Claude Code after every conversation turn
# that modified Rust source files. Pushes to GitHub (triggering CI) and
# runs the local simulation smoke-test.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# ── 1. Check whether any tracked Rust / Cargo files changed ──────────────────
CHANGED=$(git diff --name-only HEAD 2>/dev/null || true)
STAGED=$(git diff --cached --name-only 2>/dev/null || true)
ALL_CHANGED=$(printf '%s\n%s' "$CHANGED" "$STAGED")

RUST_CHANGED=$(echo "$ALL_CHANGED" | grep -E '\.(rs|toml|lock)$' | head -1 || true)

if [[ -z "$RUST_CHANGED" ]]; then
    echo "[post-change] No Rust files changed — skipping simulation and push."
    exit 0
fi

echo "[post-change] Rust source changes detected."

# ── 2. Build to catch compile errors early ────────────────────────────────────
echo "[post-change] Building…"
PKG_CONFIG_PATH="/usr/local/lib/pkgconfig" cargo build 2>&1
echo "[post-change] Build OK."

# ── 3. Git push (triggers GitHub Actions CI build) ────────────────────────────
if git remote get-url origin &>/dev/null; then
    if git status --porcelain | grep -q '^[MADRC]'; then
        echo "[post-change] Uncommitted changes — commit before pushing."
    else
        echo "[post-change] Pushing to origin/main…"
        git push origin main 2>&1 && echo "[post-change] Push OK. GitHub Actions build triggered." \
            || echo "[post-change] Push failed (offline?). Skipping."
    fi
fi

# ── 4. Run a short simulation smoke-test ─────────────────────────────────────
echo "[post-change] Running 15s simulation (5 nodes, 1000 rps, 5-10ms delay)…"
PKG_CONFIG_PATH="/usr/local/lib/pkgconfig" \
  cargo run --bin sim -- \
    --nodes 5 \
    --rps 1000 \
    --delay-min-ms 5 \
    --delay-max-ms 10 \
    --duration-secs 15 \
    --warmup-secs 2 \
  && echo "[post-change] Simulation PASSED." \
  || { echo "[post-change] Simulation FAILED — see output above."; exit 1; }
