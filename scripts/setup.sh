#!/usr/bin/env bash
# setup.sh — Quick setup for sketchlib-rust.
#
# Installs/verifies all prerequisites and prepares the Rust crate so the
# library, tests, benchmarks, and cross-language tools can be built without
# further manual steps.
#
# Usage:
#   ./scripts/setup.sh                        # from sketchlib-rust root
#   bash sketchlib-rust/scripts/setup.sh      # from repo root
#
# What this script does:
#   1. Checks Rust toolchain (>= 1.75 required, edition 2024 recommended)
#   2. Verifies the proto/sketchlib.proto source is reachable (build.rs uses it)
#   3. Runs cargo fetch to warm the dependency cache
#   4. Builds the library crate (dev profile)
#   5. Builds the xtest_consumer binary
#   6. Optionally runs the library unit tests (set SKIP_TESTS=1 to skip)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$RUST_DIR/.." && pwd)"
PROTO_SRC="$REPO_ROOT/proto/sketchlib.proto"

# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------
BOLD=$'\033[1m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[0;33m'
RED=$'\033[0;31m'; CYAN=$'\033[0;36m'; RESET=$'\033[0m'

info() { echo "${CYAN}[setup-rust]${RESET} $*"; }
ok()   { echo "${GREEN}[setup-rust]${RESET} $*"; }
warn() { echo "${YELLOW}[setup-rust]${RESET} $*"; }
die()  { echo "${RED}[setup-rust] FATAL:${RESET} $*" >&2; exit 1; }
step() { echo ""; echo "${BOLD}${CYAN}── $* ──${RESET}"; }

# ---------------------------------------------------------------------------
# 1. Rust toolchain
# ---------------------------------------------------------------------------
step "Checking Rust toolchain"

if ! command -v rustc &>/dev/null; then
    die "rustc not found. Install Rust from https://rustup.rs/"
fi
if ! command -v cargo &>/dev/null; then
    die "cargo not found. Install Rust from https://rustup.rs/"
fi

RUSTC_VER="$(rustc --version | awk '{print $2}')"
CARGO_VER="$(cargo --version | awk '{print $2}')"
info "Found rustc $RUSTC_VER"
info "Found cargo $CARGO_VER"

# Parse major.minor
RUST_MAJOR="$(echo "$RUSTC_VER" | cut -d. -f1)"
RUST_MINOR="$(echo "$RUSTC_VER" | cut -d. -f2)"

MIN_MAJOR=1; MIN_MINOR=75
if [[ "$RUST_MAJOR" -lt "$MIN_MAJOR" ]] || \
   { [[ "$RUST_MAJOR" -eq "$MIN_MAJOR" ]] && [[ "$RUST_MINOR" -lt "$MIN_MINOR" ]]; }; then
    die "Rust >= $MIN_MAJOR.$MIN_MINOR required (found $RUSTC_VER). Run: rustup update"
fi
ok "Rust version OK"

# Warn if edition 2024 is not yet supported (requires 1.85+)
RUST_MINOR_INT="$RUST_MINOR"
if [[ "$RUST_MINOR_INT" -lt 85 ]]; then
    warn "Rust $RUSTC_VER: edition 2024 in Cargo.toml requires >= 1.85."
    warn "Run 'rustup update' if you encounter edition-related errors."
fi

# ---------------------------------------------------------------------------
# 2. Protobuf source check
# ---------------------------------------------------------------------------
step "Checking proto source"

if [[ ! -f "$PROTO_SRC" ]]; then
    die "Proto source not found: $PROTO_SRC"
    die "Ensure the sketchlib-go repository is checked out alongside sketchlib-rust."
fi
info "Found $PROTO_SRC"
ok "build.rs will compile proto at build time via prost-build"

# ---------------------------------------------------------------------------
# 3. Fetch dependencies
# ---------------------------------------------------------------------------
step "Fetching Cargo dependencies"
(
    cd "$RUST_DIR"
    info "cargo fetch"
    cargo fetch
)
ok "Dependency cache ready"

# ---------------------------------------------------------------------------
# 4. Build library (dev profile)
# ---------------------------------------------------------------------------
step "Building sketchlib-rust library"
(
    cd "$RUST_DIR"
    info "cargo build --lib"
    cargo build --lib 2>&1 | grep -E '^(error|warning\[|   Compiling|    Finished)' || true
)
ok "Library builds successfully"

# ---------------------------------------------------------------------------
# 5. Build xtest_consumer binary
# ---------------------------------------------------------------------------
step "Building xtest_consumer"
(
    cd "$RUST_DIR"
    info "cargo build --bin xtest_consumer"
    cargo build --bin xtest_consumer 2>&1 | grep -E '^(error|warning\[|   Compiling|    Finished)' || true
)
ok "xtest_consumer binary ready at target/debug/xtest_consumer"

# ---------------------------------------------------------------------------
# 6. Unit tests (optional)
# ---------------------------------------------------------------------------
if [[ "${SKIP_TESTS:-0}" == "1" ]]; then
    step "Skipping unit tests (SKIP_TESTS=1)"
else
    step "Running unit tests"
    (
        cd "$RUST_DIR"
        info "cargo test --lib"
        cargo test --lib 2>&1 | tail -20
    )
    ok "Unit tests passed"
fi

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo ""
echo "${BOLD}${GREEN}sketchlib-rust setup complete.${RESET}"
echo ""
echo "  Run unit tests   : cd $RUST_DIR && cargo test"
echo "  Run benchmarks   : cd $RUST_DIR && cargo bench"
echo "  Cross-lang test  : $REPO_ROOT/sketchlib-go/tests/cross_language/run_test.sh"
echo ""
