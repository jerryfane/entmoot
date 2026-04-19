#!/bin/sh
# Entmoot installer.
# Usage:
#   Install:    curl -fsSL https://raw.githubusercontent.com/jerryfane/entmoot/main/install.sh | sh
#   Uninstall:  curl -fsSL https://raw.githubusercontent.com/jerryfane/entmoot/main/install.sh | sh -s uninstall
#
# Behaviour:
#   1. If a GitHub Release tarball exists for this OS/arch, download it.
#   2. Otherwise, fall back to cloning the repo and building from source
#      (requires Go and git).
#   3. Install entmootd to $HOME/.entmoot/bin and add that dir to PATH
#      via the user's shell rc file.
#
# Does NOT install Pilot Protocol. Pilot is a prerequisite; install the
# matching patched fork with:
#   curl -fsSL https://raw.githubusercontent.com/jerryfane/pilotprotocol/main/install.sh | sh
# Installing the upstream pilotprotocol.network version will work wire-wise
# but lacks the reliability patches these Entmoot releases rely on.

set -eu

REPO="jerryfane/entmoot"
PILOT_TAG="v1.7.2"
INSTALL_DIR="${ENTMOOT_HOME:-$HOME/.entmoot}"
BIN_DIR="$INSTALL_DIR/bin"

# --- Uninstall -----------------------------------------------------------

if [ "${1:-}" = "uninstall" ]; then
    echo ""
    echo "  Uninstalling Entmoot..."
    rm -rf "$INSTALL_DIR"
    echo "  Removed $INSTALL_DIR"
    echo "  (You may want to remove Entmoot's PATH export from your shell rc file.)"
    echo ""
    exit 0
fi

# --- Detect platform ----------------------------------------------------

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
case "$ARCH" in
    x86_64)  ARCH="amd64" ;;
    aarch64) ARCH="arm64" ;;
    arm64)   ARCH="arm64" ;;
    *)       echo "Error: unsupported architecture: $ARCH"; exit 1 ;;
esac

case "$OS" in
    linux|darwin) ;;
    *) echo "Error: unsupported OS: $OS"; exit 1 ;;
esac

echo ""
echo "  Entmoot installer"
echo "  Platform: ${OS}/${ARCH}"
echo "  Target:   ${BIN_DIR}"
echo ""

# --- Working directory --------------------------------------------------

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

# --- Try latest GitHub release ------------------------------------------

TAG=""
if command -v curl >/dev/null 2>&1; then
    TAG=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null \
          | grep '"tag_name"' | head -1 | cut -d'"' -f4 || true)
fi

if [ -n "$TAG" ]; then
    ARCHIVE="entmoot-${OS}-${ARCH}.tar.gz"
    URL="https://github.com/${REPO}/releases/download/${TAG}/${ARCHIVE}"
    echo "  Downloading ${TAG}..."
    if curl -fsSL "$URL" -o "$TMPDIR/$ARCHIVE" 2>/dev/null; then
        tar -xzf "$TMPDIR/$ARCHIVE" -C "$TMPDIR"
    else
        echo "  No prebuilt release for ${OS}/${ARCH}; falling back to source build."
        TAG=""
    fi
fi

# --- Fallback: build from source ----------------------------------------

if [ -z "$TAG" ]; then
    echo "  Building from source..."
    command -v go >/dev/null 2>&1 || {
        echo "  Error: Go is required to build from source."
        echo "  Install Go from https://go.dev/dl/ and retry."
        exit 1
    }
    command -v git >/dev/null 2>&1 || {
        echo "  Error: git is required to build from source."
        exit 1
    }
    echo "  Cloning entmoot..."
    git clone --depth 1 "https://github.com/${REPO}.git" "$TMPDIR/src" >/dev/null 2>&1
    echo "  Cloning Pilot fork (jerryfane/pilotprotocol main)..."
    git clone --depth 1 --branch main \
        https://github.com/jerryfane/pilotprotocol.git \
        "$TMPDIR/src/repos/pilotprotocol" >/dev/null 2>&1
    echo "  Building entmootd..."
    (cd "$TMPDIR/src/src" && CGO_ENABLED=0 go build -o "$TMPDIR/entmootd" ./cmd/entmootd)
fi

# --- Install -------------------------------------------------------------

mkdir -p "$BIN_DIR"
cp "$TMPDIR/entmootd" "$BIN_DIR/entmootd"
chmod 755 "$BIN_DIR/entmootd"
echo "  Installed: $BIN_DIR/entmootd"

# --- PATH setup ----------------------------------------------------------

case ":$PATH:" in
    *":${BIN_DIR}:"*)
        ;;
    *)
        SHELL_NAME=$(basename "${SHELL:-/bin/sh}" 2>/dev/null || echo "sh")
        case "$SHELL_NAME" in
            zsh)  RC="$HOME/.zshrc" ;;
            bash) RC="$HOME/.bashrc" ;;
            *)    RC="$HOME/.profile" ;;
        esac
        if [ ! -f "$RC" ] || ! grep -q "${BIN_DIR}" "$RC" 2>/dev/null; then
            {
                echo ""
                echo "# Entmoot"
                echo "export PATH=\"${BIN_DIR}:\$PATH\""
            } >> "$RC"
            echo "  Added ${BIN_DIR} to PATH in ${RC}"
        fi
        ;;
esac

# --- Done ----------------------------------------------------------------

echo ""
echo "Installed:"
echo "  entmootd  ${BIN_DIR}/entmootd"
echo ""
echo "Next steps:"
echo ""
echo "  # activate the new PATH in this shell:"
echo "  export PATH=\"${BIN_DIR}:\$PATH\""
echo ""
echo "  # install the patched Pilot Protocol (prerequisite, separate):"
echo "  curl -fsSL https://raw.githubusercontent.com/jerryfane/pilotprotocol/main/install.sh | sh"
echo ""
echo "  # verify:"
echo "  entmootd info"
echo ""
