#!/bin/sh
# Entmoot installer.
# Usage:
#   Install:   curl -fsSL https://raw.githubusercontent.com/jerryfane/entmoot/main/install.sh | sh
#   Uninstall: curl -fsSL https://raw.githubusercontent.com/jerryfane/entmoot/main/install.sh | sh -s uninstall
set -eu

REPO="jerryfane/entmoot"
INSTALL_DIR="${ENTMOOT_HOME:-$HOME/.entmoot}"
BIN_DIR="$INSTALL_DIR/bin"

if [ "${1:-}" = "uninstall" ]; then
    echo "Uninstalling Entmoot..."
    rm -rf "$INSTALL_DIR"
    echo "Removed $INSTALL_DIR"
    echo "You may want to remove Entmoot's PATH export from your shell rc file."
    exit 0
fi

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
case "$ARCH" in
    x86_64) ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *) echo "Error: unsupported architecture: $ARCH" >&2; exit 1 ;;
esac
case "$OS" in
    linux|darwin) ;;
    *) echo "Error: unsupported OS: $OS" >&2; exit 1 ;;
esac

echo "Entmoot installer"
echo "Platform: ${OS}/${ARCH}"
echo "Target:   ${BIN_DIR}"

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT
TAG=""
LOCAL_BINARY=0
if [ -n "${ENTMOOTD_LOCAL_BIN:-}" ]; then
    if [ ! -f "$ENTMOOTD_LOCAL_BIN" ]; then
        echo "Error: ENTMOOTD_LOCAL_BIN does not exist: $ENTMOOTD_LOCAL_BIN" >&2
        exit 1
    fi
    cp "$ENTMOOTD_LOCAL_BIN" "$TMPDIR/entmootd"
    chmod 755 "$TMPDIR/entmootd"
    LOCAL_BINARY=1
fi

if [ "$LOCAL_BINARY" -eq 0 ] && command -v curl >/dev/null 2>&1; then
    TAG=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null \
        | grep '"tag_name"' | head -1 | cut -d'"' -f4 || true)
fi
if [ "$LOCAL_BINARY" -eq 0 ] && [ -n "$TAG" ]; then
    ARCHIVE="entmoot-${OS}-${ARCH}.tar.gz"
    URL="https://github.com/${REPO}/releases/download/${TAG}/${ARCHIVE}"
    echo "Downloading ${TAG}..."
    if curl -fsSL "$URL" -o "$TMPDIR/$ARCHIVE" 2>/dev/null; then
        tar -xzf "$TMPDIR/$ARCHIVE" -C "$TMPDIR"
    else
        echo "No prebuilt release for ${OS}/${ARCH}; falling back to source build."
        TAG=""
    fi
fi
if [ "$LOCAL_BINARY" -eq 0 ] && [ -z "$TAG" ]; then
    command -v go >/dev/null 2>&1 || {
        echo "Error: Go is required to build from source." >&2
        exit 1
    }
    command -v git >/dev/null 2>&1 || {
        echo "Error: git is required to build from source." >&2
        exit 1
    }
    echo "Building from source..."
    git clone --depth 1 "https://github.com/${REPO}.git" "$TMPDIR/src" >/dev/null 2>&1
    (cd "$TMPDIR/src/src" && CGO_ENABLED=0 go build -o "$TMPDIR/entmootd" ./cmd/entmootd)
fi

mkdir -p "$BIN_DIR"
cp "$TMPDIR/entmootd" "$BIN_DIR/.entmootd.tmp.$$"
chmod 755 "$BIN_DIR/.entmootd.tmp.$$"
mv -f "$BIN_DIR/.entmootd.tmp.$$" "$BIN_DIR/entmootd"

write_runtime_var() {
    printf "%s='" "$1"
    printf "%s" "$2" | sed "s/'/'\\\\''/g"
    printf "'\n"
}
{
    write_runtime_var ENTMOOT_BIN "$BIN_DIR/entmootd"
    write_runtime_var ENTMOOT_DATA "$INSTALL_DIR"
    write_runtime_var ENTMOOT_IDENTITY "$INSTALL_DIR/identity.json"
    write_runtime_var ENTMOOT_LISTEN_PORT "${ENTMOOT_LISTEN_PORT:-1004}"
} > "$INSTALL_DIR/runtime.env"

cat > "$INSTALL_DIR/entmoot" <<'EOF'
#!/bin/sh
set -eu
WRAPPER=$0
while [ -L "$WRAPPER" ]; do
    LINK=$(readlink "$WRAPPER")
    case "$LINK" in
        /*) WRAPPER=$LINK ;;
        *) WRAPPER=$(dirname "$WRAPPER")/$LINK ;;
    esac
done
INSTALL_DIR=$(CDPATH= cd -P "$(dirname "$WRAPPER")" && pwd)
RUNTIME_ENV=${ENTMOOT_RUNTIME_ENV:-$INSTALL_DIR/runtime.env}
if [ -f "$RUNTIME_ENV" ]; then
    # shellcheck disable=SC1090
    . "$RUNTIME_ENV"
fi
ENTMOOT_BIN=${ENTMOOT_BIN:-$INSTALL_DIR/bin/entmootd}
ENTMOOT_DATA=${ENTMOOT_DATA:-$INSTALL_DIR}
ENTMOOT_IDENTITY=${ENTMOOT_IDENTITY:-$ENTMOOT_DATA/identity.json}
ENTMOOT_LISTEN_PORT=${ENTMOOT_LISTEN_PORT:-1004}
exec "$ENTMOOT_BIN" -identity "$ENTMOOT_IDENTITY" -data "$ENTMOOT_DATA" -listen-port "$ENTMOOT_LISTEN_PORT" "$@"
EOF
chmod 755 "$INSTALL_DIR/entmoot"
ln -sf ../entmoot "$BIN_DIR/entmoot"

case ":$PATH:" in
    *":${BIN_DIR}:"*) ;;
    *)
        SHELL_NAME=$(basename "${SHELL:-/bin/sh}" 2>/dev/null || echo sh)
        case "$SHELL_NAME" in
            zsh) RC="$HOME/.zshrc" ;;
            bash) RC="$HOME/.bashrc" ;;
            *) RC="$HOME/.profile" ;;
        esac
        if [ ! -f "$RC" ] || ! grep -Fq "$BIN_DIR" "$RC" 2>/dev/null; then
            printf '\n# Entmoot\nexport PATH="%s:$PATH"\n' "$BIN_DIR" >> "$RC"
        fi
        ;;
esac

echo "Installed: $BIN_DIR/entmootd"
echo "Activate PATH: export PATH=\"$BIN_DIR:\$PATH\""
echo "Initialize: entmoot info"
