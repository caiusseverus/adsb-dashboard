#!/usr/bin/env bash
# ADS-B Dashboard — bare metal installer for Raspberry Pi OS (Debian-based)
# Usage: curl -fsSL https://raw.githubusercontent.com/caiusseverus/adsb-dashboard/main/install.sh | bash

set -euo pipefail

INSTALL_DIR="/opt/adsb-dashboard"
VENV_DIR="/var/lib/adsb-dashboard"   # venv lives outside /opt to avoid noexec issues
SERVICE_USER="adsb"
SERVICE_NAME="adsb-dashboard"
REPO_URL="https://github.com/caiusseverus/adsb-dashboard.git"
GO_REQUIRED_VERSION="1.24.2"
RADAR_CORE_BINARY_PATH="/usr/local/bin/radar-core"

# Colour helpers (silent if not a terminal)
_red()   { printf '\033[0;31m%s\033[0m\n' "$*"; }
_green() { printf '\033[0;32m%s\033[0m\n' "$*"; }
_bold()  { printf '\033[1m%s\033[0m\n' "$*"; }

_step() { echo; _bold "==> $*"; }
_die()  { _red "ERROR: $*"; exit 1; }

_version_ge() {
    # True when $1 >= $2
    [[ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -n1)" == "$1" ]]
}

_go_arch_suffix() {
    case "$(dpkg --print-architecture)" in
        amd64) echo "amd64" ;;
        arm64) echo "arm64" ;;
        armhf|armel) echo "armv6l" ;;
        *)
            _die "Unsupported architecture for Go install: $(dpkg --print-architecture)"
            ;;
    esac
}

_install_go_toolchain() {
    local arch_suffix go_tar_url tmp_tar
    arch_suffix="$(_go_arch_suffix)"
    go_tar_url="https://go.dev/dl/go${GO_REQUIRED_VERSION}.linux-${arch_suffix}.tar.gz"
    tmp_tar="/tmp/go${GO_REQUIRED_VERSION}.tar.gz"

    echo "  Installing Go ${GO_REQUIRED_VERSION} (${arch_suffix}) …"
    curl -fsSL "$go_tar_url" -o "$tmp_tar" || _die "Failed to download Go toolchain from $go_tar_url"
    rm -rf /usr/local/go
    tar -C /usr/local -xzf "$tmp_tar" || _die "Failed to extract Go toolchain"
    ln -sf /usr/local/go/bin/go /usr/local/bin/go
    ln -sf /usr/local/go/bin/gofmt /usr/local/bin/gofmt
    rm -f "$tmp_tar"
}

_ensure_go_toolchain() {
    local current_version
    if command -v go >/dev/null 2>&1; then
        current_version="$(go version | sed -n 's/^go version go\([0-9.]*\) .*/\1/p')"
    else
        current_version=""
    fi

    if [[ -n "$current_version" ]] && _version_ge "$current_version" "$GO_REQUIRED_VERSION"; then
        echo "  Go $current_version already installed"
        return
    fi

    if [[ -n "$current_version" ]]; then
        echo "  Go $current_version is older than required $GO_REQUIRED_VERSION"
    fi
    _install_go_toolchain
}

# Detect whether this is a fresh install or an update
IS_UPDATE=false
[[ -d "$INSTALL_DIR/.git" ]] && IS_UPDATE=true

# ---------------------------------------------------------------------------
# Step 1 — check we are running as root (or via sudo)
# ---------------------------------------------------------------------------
_step "Checking permissions"
[[ $EUID -eq 0 ]] || _die "This installer must be run as root. Try: sudo bash install.sh"

if [[ "$IS_UPDATE" == true ]]; then
    _bold "  Updating existing installation at $INSTALL_DIR"
else
    _bold "  Fresh installation to $INSTALL_DIR"
fi

# ---------------------------------------------------------------------------
# Step 2 — install system dependencies
# ---------------------------------------------------------------------------
_step "Installing system dependencies"

PACKAGES=(git curl nodejs npm)

apt-get update -qq
for pkg in "${PACKAGES[@]}"; do
    if ! dpkg -s "$pkg" &>/dev/null; then
        echo "  Installing $pkg …"
        apt-get install -y -qq "$pkg"
    else
        echo "  $pkg already installed"
    fi
done

# Install uv (Python package/project manager) into /usr/local/bin so it is
# on the system PATH for all users and survives repeated sudo invocations.
if ! [[ -x /usr/local/bin/uv ]]; then
    echo "  Installing uv …"
    curl -LsSf https://astral.sh/uv/install.sh | UV_INSTALL_DIR=/usr/local/bin sh
fi

# Verify Node version is >=18 (required for Vite)
NODE_MAJOR=$(node --version | sed 's/v\([0-9]*\).*/\1/')
if [[ "$NODE_MAJOR" -lt 18 ]]; then
    _die "Node.js 18+ is required. Detected: $(node --version). Install via: curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && apt-get install -y nodejs"
fi

# ---------------------------------------------------------------------------
# Step 3 — clone or update the repository
# ---------------------------------------------------------------------------
if [[ "$IS_UPDATE" == true ]]; then
    _step "Updating repository"
    # The repo is owned by the service user; mark it safe for the root user
    # running this script (git rejects operations on directories owned by
    # a different uid without this).
    git config --global --add safe.directory "$INSTALL_DIR"
    git -C "$INSTALL_DIR" pull --ff-only
else
    _step "Cloning repository to $INSTALL_DIR"
    git clone "$REPO_URL" "$INSTALL_DIR"
fi

# ---------------------------------------------------------------------------
# Step 4 — interactive .env setup
# ---------------------------------------------------------------------------
_step "Configuring environment"

ENV_EXAMPLE="$INSTALL_DIR/backend/.env.example"
ENV_FILE="$INSTALL_DIR/backend/.env"

[[ -f "$ENV_EXAMPLE" ]] || _die ".env.example not found at $ENV_EXAMPLE"

if [[ -f "$ENV_FILE" ]]; then
    echo "  $ENV_FILE already exists — skipping interactive setup."
    echo "  To reconfigure, delete $ENV_FILE and re-run this installer."

    # On update: check for keys in .env.example that are absent from .env.
    # New config options added since the last install are appended commented-out
    # so the service starts with defaults while the user can review and enable them.
    if [[ "$IS_UPDATE" == true ]]; then
        NEW_KEYS=()
        while IFS= read -r line; do
            # Active key: KEY=value
            if [[ "$line" =~ ^([A-Z_][A-Z0-9_]*)=(.*)$ ]]; then
                key="${BASH_REMATCH[1]}"
                val="${BASH_REMATCH[2]}"
                if ! grep -qE "^#?[[:space:]]*${key}=" "$ENV_FILE"; then
                    NEW_KEYS+=("${key}=${val}")
                fi
            # Commented-out optional key: # KEY=value
            elif [[ "$line" =~ ^#[[:space:]]([A-Z_][A-Z0-9_]*)=(.*)$ ]]; then
                key="${BASH_REMATCH[1]}"
                val="${BASH_REMATCH[2]}"
                if ! grep -qE "^#?[[:space:]]*${key}=" "$ENV_FILE"; then
                    NEW_KEYS+=("# ${key}=${val}")
                fi
            fi
        done < "$ENV_EXAMPLE"

        if [[ ${#NEW_KEYS[@]} -gt 0 ]]; then
            echo
            echo "  New configuration options found — appending to $ENV_FILE:"
            { echo; echo "# Options added by update on $(date)"; } >> "$ENV_FILE"
            for entry in "${NEW_KEYS[@]}"; do
                echo "    $entry"
                echo "$entry" >> "$ENV_FILE"
            done
            echo
            _bold "  Review new options in $ENV_FILE — defaults are active unless commented out."
        else
            echo "  No new configuration options since last install."
        fi
    fi
else
    echo "  You will be prompted for each configuration value."
    echo "  Press Enter to accept the default shown in [brackets]."
    echo

    # Parse .env.example: collect comment lines and KEY=value lines
    declare -a ENV_KEYS=()
    declare -A ENV_DEFAULTS=()
    declare -A ENV_COMMENTS=()
    declare -A ENV_REQUIRED=()

    current_comment=""
    while IFS= read -r line; do
        # Accumulate comment lines
        if [[ "$line" =~ ^# ]]; then
            # Strip leading '# ' for display
            current_comment+="${line#\# }"$'\n'
            continue
        fi

        # Blank line resets the comment block
        if [[ -z "$line" ]]; then
            current_comment=""
            continue
        fi

        # Uncommented KEY=value (active setting)
        if [[ "$line" =~ ^([A-Z_][A-Z0-9_]*)=(.*)$ ]]; then
            key="${BASH_REMATCH[1]}"
            val="${BASH_REMATCH[2]}"
            ENV_KEYS+=("$key")
            ENV_DEFAULTS["$key"]="$val"
            ENV_COMMENTS["$key"]="$current_comment"
            ENV_REQUIRED["$key"]="yes"
            current_comment=""
            continue
        fi

        # Commented-out # KEY=value (optional setting)
        if [[ "$line" =~ ^#\ ([A-Z_][A-Z0-9_]*)=(.*)$ ]]; then
            key="${BASH_REMATCH[1]}"
            val="${BASH_REMATCH[2]}"
            ENV_KEYS+=("$key")
            ENV_DEFAULTS["$key"]="$val"
            ENV_COMMENTS["$key"]="$current_comment"
            ENV_REQUIRED["$key"]="no"
            current_comment=""
            continue
        fi

        current_comment=""
    done < "$ENV_EXAMPLE"

    # Collect values interactively
    declare -A COLLECTED=()

    for key in "${ENV_KEYS[@]}"; do
        comment="${ENV_COMMENTS[$key]:-}"
        default="${ENV_DEFAULTS[$key]:-}"
        required="${ENV_REQUIRED[$key]:-no}"

        echo "---"
        [[ -n "$comment" ]] && echo "$comment"

        if [[ "$required" == "yes" ]]; then
            printf '%s (REQUIRED) [%s]: ' "$key" "$default"
        else
            printf '%s (optional) [%s]: ' "$key" "$default"
        fi

        # Secret / password fields: hidden input with confirmation
        if [[ "$key" =~ (PASS|SECRET|PASSWORD) ]]; then
            while true; do
                read -rs value1
                echo
                printf 'Confirm %s: ' "$key"
                read -rs value2
                echo
                if [[ "$value1" == "$value2" ]]; then
                    break
                fi
                _red "Values do not match, try again."
                printf '%s: ' "$key"
            done
            value="$value1"
        else
            read -r value
        fi

        # Use default for empty optional; re-prompt for empty required
        if [[ -z "$value" ]]; then
            if [[ "$required" == "yes" && -z "$default" ]]; then
                while [[ -z "$value" ]]; do
                    _red "  $key is required — please enter a value."
                    printf '%s: ' "$key"
                    read -r value
                done
            else
                value="$default"
            fi
        fi

        COLLECTED["$key"]="$value"
    done

    # Write .env
    {
        echo "# Generated by install.sh on $(date)"
        echo
        for key in "${ENV_KEYS[@]}"; do
            val="${COLLECTED[$key]:-}"
            required="${ENV_REQUIRED[$key]:-no}"
            if [[ -n "$val" ]]; then
                echo "${key}=${val}"
            elif [[ "$required" == "no" ]]; then
                echo "# ${key}="
            fi
        done
    } > "$ENV_FILE"

    _green "  Configuration written to $ENV_FILE"
fi

# ---------------------------------------------------------------------------
# Step 5 — install application dependencies
# ---------------------------------------------------------------------------
_step "Installing Python dependencies"
mkdir -p "$VENV_DIR"
# Install the venv to /var/lib (root filesystem) rather than /opt, which may
# be mounted noexec on some Raspberry Pi configurations.
# Use the system Python — uv's managed downloads land in /root/.local/share
# which is inaccessible to the service user.
UV_PROJECT_ENVIRONMENT="$VENV_DIR/.venv" \
    uv sync --directory "$INSTALL_DIR/backend" --no-dev --frozen --python-preference only-system

_step "Building native extensions"
if ! dpkg -s build-essential &>/dev/null 2>&1; then
    echo "  Installing build-essential for compilation…"
    apt-get install -y -qq build-essential
fi
UV_PROJECT_ENVIRONMENT="$VENV_DIR/.venv" bash "$INSTALL_DIR/backend/build_pymodes_cython.sh"
make -C "$INSTALL_DIR/backend/native"
make -C "$INSTALL_DIR/backend/native" install

_step "Building radar-core"
_ensure_go_toolchain
_tmp_radar_core_bin="$(mktemp /tmp/radar-core.XXXXXX)"
if ! (cd "$INSTALL_DIR/radar-core" && /usr/local/bin/go build -trimpath -o "$_tmp_radar_core_bin" ./cmd/radar-core); then
    rm -f "$_tmp_radar_core_bin"
    _die "radar-core build failed"
fi
install -m 0755 "$_tmp_radar_core_bin" "$RADAR_CORE_BINARY_PATH" || _die "Failed to install radar-core binary"
rm -f "$_tmp_radar_core_bin"
echo "  Installed radar-core binary at $RADAR_CORE_BINARY_PATH"

_step "Fetching airport and coastline data"
DATA_DIR="$INSTALL_DIR/backend/data"
if [[ ! -f "$DATA_DIR/airports.json" ]]; then
    python3 "$INSTALL_DIR/tools/fetch_airports.py"
else
    echo "  airports.json already present — skipping"
fi
if [[ ! -f "$DATA_DIR/coastline.json" ]]; then
    python3 "$INSTALL_DIR/tools/fetch_coastline.py"
else
    echo "  coastline.json already present — skipping"
fi

_step "Building frontend"
cd "$INSTALL_DIR/frontend"
npm ci --ignore-scripts
npm run build
cd "$INSTALL_DIR"

# ---------------------------------------------------------------------------
# Step 6 — create service user and set permissions
# ---------------------------------------------------------------------------
_step "Creating service user '$SERVICE_USER'"
if ! id "$SERVICE_USER" &>/dev/null; then
    useradd --system --no-create-home --shell /bin/false "$SERVICE_USER"
fi

chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR"
chown -R "$SERVICE_USER:$SERVICE_USER" "$VENV_DIR"

# .env contains secrets — world access is never appropriate.
# Own it by the real (sudoing) user so they can edit without sudo,
# with group adsb so the service can read it.
# Falls back to root:adsb when run directly as root.
_env_owner="${SUDO_USER:-root}"
chown "${_env_owner}:${SERVICE_USER}" "$ENV_FILE"
chmod 640 "$ENV_FILE"

# ---------------------------------------------------------------------------
# Step 7 — install and start the systemd service
# ---------------------------------------------------------------------------
_step "Installing systemd service"

SERVICE_SRC="$INSTALL_DIR/systemd/$SERVICE_NAME.service"
SERVICE_DEST="/etc/systemd/system/$SERVICE_NAME.service"

[[ -f "$SERVICE_SRC" ]] || _die "Service file not found: $SERVICE_SRC"

# Write the installed service file with paths resolved to INSTALL_DIR.
# Using a heredoc avoids sed quoting issues, especially for the ExecStart
# sh -c wrapper needed to expand HOST_PORT at runtime.
cat > "$SERVICE_DEST" << EOF
[Unit]
Description=ADS-B Dashboard
Documentation=https://github.com/caiusseverus/adsb-dashboard
After=network.target
Wants=network.target

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_USER

WorkingDirectory=$INSTALL_DIR/backend

ExecStart=/bin/sh -c 'exec $VENV_DIR/.venv/bin/python3 -m uvicorn main:app --host 0.0.0.0 --port "\${HOST_PORT:-8000}"'

EnvironmentFile=$INSTALL_DIR/backend/.env
RuntimeDirectory=adsb
RuntimeDirectoryMode=0755

Restart=on-failure
RestartSec=5

PrivateTmp=true
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo
if [[ "$IS_UPDATE" == true ]]; then
    _green "=========================================="
    _green " ADS-B Dashboard updated successfully!"
    _green "=========================================="
else
    _green "=========================================="
    _green " ADS-B Dashboard installed successfully!"
    _green "=========================================="
fi
echo
echo "  Dashboard URL : http://$(hostname -I | awk '{print $1}'):8000"
echo
echo "  Check service status : sudo systemctl status $SERVICE_NAME"
echo "  View live logs        : sudo journalctl -u $SERVICE_NAME -f"
echo "  Config file           : $INSTALL_DIR/backend/.env"
echo
