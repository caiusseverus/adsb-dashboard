#!/usr/bin/env bash
# ADS-B Dashboard — bare metal installer for Raspberry Pi OS (Debian-based)
# Usage: curl -fsSL https://raw.githubusercontent.com/caiusseverus/adsb-dashboard/main/install.sh | bash

set -euo pipefail

INSTALL_DIR="/opt/adsb-dashboard"
SERVICE_USER="adsb"
SERVICE_NAME="adsb-dashboard"
REPO_URL="https://github.com/caiusseverus/adsb-dashboard.git"

# Colour helpers (silent if not a terminal)
_red()   { printf '\033[0;31m%s\033[0m\n' "$*"; }
_green() { printf '\033[0;32m%s\033[0m\n' "$*"; }
_bold()  { printf '\033[1m%s\033[0m\n' "$*"; }

_step() { echo; _bold "==> $*"; }
_die()  { _red "ERROR: $*"; exit 1; }

# ---------------------------------------------------------------------------
# Step 1 — check we are running as root (or via sudo)
# ---------------------------------------------------------------------------
_step "Checking permissions"
[[ $EUID -eq 0 ]] || _die "This installer must be run as root. Try: sudo bash install.sh"

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

# Install uv (Python package/project manager)
if ! command -v uv &>/dev/null; then
    echo "  Installing uv …"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    # Make uv available in the current shell
    export PATH="$HOME/.local/bin:$PATH"
fi

# Verify Node version is >=18 (required for Vite)
NODE_MAJOR=$(node --version | sed 's/v\([0-9]*\).*/\1/')
if [[ "$NODE_MAJOR" -lt 18 ]]; then
    _die "Node.js 18+ is required. Detected: $(node --version). Install via: curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && apt-get install -y nodejs"
fi

# ---------------------------------------------------------------------------
# Step 3 — clone the repository
# ---------------------------------------------------------------------------
_step "Cloning repository to $INSTALL_DIR"

if [[ -d "$INSTALL_DIR/.git" ]]; then
    echo "  Repository already exists — pulling latest changes"
    git -C "$INSTALL_DIR" pull --ff-only
else
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
uv sync --directory "$INSTALL_DIR/backend" --no-dev --frozen

_step "Fetching airport and coastline data"
python3 "$INSTALL_DIR/tools/fetch_airports.py"
python3 "$INSTALL_DIR/tools/fetch_coastline.py"

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

# ---------------------------------------------------------------------------
# Step 7 — install and start the systemd service
# ---------------------------------------------------------------------------
_step "Installing systemd service"

SERVICE_SRC="$INSTALL_DIR/systemd/$SERVICE_NAME.service"
SERVICE_DEST="/etc/systemd/system/$SERVICE_NAME.service"

[[ -f "$SERVICE_SRC" ]] || _die "Service file not found: $SERVICE_SRC"

# Patch the WorkingDirectory and EnvironmentFile to the actual install path
sed \
    -e "s|WorkingDirectory=.*|WorkingDirectory=$INSTALL_DIR|" \
    -e "s|EnvironmentFile=.*|EnvironmentFile=$INSTALL_DIR/backend/.env|" \
    "$SERVICE_SRC" > "$SERVICE_DEST"

systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo
_green "=========================================="
_green " ADS-B Dashboard installed successfully!"
_green "=========================================="
echo
echo "  Dashboard URL : http://$(hostname -I | awk '{print $1}'):8000"
echo
echo "  Check service status : sudo systemctl status $SERVICE_NAME"
echo "  View live logs        : sudo journalctl -u $SERVICE_NAME -f"
echo "  Config file           : $INSTALL_DIR/backend/.env"
echo
