# Installing ADS-B Dashboard

Three installation methods are available:

- **Bare Metal (Raspberry Pi)** — runs directly on Raspberry Pi OS. The installer handles everything.
- **Docker** — uses a pre-built image from Docker Hub. Runs on any Linux, macOS, or Windows machine with Docker installed.
- **Dockge** — uses the same Docker image, configured directly in the Dockge UI.

---

## Bare Metal (Raspberry Pi)

### Prerequisites

- Raspberry Pi running **Raspberry Pi OS** (Bookworm or Bullseye, 64-bit or 32-bit)
- A working ADS-B receiver with **readsb** or **dump1090-fa** broadcasting Beast TCP on port 30005
- Internet access from the Pi (to download dependencies)

### Install

Run this single command on your Pi:

```bash
curl -fsSL https://raw.githubusercontent.com/caiusseverus/adsb-dashboard/main/install.sh | sudo bash
```

The installer will:

1. Install required packages (`git`, `nodejs`, `npm`, `uv`)
2. Clone the repository to `/opt/adsb-dashboard`
3. Walk you through every configuration option interactively — you will be asked for things like your receiver's hostname and your coordinates. Required fields will not accept empty input.
4. Build the frontend and install Python dependencies
5. Create an `adsb` system user and install a systemd service that starts the dashboard automatically on boot

When it finishes, open **http://\<your-pi-ip\>:8000** in a browser.

### Checking the service

```bash
# Is it running?
sudo systemctl status adsb-dashboard

# Live logs
sudo journalctl -u adsb-dashboard -f
```

### Updating

```bash
cd /opt/adsb-dashboard
sudo git pull
cd frontend && npm ci --ignore-scripts && npm run build && cd ..
sudo systemctl restart adsb-dashboard
```

### Configuration

All settings live in `/opt/adsb-dashboard/backend/.env`. Edit that file and restart the service to apply changes:

```bash
sudo nano /opt/adsb-dashboard/backend/.env
sudo systemctl restart adsb-dashboard
```

See `backend/.env.example` for a description of every available option.

---

## Docker

### Prerequisites

- Docker Engine 24+ and the Docker Compose plugin (`docker compose` — note: no hyphen)
- A working ADS-B receiver reachable from the Docker host on Beast TCP port 30005

### Install

**Step 1 — Download the compose file**

```bash
mkdir adsb-dashboard && cd adsb-dashboard
curl -fsSL https://raw.githubusercontent.com/caiusseverus/adsb-dashboard/main/docker-compose.yml -o docker-compose.yml
```

**Step 2 — Edit your configuration**

Open `docker-compose.yml` in a text editor and fill in the environment variables. At a minimum, set:

| Variable | What it is |
|---|---|
| `BEAST_HOST` | Hostname or IP of your receiver (e.g. `192.168.1.10`) |
| `BEAST_PORT` | Beast TCP port (default `30005`) |
| `RECEIVER_LAT` | Your latitude in decimal degrees (e.g. `51.5`) |
| `RECEIVER_LON` | Your longitude in decimal degrees (e.g. `-0.1`) |
| `HOME_COUNTRY` | Your country name (e.g. `United Kingdom`) |

Optional settings are included as commented-out lines — uncomment and set any you need.

**Step 3 — Pull the image and start**

```bash
docker compose pull
docker compose up -d
```

Open **http://localhost:8000** in a browser.

### Checking status

```bash
# Is it running?
docker compose ps

# Live logs
docker compose logs -f
```

### Updating

```bash
docker compose pull
docker compose up -d
```

Docker will download the new image and restart the container. The database and SRTM terrain tiles are stored in a named volume (`adsb-data`) and are preserved across updates.

---

## Dockge

### Prerequisites

- [Dockge](https://github.com/louislam/dockge) installed and running
- A working ADS-B receiver reachable from the Dockge host on Beast TCP port 30005

### Install

1. In the Dockge UI, click **+ Compose**
2. Give the stack a name (e.g. `adsb-dashboard`)
3. Paste in the contents of [`docker-compose.yml`](https://raw.githubusercontent.com/caiusseverus/adsb-dashboard/main/docker-compose.yml)
4. Edit the environment variables directly in the editor — at minimum set:
   - `BEAST_HOST` — IP or hostname of your receiver
   - `RECEIVER_LAT` and `RECEIVER_LON` — your coordinates
   - `HOME_COUNTRY` — your country name
5. Click **Deploy**

Open **http://\<your-host-ip\>:8000** in a browser.

### Updating

In the Dockge UI, click the stack → **Pull & Restart**. Your data volume is preserved automatically.

---

## Troubleshooting

### The dashboard loads but shows no aircraft

**Likely cause:** the backend cannot connect to your receiver's Beast TCP stream.

Check: is the `BEAST_HOST` and `BEAST_PORT` in your `.env` correct? Can the machine running the dashboard reach that host on that port?

```bash
# Test connectivity (run on the Pi or Docker host)
nc -zv <BEAST_HOST> <BEAST_PORT>
```

If you are running Docker on a machine that is *also* running readsb locally, use `BEAST_HOST=host.docker.internal` (Linux requires `--add-host=host.docker.internal:host-gateway`, which `docker-compose.yml` does not add by default — set `BEAST_HOST` to the host's LAN IP instead).

### The service fails to start / exits immediately

Check the logs for the specific error:

```bash
# Bare metal
sudo journalctl -u adsb-dashboard -n 50 --no-pager

# Docker
docker compose logs --tail 50
```

Common causes: a missing or malformed `.env` file; a Python dependency that failed to install (check uv output); the port 8000 already in use by another process.

### The polar chart / range statistics are missing

**Cause:** `RECEIVER_LAT` and `RECEIVER_LON` are not set (or are commented out) in your `.env`.

These two values are optional but strongly recommended — without them the dashboard cannot calculate range and bearing, so the polar coverage chart, range percentiles, and azimuth/elevation scatter plot are all unavailable.

Add them to your `.env` and restart the service.
