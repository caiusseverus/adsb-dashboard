#!/bin/sh
# Copy static data files into the volume if they are missing.
# The source files live at /app/static_data/ (outside the volume) so they
# survive image updates regardless of whether the named volume already exists.
for f in airports.json coastline.json; do
    if [ ! -f "/app/backend/data/$f" ]; then
        echo "Seeding $f from image..."
        cp "/app/static_data/$f" "/app/backend/data/$f"
    fi
done

exec .venv/bin/uvicorn main:app --host 0.0.0.0 --port "${HOST_PORT:-8000}"
