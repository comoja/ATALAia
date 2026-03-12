#!/bin/bash

# Change directory to the project root (the parent of the 'backend' directory)
# This ensures that Python can find the 'backend' package and resolve module imports.
cd "$(dirname "$0")/.."

echo "Starting sentinel bot from project root..."
# Execute the bot as a module to handle relative imports correctly.
python3 -m backend.bots.sentinel

# --- PM2 Process Management ---
# The commands below are for running the bot as a persistent background service using pm2.
# They have been commented out. Uncomment and adapt them if you need to use pm2.
# Make sure to run pm2 from the project root directory.

# Example of how to start with pm2:
# pm2 start "python3 -m backend.bots.sentinel" --name "sentinel-bot"

# Original pm2 commands (commented out):
# pm2 start backend/bots/sentinel.py --name "sentinel-bot" --interpreter python3
# pm2 startup
# pm2 stop sentinel-bot
# pm2 start sentinel-bot