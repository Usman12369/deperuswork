FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .

# Install a site-wide startup hook so Python always runs our patch early.
COPY sitecustomize.py /usr/local/lib/python3.9/site-packages/sitecustomize.py

# Add a small entrypoint that logs the first lines of /app/bot.py (diagnostic) and starts the bot
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

VOLUME ["/app/data"]

# Use ENTRYPOINT so the script always runs (even if platform overrides CMD)
ENTRYPOINT ["/entrypoint.sh"]