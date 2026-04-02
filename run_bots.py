import os
import signal
import subprocess
import sys
import time


def build_bot_env(name: str) -> dict:
    upper = name.upper()
    env = os.environ.copy()
    env["BOT_INSTANCE_NAME"] = name
    env["BOT_TOKEN"] = os.getenv(f"{upper}_TOKEN", os.getenv("BOT_TOKEN", ""))
    env["ADMIN_ID"] = os.getenv(f"{upper}_ADMIN_ID", os.getenv("ADMIN_ID", "7019136722"))
    env["BOT_DATA_DIR"] = os.getenv(f"{upper}_DATA_DIR", f"/app/data/{name}")
    env["HELP_HANDLE"] = os.getenv(f"{upper}_HELP_HANDLE", os.getenv("HELP_HANDLE", "@U5M4H"))
    env["HELP_URL"] = os.getenv(f"{upper}_HELP_URL", os.getenv("HELP_URL", "https://t.me/U5M4H"))
    env["ENABLE_HEALTH_SERVER"] = "0"
    return env


def get_bot_defs():
    names = [name.strip() for name in os.getenv("BOT_INSTANCES", "bot1").split(",") if name.strip()]
    bot_defs = []
    for name in names:
        upper = name.upper()
        script = os.getenv(f"{upper}_SCRIPT", os.getenv("BOT_SCRIPT", "/app/bot.py"))
        bot_defs.append({"name": name, "script": script, "env": build_bot_env(name)})
    return bot_defs


def terminate_all(processes):
    for process in processes:
        if process.poll() is None:
            process.terminate()
    deadline = time.time() + 10
    for process in processes:
        while process.poll() is None and time.time() < deadline:
            time.sleep(0.2)
        if process.poll() is None:
            process.kill()


def main():
    bot_defs = get_bot_defs()
    processes = []

    def handle_signal(signum, _frame):
        print(f"Received signal {signum}, stopping bots...")
        terminate_all(processes)
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    for bot_def in bot_defs:
        token = bot_def["env"].get("BOT_TOKEN", "").strip()
        if not token:
            print(f"Skipping {bot_def['name']}: no token configured")
            continue

        os.makedirs(bot_def["env"]["BOT_DATA_DIR"], exist_ok=True)
        print(
            f"Starting {bot_def['name']} from {bot_def['script']} "
            f"with data dir {bot_def['env']['BOT_DATA_DIR']}"
        )
        process = subprocess.Popen([sys.executable, bot_def["script"]], env=bot_def["env"])
        processes.append(process)

    if not processes:
        print("No bots were started. Configure BOT_INSTANCES and *_TOKEN env vars.")
        return 1

    while True:
        for process in processes:
            code = process.poll()
            if code is not None:
                print(f"Bot process exited with code {code}, stopping the rest...")
                terminate_all(processes)
                return code
        time.sleep(2)


if __name__ == "__main__":
    raise SystemExit(main())
