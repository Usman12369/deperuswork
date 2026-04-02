"""
sitecustomize.py

This file is executed automatically by Python at startup (if found on sys.path).
We use it to monkeypatch telebot.util.escape_markdown if the installed
pyTelegramBotAPI version does not expose it. This prevents ImportError in
`from telebot.util import escape_markdown` even if an older image/file is used.

Keep this file minimal and defensive.
"""
import sys
import html
import logging

logger = logging.getLogger("sitecustomize")

def _escape_md_fallback(s: object) -> str:
    s = str(s)
    s = s.replace('\\', '\\\\')
    for ch in ['_', '*', '[', ']', '(', ')', '`']:
        s = s.replace(ch, f'\\{ch}')
    return s

try:
    # Attempt to import telebot.util and ensure function exists
    import telebot.util as _tutil  # type: ignore
    if not hasattr(_tutil, 'escape_markdown'):
        _tutil.escape_markdown = _escape_md_fallback  # type: ignore
        # Print to stdout so container logs always show the action
        try:
            print("sitecustomize: injected escape_markdown fallback into telebot.util")
        except Exception:
            pass
except Exception as e:
    # If telebot isn't installed yet or another error happens, fail silently
    # but log to stdout/stderr so container logs show it
    try:
        print(f"sitecustomize: warning - could not patch telebot.util: {e}")
    except Exception:
        pass


