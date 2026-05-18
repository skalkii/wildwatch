# Setup — from zero to first alert

Linear walkthrough. Mac / Linux / Windows. No terminal experience required. Time: **15–30 min** depending on what's already installed.

You'll end with: dashboard running on your laptop, a Telegram bot pinging when a clip you upload contains a gunshot / alarm call / rare species.

---

## Two paths

### Path A — Let an AI tool do it (Claude Code, Cursor, etc.)

Open this folder in your AI coding tool. Paste this prompt:

> Read `CLAUDE.md` + `docs/SETUP.md`. Install all prerequisites for my OS, create a Telegram bot with me, get my VideoDB API key from me, fill `.env`, start the uvicorn server in background, start cloudflared tunnel, set `WEBHOOK_BASE_URL` to the tunnel URL, restart uvicorn, then run a smoke test that hits `/webhook/2` and confirms a Telegram message arrives.

Skip everything below — the AI handles it. You'll need to (a) paste your VideoDB API key when asked, (b) follow the BotFather prompts on Telegram.

If something fails, scroll to **Troubleshooting** at the bottom.

### Path B — Do it yourself

Continue reading. Each step has a "did it work?" check.

---

## 1. Open a terminal

| OS | How |
|---|---|
| **Mac** | `⌘ + Space` → type `Terminal` → Enter |
| **Linux** | `Ctrl + Alt + T` (most distros) |
| **Windows** | Install [WSL](https://learn.microsoft.com/en-us/windows/wsl/install) first, then open Ubuntu from Start menu. The rest of this guide assumes Linux commands. |

**Check:** typing `echo hello` should print `hello`.

---

## 2. Install Homebrew (Mac only)

Skip on Linux/WSL.

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

Follow the on-screen instructions (it'll ask for your password — that's normal). When done, it tells you to run two `eval` commands — run them.

**Check:** `brew --version` should print `Homebrew 4.x.x`.

---

## 3. Install Python 3.12

| OS | Command |
|---|---|
| Mac | `brew install python@3.12` |
| Ubuntu/Debian | `sudo apt update && sudo apt install python3.12 python3.12-venv` |

**Check:** `python3.12 --version` should print `Python 3.12.x`.

---

## 4. Install git

| OS | Command |
|---|---|
| Mac | Pre-installed. Run `git --version` to confirm. If missing: `brew install git`. |
| Ubuntu | `sudo apt install git` |

**Check:** `git --version` prints `git version 2.x`.

---

## 5. Install cloudflared (for public webhook URL)

Only needed if you want **live alerts** to reach you. Skip for the upload-only demo.

| OS | Command |
|---|---|
| Mac | `brew install cloudflare/cloudflare/cloudflared` |
| Linux | [Download binary from cloudflare](https://github.com/cloudflare/cloudflared/releases) |

**Check:** `cloudflared --version` prints a version line.

---

## 6. Install ffmpeg + streamlink + Docker (live feeds only)

Skip if you only want the **upload-clip demo** (the recommended demo path).

| Tool | Mac | Linux |
|---|---|---|
| ffmpeg | `brew install ffmpeg` | `sudo apt install ffmpeg` |
| streamlink | `brew install streamlink` | `sudo apt install streamlink` |
| Docker Desktop | [download](https://docs.docker.com/desktop/setup/install/mac-install/) | [download](https://docs.docker.com/desktop/setup/install/linux-install/) |

---

## 7. Get a VideoDB API key

1. Open https://console.videodb.io
2. Sign in (Google / email).
3. Left sidebar → **API Keys** → **Create new key**.
4. Copy the key. **Don't share it.** Save it somewhere for step 12.
5. Free trial credits are auto-applied — enough for the demo flow.

**Check:** your VideoDB console shows "Credits: $X.XX available".

---

## 8. Create a Telegram bot

1. Open Telegram (phone or desktop app).
2. Search for **@BotFather** → open chat → tap **Start**.
3. Send `/newbot` → follow prompts:
   - Choose a name (e.g. `WildWatch Demo`)
   - Choose a username ending in `bot` (e.g. `wildwatch_demo_bot`)
4. BotFather replies with a **token** like `8601136374:AAH…`. Copy this. Save for step 12.

**Check:** BotFather replied with `Use this token to access the HTTP API:` followed by your token.

---

## 9. Get your Telegram chat ID

1. In Telegram, open the chat with your new bot (search by the username you chose).
2. Tap **Start** or send any message (e.g. `hi`).
3. Back in terminal, replace `<TOKEN>` with your token from step 8:

```bash
curl "https://api.telegram.org/bot<TOKEN>/getUpdates"
```

4. Output is JSON. Find `"chat":{"id":NUMBER,...}` — copy that **number**. Save it for step 12.

Example output (number to copy is `8636175241`):
```json
{"ok":true,"result":[{"message":{"chat":{"id":8636175241,"first_name":"Kal","type":"private"},...}}]}
```

**Check:** you have a numeric chat ID like `8636175241`.

---

## 10. Clone the code

```bash
git clone https://github.com/skalkii/wildwatch.git
cd wildwatch
```

**Check:** `ls` shows `README.md`, `wildwatch/`, `prompts/`, etc.

---

## 11. Create the Python environment

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

The last command takes 1–3 minutes (downloads dependencies).

**Check:** prompt now starts with `(.venv)`. Running `python -c "import wildwatch"` prints nothing (no error = success).

---

## 12. Configure secrets

```bash
cp .env.example .env
```

Open `.env` in a text editor (TextEdit, VS Code, `nano .env`, whatever). Fill the three required values:

```
VIDEO_DB_API_KEY=your_key_from_step_7
TELEGRAM_BOT_TOKEN=your_token_from_step_8
TELEGRAM_CHAT_ID=your_chat_id_from_step_9
WEBHOOK_BASE_URL=http://localhost:8000
```

Save the file.

**Check:** `grep -c "^[A-Z]" .env` prints at least `4`.

---

## 13. Start the server

```bash
uvicorn wildwatch.webhooks:app --host 127.0.0.1 --port 8000 --reload
```

You'll see logs starting with `INFO: Uvicorn running on http://127.0.0.1:8000`.

Open http://localhost:8000/ in a browser. The dashboard loads.

**Check:** browser shows the WildWatch dashboard with four tabs (Alerts / Sources / Indexed Content / Usage).

**Leave this terminal running.** Open a second one for the next steps.

---

## 14. Make webhook public (optional — for live feeds)

Skip if you're only doing the upload demo.

In a **second terminal**:

```bash
cloudflared tunnel --url http://localhost:8000
```

Cloudflared prints a line like:
```
2026-05-18T10:00:00Z INF +--------------------------------------------------------------------------------------------+
2026-05-18T10:00:00Z INF |  Your quick Tunnel has been created! Visit it at (it may take some time to be reachable):  |
2026-05-18T10:00:00Z INF |  https://wildwatch-demo-abc-xyz.trycloudflare.com                                          |
```

Copy that `https://*.trycloudflare.com` URL. Open `.env`, set:
```
WEBHOOK_BASE_URL=https://wildwatch-demo-abc-xyz.trycloudflare.com
```

In the **first terminal** (where uvicorn runs), press `Ctrl+C` then re-run the `uvicorn` command. The `--reload` flag means future `.env` edits don't need a restart.

**Check:** `curl https://your-tunnel-url.trycloudflare.com/` returns dashboard HTML.

---

## 15. First demo — upload a clip

1. Open http://localhost:8000/.
2. Click **Sources** tab → **+ Add source** → **File upload**.
3. Drop in any wildlife clip (sample MP4s live in `samples/`). For best demo: a clip with audible alarm calls or visible animals.
4. Watch the card progress: `queued → connecting → ingesting → indexing → ready` (1–3 min depending on length).
5. Click **Alerts** tab. Within ~30s of `ready`, you should see:
   - The dashboard alert feed fills with tier-coloured cards.
   - Your phone buzzes — Telegram bot sends a message per alert with a tappable clip link.

**Check:** Telegram shows at least one alert message from your bot.

If the alerts don't fire automatically (e.g. the clip has no detectable threats):
- Alerts tab → scroll to **Test the alert system** → click 🟢 / 🟡 / 🔴 buttons. Each fires a synthetic test alert through the same pipeline. Telegram should buzz immediately.

---

## 16. (Optional) Build a daily summary reel

Once you have a few alerts in the feed:

1. Alerts tab → **Daily summary** card → **Build**.
2. Wait 30–90s (server-side reel composition).
3. Modal opens: 4-up KPI strip, charts, inline playable reel, narration transcript.
4. Telegram receives a 4-image album (charts) + narration paragraph + reel link.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `command not found: brew` | Homebrew not installed | Run step 2 |
| `command not found: python3.12` | Python 3.12 not installed | Run step 3. Mac users: `brew link --overwrite python@3.12` if `brew` complains |
| `Address already in use` on `uvicorn` | Port 8000 occupied | `lsof -ti:8000 \| xargs kill -9` then retry |
| `pip install` errors with `error: subprocess-exited-with-error` | Old pip | `pip install --upgrade pip` then retry step 11 |
| `VIDEO_DB_API_KEY: api key is invalid` | Wrong key or expired | Re-generate at https://console.videodb.io → API Keys |
| `getUpdates` returns `{"ok":true,"result":[]}` | You haven't messaged the bot yet | Open Telegram → message your bot → re-run curl |
| Telegram bot silent after webhook fires | Wrong `TELEGRAM_CHAT_ID` | Re-run step 9. ID should be a number, no quotes around it in `.env` |
| `cloudflared` exits immediately | Free quick-tunnel transient | Just re-run — Cloudflare assigns a new URL |
| Tunnel URL works but webhook 401s | `WILDWATCH_WEBHOOK_SECRET` set without forwarding header | Either unset it in `.env`, or configure VideoDB alerts to send the matching `X-WildWatch-Secret` header |
| Dashboard loads but Sources tab empty | `.state.json` got reset | That's fine. Add a new source. |
| Upload stuck on `ingesting` forever | VideoDB API or cloudflared dead | Check uvicorn logs in terminal 1. Look for stack traces. |
| `RuntimeError: cannot stop transmitter, alerts not active` on `bootstrap.py` re-run | Stale `.state.json` | Delete `.state.json`, re-run bootstrap |

For anything else: open uvicorn's terminal — error traces are visible there. Grep for `ERROR` or `Exception`.

---

## What's next

- **Add a live wildlife stream** → see [`bridge/README.md`](../bridge/README.md) for the YouTube → RTSP bridge.
- **Understand the architecture** → [`docs/FEATURE_FLOWS.md`](FEATURE_FLOWS.md).
- **Tweak prompts or events** → [`docs/REPO_MAP.md`](REPO_MAP.md) tells you where to edit.
- **Cost watching** → Usage tab on the dashboard shows real-time burn from VideoDB's billing API.
