# DEPLOY.md
## Deploying to Render (production)

This is the steps-to-take version. For the why-and-how, see comments
in `render.yaml` and `start.sh`.

---

## One-time setup

### 1. Connect Render to GitHub
1. Log in to https://dashboard.render.com
2. Click **New +** → **Blueprint**
3. Connect your GitHub account if you haven't already
4. Pick the `jamespudney/cin7_sync` repo
5. Render reads `render.yaml` and shows you a preview of the services
   it'll create (`wired4signs-app` web service + `wired4signs-sync`
   background worker + 5GB shared disk).
6. Click **Apply**

### 2. Set secrets (env vars marked `sync: false` in render.yaml)
Render will prompt you for each secret on first apply. Have these ready:

| Variable | Where to find it |
|----------|------------------|
| `APP_PASSWORD` | Pick a strong password — share with your team out-of-band (Bitwarden, 1Password). |
| `CIN7_ACCOUNT_ID` | From your local `.env` file. Value starts with the UUID prefix you already use. |
| `CIN7_APPLICATION_KEY` | From your local `.env` file. Same as you use locally. |
| `IP_API_KEY` | From `.env`. Inventory Planner token. |
| `IP_ACCOUNT` | From `.env`. Your IP account number (e.g. `a13444`). |

### 3. First deploy
1. Render kicks off the build automatically once secrets are set
2. Build time is ~5 minutes (pip install ~3min, Streamlit boot ~2min)
3. Watch the build logs in Render's UI
4. When status flips to **Live**, click the URL (something like
   `https://wired4signs-app.onrender.com`)
5. You should see the password gate — enter `APP_PASSWORD`

### 4. Seed the persistent disk with current data
The first deploy starts with an EMPTY `/data` disk — no SQLite, no
CSVs. Two options to seed:

**Option A — let the background worker populate it (slow but clean):**
1. Trigger the background worker manually in Render's UI: `wired4signs-sync` → **Trigger Run**.
2. It'll run the CIN7 sync from scratch. First run takes 30-60 min
   because it pulls deep history (5 years of sale_lines).
3. While that runs, the app shows blank/empty (no data yet).

**Option B — copy your local DB and CSVs across (faster):**
1. Render gives you SSH access on Pro tier. Use it to upload the local
   `team_actions.db` and the latest CSVs from `output/` to `/data/`.
2. Reload the app — your local state appears in production.

Recommended: Option A for cleanliness; Option B if you want zero
downtime in cutover.

### 5. (Optional) Custom domain
1. In Render's UI, go to your `wired4signs-app` service → Settings →
   Custom Domains
2. Add `analytics.w4susa.com` (or whatever you want)
3. Render shows you a CNAME record to add at your DNS provider
4. Add the CNAME, wait 5-30 min for propagation
5. HTTPS is automatic via Render's Let's Encrypt integration

---

## Day-to-day workflow

### Pushing code changes
```
git add -A
git commit -m "your message"
git push origin main
```
Render auto-deploys on push (configured in `render.yaml`). Build takes
~5 min. Streamlit users get auto-reloaded into the new version.

### Forcing a manual sync
- In Render's UI, navigate to `wired4signs-sync`
- Click **Trigger Run**
- Logs stream in the UI

### Inspecting logs
- Web service: `wired4signs-app` → Logs tab
- Cron service: `wired4signs-sync` → Logs tab (each run is its own log)
- Daily-sync application log: `/data/output/daily_sync.log` on the disk
  (read via Render's shell)

### Backups
SQLite DB and CSV outputs all live on `/data`. To back up:
1. Render Pro auto-snapshots disks daily, retained 7 days. Restore in
   the UI.
2. For longer retention, set up a nightly cron that uploads
   `team_actions.db` to S3/Backblaze/etc. (Not configured yet — TODO.)

---

## Local development still works

Nothing about this deploy changes how the app runs locally. To dev:

```powershell
cd C:\Tools\cin7_sync
.venv\Scripts\activate
streamlit run app.py
```

`DATA_DIR` is unset locally, so `data_paths.py` defaults to the
project folder (same behaviour as before).

`APP_PASSWORD` is unset locally, so the password gate is bypassed.

---

## Troubleshooting

### "Application failed to start"
Check the build logs. Most common causes:
- Missing dependency in `requirements.txt`
- Import error from a path-refactor we missed
- Env var typo (e.g. `CIN7_ACCOUNT_ID` vs `CIN7_ACCOUNT_KEY`)

### "Wrong password" but I'm sure it's right
Render's env-var UI sometimes adds a trailing whitespace. Edit the var
in Render and ensure no trailing space.

### Daily sync didn't run
- Check `wired4signs-sync` → Logs in Render UI
- Check `/data/output/daily_sync.log` for partial output
- Run it manually via **Trigger Run** to surface the error

### App is slow on first page load after deploy
Normal. The disk-persisted ABC engine cache is rebuilt on first request
after a deploy. Subsequent loads are fast.

### Need to wipe everything and start fresh
- In Render UI: delete the disk via service → Settings → Disk → Delete
- Re-trigger the background worker to repopulate
- Lose: all draft POs, all migration mappings, all supplier configs
- Don't do this lightly.

---

## When to call for help

- Build keeps failing: logs will tell you why; if not, share with developer
- Daily sync repeatedly fails: check CIN7 API key isn't expired
- Disk usage approaching 5GB: bump disk size in render.yaml + redeploy
- Need to change schedule: edit `cron.schedule` in render.yaml
- Need a second tenant / customer: see `SAAS_NOTES.md`
