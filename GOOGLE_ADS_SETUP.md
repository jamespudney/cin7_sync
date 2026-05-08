# Google Ads + GA4 — OAuth Setup Walkthrough

This is the one-time setup needed to unlock `google_ads_sync.py` and `ga4_sync.py`. Once these env vars are set, both scripts run automatically on the worker's daily cycle.

**Time required: ~10 minutes total.**

---

## Step 1 — Cloud Console project + OAuth client (5 min)

1. Go to https://console.cloud.google.com
2. Top bar → **Select a project** → **New Project** → name it `cin7-sync-marketing` → Create
3. Make sure that new project is selected at the top
4. Left menu → **APIs & Services** → **Library** → search and ENABLE these:
   - **Google Ads API**
   - **Google Analytics Data API**
5. Left menu → **APIs & Services** → **OAuth consent screen** → User Type: **External** → Create
   - App name: `cin7-sync` (anything works)
   - User support email: yours
   - Developer email: yours
   - Save through the rest accepting defaults
   - **Test users**: add your own email (the one with Google Ads + GA4 access)
6. Left menu → **APIs & Services** → **Credentials** → **+ Create Credentials** → **OAuth client ID**
   - Application type: **Desktop app**
   - Name: `cin7-sync-desktop`
   - Create

You'll get a popup with **Client ID** and **Client secret**. Copy both.

**Set in Render** (`cin7-sync-slack-bot` worker → Environment):
```
GOOGLE_ADS_CLIENT_ID      = <Client ID from popup>
GOOGLE_ADS_CLIENT_SECRET  = <Client secret from popup>
```

---

## Step 2 — Generate REFRESH_TOKEN via OAuth Playground (3 min)

This is the easiest way to get a refresh token without writing code.

1. Go to https://developers.google.com/oauthplayground
2. Click the **gear icon** (top right) → **Settings**
3. ☑ Tick **Use your own OAuth credentials**
4. Paste your `Client ID` and `Client secret` (from Step 1)
5. Click **Close**
6. In the left panel, scroll down to **Step 1: Select & authorize APIs**
7. In the input box at the bottom (it says "Input your own scopes"), paste these **two scopes** separated by a space:
   ```
   https://www.googleapis.com/auth/adwords https://www.googleapis.com/auth/analytics.readonly
   ```
8. Click **Authorize APIs**
9. Sign in with the **Google account that has Google Ads + GA4 access** (the one you log into ads.google.com / analytics.google.com with)
10. Click **Allow** on the consent screen
11. You're now on **Step 2: Exchange authorization code for tokens** → click **Exchange authorization code for tokens**
12. The right panel shows JSON containing a `refresh_token`. **Copy the refresh_token value** (the long string, not the quotes)

**Set in Render**:
```
GOOGLE_ADS_REFRESH_TOKEN  = <refresh_token from playground>
```

---

## Step 3 — Find your Google Ads customer ID (1 min)

This is the 10-digit account number, not your email.

1. Log into https://ads.google.com
2. Top right corner — your account ID is shown like `123-456-7890`
3. Strip the dashes: `1234567890`

**Set in Render**:
```
GOOGLE_ADS_CUSTOMER_ID    = 1234567890
```

If you have a Manager (MCC) account that owns sub-accounts, you also need:
```
GOOGLE_ADS_LOGIN_CUSTOMER_ID  = <MCC ID, dashes stripped>
```
Otherwise leave that one unset.

---

## Step 4 — Find your GA4 property ID (30 seconds)

1. Log into https://analytics.google.com
2. Bottom left → **Admin** ⚙️
3. Property Settings → top of the page shows **Property ID** (numeric, like `387654321`)

**Set in Render**:
```
GA4_PROPERTY_ID  = 387654321
```

---

## Step 5 — Verify (run on worker shell, ~2 min)

After setting all env vars, give Render 60 seconds to redeploy the worker, then SSH into the worker and run:

```bash
python google_ads_sync.py recent --days 7 --verbose
```

Expected output (if everything works):
```
[google_ads_sync] Refreshing Google Ads OAuth access token...
[google_ads_sync] Pulling campaign daily metrics 2026-05-01 -> 2026-05-08
[google_ads_sync] Got 47 row(s) from Google Ads API
[google_ads_sync] DONE: {'written': 47, 'skipped': 0, ...}
```

Then:

```bash
python ga4_sync.py recent --days 7 --verbose
```

Expected:
```
[ga4_sync] Refreshing OAuth access token...
[ga4_sync] === campaign totals ===
[ga4_sync] GA4 campaign-totals 2026-05-01 -> 2026-05-08
[ga4_sync] Got N GA4 rows
[ga4_sync] DONE: {'written': N, 'skipped': M, ...}
[ga4_sync] === per-sku ===
[ga4_sync] DONE: {'written': X, 'skipped': Y, ...}
```

---

## Common errors and fixes

### `OAuth token refresh failed`

→ One of CLIENT_ID, CLIENT_SECRET, or REFRESH_TOKEN is wrong. Re-run Step 2.

### `403 The caller does not have permission`

→ The OAuth client wasn't granted the necessary scopes during Step 2.9. Re-do the playground flow making sure both scopes are pasted in step 7.

### `developer-token not approved`

→ Your Google Ads developer token is in "Test access" mode (limited to test accounts). For production accounts: Google Ads → Tools → API Center → click your token → Apply for **Standard access**. Approval takes ~1–2 business days. Until then, the sync will only return data for test accounts.

### `customer not found`

→ `GOOGLE_ADS_CUSTOMER_ID` has dashes or letters. Strip to 10 digits only.

---

## What unlocks once this is set up

The Slack bot + dashboard AI Assistant will be able to answer:

- *"Which Google Ads campaigns are below 2.0x ROAS this week?"*
- *"Compare April vs March campaign efficiency by type"*
- *"Reconcile Google Ads' reported conversions vs GA4 reality on PMax-Q2"*
- *"What % of our paid traffic is on branded queries?"*
- *"Which SKUs are getting the most ad-driven add-to-carts but no purchases?"*

This is the Moby-replacement Phase 2. Combined with Phase 1 (Klaviyo + Reviews.io + SEMrush already live), this gives the bot full marketing-attribution context for any SKU question.

**Cancel Triple Whale on June 1.** ✓
