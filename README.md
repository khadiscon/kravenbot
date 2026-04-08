# KOL Campaign Manager Bot

Full-featured Telegram bot for managing KOL link-drop sessions, queue
enforcement, auto-moderation, and campaign tracking.

---

## Setup

### 1. Create your bot

1. Open Telegram and message **@BotFather**
2. Send `/newbot` and follow the prompts
3. Copy the API token you receive

### 2. Add the bot to your group

1. Add the bot to your Telegram group
2. Promote the bot to **Administrator**
3. Grant it: Delete messages, Ban users, Restrict members

### 3. Install and run locally

```bash
# Clone / copy the project
cd kraven\ bot

# Create a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Set up your environment
cp env.example .env
nano .env        # paste your BOT_TOKEN

# Run the bot
python bot.py
```

### 4. Run in the background (Linux / Ubuntu)

Using `screen`:
```bash
screen -S kolbot
python bot.py
# Ctrl+A then D to detach
```

Using `nohup`:
```bash
nohup python bot.py > bot.log 2>&1 &
```

---

## GitHub + Railway Deploy

This duplicate folder is prepared specifically for GitHub upload and Railway deployment.

What is already cleaned up:
- no `venv/`
- no local `.env`
- no local SQLite database file
- `.gitignore` added
- `railway.toml` added

### Upload to GitHub

Upload the contents of this folder, not the original local runtime folder.

### Deploy to Railway

1. Push this folder to GitHub
2. Create a new Railway project from that GitHub repo
3. Add a volume and mount it at `/app/data`
4. Add these Railway variables:

```bash
BOT_TOKEN=your_bot_token
DEFAULT_QUEUE_SIZE=15
DB_PATH=/app/data/kol_bot.db
```

5. Deploy

### Important note

This bot still uses SQLite. On Railway, you should mount a volume at `/app/data` so the database survives restarts and redeploys.

---

## Full Command Reference

### User Commands

| Command | Description |
|---|---|
| `/mystatus` | Your queue position, warnings, total links, points |
| `/leaderboard` | All-time top 10 link posters |
| `/campaignstatus` | Current campaign progress, top contributors |
| `/mycampaignstats` | Your submissions and rank in the active campaign for this topic |
| `/stats` | Group-wide totals: users, links, campaigns, bans |

### Admin Commands — Session Control

| Command | Description |
|---|---|
| `/startsession` | Open the default 15-link session (clears the queue) |
| `/startsession15` | Open a 15-link session |
| `/startsession28` | Open a 28-link session |
| `/stopsession` | Close the session |
| `/setqueue [n]` | Change the queue size (default: 15) |
| `/setpoints [n]` | Points awarded per valid link (default: 10) |

### Admin Commands — User Management

| Command | Description |
|---|---|
| `/reset @user` | Wipe a user's warnings, links, points, and queue position |
| `/whitelist @user` | Toggle queue exemption for a user |
| `/warn @user` | Issue a manual warning (5-step escalation: notice, mute, then ban) |
| `/ban @user` | Immediately ban a user |
| `/unban @user` | Unban and clear warnings |
| `/tagall [message]` | Mention all tracked members in chunked messages |

### Admin Commands — Campaign Management

| Command | Description |
|---|---|
| `/newcampaign Name \| Description \| Target \| Reward \| Deadline` | Launch a new campaign |
| `/endcampaign` | End the active campaign in this topic and print final leaderboard |
| `/exportlinks` | Download a `.txt` file containing every submitted link in this topic campaign, useful when you want to review entries or send the full link list to the person handling rewards |
| `/verifysub @user` | Mark a user's submissions as verified in this topic |
| `/logpayout @user [amount] [reason]` | Save a payment record for a user so you know they were already paid, how much they got, and what the payment was for |
| `/payouts` | Show the last 20 saved payment records so you can quickly check who has already been paid |

---

## How the Queue Works

1. A user posts a Twitter/X link
2. The bot records their position in the queue
3. They **cannot post again** until 15 other unique people have posted after them
4. `/mystatus` shows their progress (e.g. `8/15 done`)
5. Any message that is not a single Twitter/X link is deleted with a notice during the session
6. Valid drops are reposted by the bot as a standardized X link, then the user's original message is deleted
7. Posting too early removes the message and tells the user how long to wait
8. Warnings are manual only via `/warn`. 5 warnings = permanent ban

---

## How Campaigns Work

1. Admin runs `/newcampaign` with campaign details
2. This automatically starts a session and clears the queue in that topic
3. All valid link drops are recorded as campaign submissions
4. `/campaignstatus` shows progress bar, top contributors for that topic campaign
5. Admin uses `/exportlinks` to download the full list of submitted links
6. Admin checks that list, verifies valid work, then uses `/logpayout` to record who got paid and why
7. Admin runs `/endcampaign` to close it and print the final leaderboard

---

## Bot Permission Requirements

The bot **must** be an administrator with these permissions:
- Delete messages
- Ban users
- Restrict members

---

## Notes

- The database (`kol_bot.db`) is created automatically on first run
- The bot works across multiple groups simultaneously — data is scoped per chat, and sessions/campaigns are scoped per topic
- Whitelisted users bypass the queue rule entirely (useful for mods or campaign coordinators)
- Warning escalation is: 1-2 notices, 3 = 24h mute, 4 = 72h mute, 5 = permanent ban
- In private chat, `/start` and `/help` open an inline keyboard control panel
