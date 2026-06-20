import os
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Tuple
from threading import Thread
#import socket

import discord
from discord.ext import commands, tasks
from discord import app_commands
from flask import Flask
import time

app = Flask('')

@app.route('/')
def home():
    return "Bot is alive!"

def run_flask():
    app.run(host='0.0.0.0', port=8080, debug=False, use_reloader=False)

def keep_alive():
    t = Thread(target=run_flask, daemon=True)
    t.daemon = True
    t.start()
    time.sleep(1)  # Give Flask time to start
    
# =========================
# Quick setup
# pip install -U discord.py
# env:
# DISCORD_BOT_TOKEN=...
# GUILD_ID=your_server_id
# HAILSHARE_CATEGORY_ID=optional_category_id
# run python bot.py
# =========================

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "YOUR_BOT_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID", "0"))  # required
HAILSHARE_CATEGORY_ID = int(os.getenv("HAILSHARE_CATEGORY_ID", "0"))  # recommended
DB_PATH = os.getenv("HAILSHARE_DB_PATH", "hailshare.db")

CHANNEL_PREFIX = "trio-"
TZ = timezone(timedelta(hours=7))  # UTC+7

LOCATIONS = [
    "airport",
    "market",
    "palace",
    "bitexco",
    "tasco",
    "VNG campus",
    "nova gallery",
    "pink church",
    "cathedral",
    "galaxy innovation park"
]
LOCATION_SET = set(LOCATIONS)

BUFFER_CHOICES = [15, 30, 60, 90]  # minutes
MAX_BUFFER = max(BUFFER_CHOICES)


# =========================
# DB
# =========================
class DB:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            meetup_dt TEXT NOT NULL,
            from_location TEXT NOT NULL,
            to_location TEXT NOT NULL,
            buffer_minutes INTEGER NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('current','matched','cancelled')),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_requests_user_status
            ON requests(user_id, status);

            CREATE INDEX IF NOT EXISTS idx_requests_match
            ON requests(status, from_location, to_location, meetup_dt);

            CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT NOT NULL UNIQUE,
            channel_name TEXT NOT NULL,
            meetup_dt TEXT NOT NULL,
            route_from TEXT,
            route_to TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_channels_meetup_dt
            ON channels(meetup_dt);

            CREATE TABLE IF NOT EXISTS channel_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            joined_at TEXT NOT NULL,
            UNIQUE(channel_id, user_id),
            FOREIGN KEY(channel_id) REFERENCES channels(channel_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_channel_members_user
            ON channel_members(user_id);

            CREATE INDEX IF NOT EXISTS idx_channel_members_channel_user
            ON channel_members(channel_id, user_id);

            CREATE TABLE IF NOT EXISTS channel_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT NOT NULL,
            event_type TEXT NOT NULL CHECK (
                event_type IN ('create','update','member_join','member_leave')
            ),
            event_time TEXT NOT NULL,
            member_user_ids TEXT NOT NULL,
            FOREIGN KEY(channel_id) REFERENCES channels(channel_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_channel_events_channel_time
            ON channel_events(channel_id, event_time);

            CREATE TABLE IF NOT EXISTS locks (
            name TEXT PRIMARY KEY,
            locked_until TEXT NOT NULL
            );
            """
        )
        self.conn.commit()

    def now(self) -> datetime:
        return datetime.now(TZ)

    def now_iso(self) -> str:
        return self.now().isoformat()

    # ---------- lock ----------
    def try_acquire_lock(self, name: str, ttl_seconds: int = 50) -> bool:
        # lightweight mutex
        now = self.now()
        until = now + timedelta(seconds=ttl_seconds)
        cur = self.conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        row = cur.execute("SELECT locked_until FROM locks WHERE name=?", (name,)).fetchone()
        if row:
            locked_until = datetime.fromisoformat(row["locked_until"])
            if locked_until > now:
                self.conn.rollback()
                return False
            cur.execute("UPDATE locks SET locked_until=? WHERE name=?", (until.isoformat(), name))
        else:
            cur.execute("INSERT INTO locks(name, locked_until) VALUES(?,?)", (name, until.isoformat()))
        self.conn.commit()
        return True

    # ---------- requests ----------
    def get_current_request_for_user(self, user_id: int) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT * FROM requests
            WHERE user_id=? AND status='current'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (str(user_id),),
        ).fetchone()

    def insert_request(self, user_id: int, meetup_dt: datetime, from_loc: str, to_loc: str, buffer: int):
        now = self.now_iso()
        self.conn.execute(
            """
            INSERT INTO requests (user_id, meetup_dt, from_location, to_location, buffer_minutes, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 'current', ?, ?)
            """,
            (str(user_id), meetup_dt.isoformat(), from_loc, to_loc, buffer, now, now),
        )
        self.conn.commit()

    def cancel_request(self, request_id: int):
        self.conn.execute(
            "UPDATE requests SET status='cancelled', updated_at=? WHERE id=?",
            (self.now_iso(), request_id),
        )
        self.conn.commit()

    def set_matched(self, request_id: int):
        self.conn.execute(
            "UPDATE requests SET status='matched', updated_at=? WHERE id=?",
            (self.now_iso(), request_id),
        )
        self.conn.commit()

    def list_current_requests(self) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM requests WHERE status='current' ORDER BY created_at ASC"
        ).fetchall()

    def cancel_past_current_requests(self):
        now = self.now()
        rows = self.list_current_requests()
        for r in rows:
            if datetime.fromisoformat(r["meetup_dt"]) < now:
                self.cancel_request(r["id"])

    def latest_current_request(self, user_id: int) -> Optional[sqlite3.Row]:
        return self.get_current_request_for_user(user_id)

    # ---------- channels ----------
    def upsert_channel(self, channel_id: int, channel_name: str, meetup_dt: datetime, route_from: Optional[str] = None, route_to: Optional[str] = None):
        now = self.now_iso()
        self.conn.execute(
            """
            INSERT INTO channels (channel_id, channel_name, meetup_dt, route_from, route_to, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET
            channel_name=excluded.channel_name,
            meetup_dt=excluded.meetup_dt,
            route_from=COALESCE(excluded.route_from, channels.route_from),
            route_to=COALESCE(excluded.route_to, channels.route_to),
            updated_at=excluded.updated_at
            """,
            (str(channel_id), channel_name, meetup_dt.isoformat(), route_from, route_to, now, now),
        )
        self.conn.commit()

    def get_channel_route(self, channel_id: int) -> Optional[Tuple[str, str]]:
        row = self.conn.execute(
            "SELECT route_from, route_to FROM channels WHERE channel_id=? LIMIT 1",
            (str(channel_id),),
        ).fetchone()
        if not row:
            return None
        if not row["route_from"] or not row["route_to"]:
            return None
        return (row["route_from"], row["route_to"])

    def get_channel_members(self, channel_id: int) -> List[int]:
        rows = self.conn.execute(
            "SELECT user_id FROM channel_members WHERE channel_id=?",
            (str(channel_id),),
        ).fetchall()
        return [int(r["user_id"]) for r in rows]

    def get_max_buffer_for_users(self, user_ids: List[int]) -> int:
        if not user_ids:
            return MAX_BUFFER
        placeholders = ",".join(["?"] * len(user_ids))
        rows = self.conn.execute(
            f"""
            SELECT MAX(buffer_minutes) AS max_buf
            FROM requests
            WHERE user_id IN ({placeholders}) AND status='matched'
            """,
            [str(u) for u in user_ids],
        ).fetchone()
        return int(rows["max_buf"]) if rows and rows["max_buf"] is not None else MAX_BUFFER

    def replace_channel_members(self, channel_id: int, user_ids: List[int]):
        now = self.now_iso()
        self.conn.execute("DELETE FROM channel_members WHERE channel_id=?", (str(channel_id),))
        self.conn.executemany(
            "INSERT INTO channel_members(channel_id, user_id, joined_at) VALUES (?, ?, ?)",
            [(str(channel_id), str(uid), now) for uid in user_ids],
        )
        self.conn.commit()

    def sync_channel_members_incremental(self, channel_id: int, new_user_ids: List[int]):
        old = set(self.get_channel_members(channel_id))
        new = set(int(u) for u in new_user_ids)

        to_add = sorted(new - old)
        to_remove = sorted(old - new)

        now = self.now_iso()

        if to_add:
            self.conn.executemany(
                "INSERT OR IGNORE INTO channel_members(channel_id, user_id, joined_at) VALUES (?, ?, ?)",
                [(str(channel_id), str(uid), now) for uid in to_add],
            )
            for uid in to_add:
                self.add_channel_event(channel_id, "member_join", [uid])

        if to_remove:
            self.conn.executemany(
                "DELETE FROM channel_members WHERE channel_id=? AND user_id=?",
                [(str(channel_id), str(uid)) for uid in to_remove],
            )
            for uid in to_remove:
                self.add_channel_event(channel_id, "member_leave", [uid])

        self.conn.commit()
        
    def add_channel_event(self, channel_id: int, event_type: str, user_ids: List[int]):
        self.conn.execute(
            """
            INSERT INTO channel_events(channel_id, event_type, event_time, member_user_ids)
            VALUES (?, ?, ?, ?)
            """,
            (str(channel_id), event_type, self.now_iso(), json.dumps([str(u) for u in user_ids])),
        )
        self.conn.commit()

    def user_has_active_channel_db(self, user_id: int) -> bool:
        now = self.now_iso()
        row = self.conn.execute(
            """
            SELECT 1
            FROM channel_members cm
            JOIN channels c ON c.channel_id = cm.channel_id
            WHERE cm.user_id=? AND c.channel_name LIKE ? AND c.active_until > ?
            LIMIT 1
            """,
            (str(user_id), f"{CHANNEL_PREFIX}%", now),
        ).fetchone()
        return row is not None

    def clear_active_channel_membership(self, user_id: int):
        self.conn.execute(
            "DELETE FROM channel_members WHERE user_id = ?",
            (str(user_id),),
        )
        self.conn.commit()

db = DB(DB_PATH)


# =========================
# HELPERS
# =========================
def parse_meetup_dt(date_str: str, time_str: str) -> datetime:
    # input local UTC+7
    dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
    return dt.replace(tzinfo=TZ)


def build_channel_name(meetup_dt: datetime) -> str:
    # trio-YYYYMMDD_HHMM-timestamp
    return f"{CHANNEL_PREFIX}{meetup_dt.strftime('%Y%m%d_%H%M')}-{datetime.now(TZ).strftime('%Y%m%d%H%M%S')}"

def parse_channel_meetup(channel_name: str) -> Optional[datetime]:
    # trio-YYYYMMDD_HHMM-YYYYMMDDHHMMSS
    if not channel_name.startswith(CHANNEL_PREFIX):
        return None
    parts = channel_name.split("-")
    if len(parts) < 3:
        return None
    try:
        return datetime.strptime(parts[1], "%Y%m%d_%H%M").replace(tzinfo=TZ)
    except ValueError:
        return None


def request_text(r: sqlite3.Row) -> str:
    meetup = datetime.fromisoformat(r["meetup_dt"]).strftime("%Y-%m-%d %H:%M")
    return (
        f"Request #{r['id']}\n"
        f"- meetup: {meetup} (UTC+7)\n"
        f"- from: {r['from_location']}\n"
        f"- to: {r['to_location']}\n"
        f"- buffer: {r['buffer_minutes']} minutes\n"
        f"- status: {r['status']}"
    )


def request_key(r: sqlite3.Row) -> Tuple[str, str, str]:
    dt = datetime.fromisoformat(r["meetup_dt"]).strftime("%Y-%m-%d %H:%M")
    return (r["from_location"], r["to_location"], dt)

def within_user_buffer(user_meetup: datetime, target_meetup: datetime, user_buffer_minutes: int) -> bool:
    return abs((user_meetup - target_meetup).total_seconds()) <= user_buffer_minutes * 60

async def find_user_active_trio_channel(guild: discord.Guild, user_id: int) -> bool:
    now = datetime.now(TZ)
    for ch in guild.text_channels:
        if not ch.name.startswith(CHANNEL_PREFIX):
            continue
        meetup = parse_channel_meetup(ch.name)
        if not meetup:
            continue
        if meetup <= now:
            continue
        if any(m.id == user_id for m in ch.members):
            return ch
    return None

async def user_eligible_for_channel(guild: discord.Guild, user_id: int) -> bool:
    has_active_discord = await find_user_active_trio_channel(guild, user_id)
    has_active_db = db.user_has_active_channel_db(user_id)

    if has_active_discord:
        return False
    elif has_active_db:
        db.clear_active_channel_membership(user_id)
        has_active_db = False

    return True


# =========================
# HEALTH CHECK SERVER
# =========================
#def start_health_check():
#    """Simple TCP health check on port 8080 - keeps Render from spinning down"""
#    def run_server():
#        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#        try:
#            sock.bind(('0.0.0.0', 8080))
#            sock.listen(1)
#            while True:
#                try:
#                    conn, addr = sock.accept()
#                    conn.send(b'HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nAlive')
#                    conn.close()
#                except:
#                    pass
#        except Exception as e:
#            print(f"Health check error: {e}")
#    
#    thread = Thread(target=run_server, daemon=True)
#    thread.start()


# =========================
# BOT
# =========================
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)


class ReplaceView(discord.ui.View):
    def __init__(self, user_id: int, payload: dict):
        super().__init__(timeout=120)
        self.user_id = user_id
        self.payload = payload

    @discord.ui.button(label="Cancel previous & submit new", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Not your action.", ephemeral=True)
            return

        prev = db.get_current_request_for_user(self.user_id)
        if prev:
            db.cancel_request(prev["id"])
        db.insert_request(
            user_id=self.user_id,
            meetup_dt=self.payload["meetup_dt"],
            from_loc=self.payload["from_location"],
            to_loc=self.payload["to_location"],
            buffer=self.payload["buffer_minutes"],
        )
        await interaction.response.edit_message(content="Done. Previous cancelled and new request submitted.", view=None)
        self.stop()

    @discord.ui.button(label="Keep previous", style=discord.ButtonStyle.secondary)
    async def keep(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Not your action.", ephemeral=True)
            return
        await interaction.response.edit_message(content="Kept your previous current request.", view=None)
        self.stop()


class CancelView(discord.ui.View):
    def __init__(self, user_id: int, req_id: int):
        super().__init__(timeout=120)
        self.user_id = user_id
        self.req_id = req_id

    @discord.ui.button(label="Cancel request", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Not your action.", ephemeral=True)
            return
        db.cancel_request(self.req_id)
        await interaction.response.edit_message(content="Your current request was cancelled.", view=None)
        self.stop()

    @discord.ui.button(label="Keep request", style=discord.ButtonStyle.secondary)
    async def keep(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Not your action.", ephemeral=True)
            return
        await interaction.response.edit_message(content="Kept your current request.", view=None)
        self.stop()


location_choices = [app_commands.Choice(name=loc, value=loc) for loc in LOCATIONS]
buffer_choices = [app_commands.Choice(name=f"{b} minutes", value=b) for b in BUFFER_CHOICES]


@bot.tree.command(name="request", description="Submit hailshare request")
@app_commands.choices(
    from_location=location_choices,
    to_location=location_choices,
    buffer=buffer_choices,
)
@app_commands.describe(
    meetup_date="YYYY-MM-DD (UTC+7)",
    meetup_time="HH:MM in 5-minute interval (UTC+7)",
    from_location="Pickup location",
    to_location="Drop-off location",
    buffer="Matching buffer"
)
async def request_cmd(
    interaction: discord.Interaction,
    meetup_date: str,
    meetup_time: str,
    from_location: app_commands.Choice[str],
    to_location: app_commands.Choice[str],
    buffer: app_commands.Choice[int],
):
    if interaction.guild is None or interaction.guild.id != GUILD_ID:
        await interaction.response.send_message("Use this command in the target server only.", ephemeral=True)
        return

    if from_location.value not in LOCATION_SET or to_location.value not in LOCATION_SET:
        await interaction.response.send_message("Invalid location choice.", ephemeral=True)
        return

    try:
        meetup_dt = parse_meetup_dt(meetup_date, meetup_time)
    except ValueError as e:
        await interaction.response.send_message(f"Invalid datetime: {e}", ephemeral=True)
        return

    prev = db.get_current_request_for_user(interaction.user.id)
    if not prev:
        db.insert_request(interaction.user.id, meetup_dt, from_location.value, to_location.value, buffer.value)
        await interaction.response.send_message("Request submitted.", ephemeral=True)
        return

    prev_dt = datetime.fromisoformat(prev["meetup_dt"])
    if prev_dt < datetime.now(TZ):
        db.cancel_request(prev["id"])
        db.insert_request(interaction.user.id, meetup_dt, from_location.value, to_location.value, buffer.value)
        await interaction.response.send_message(
            "Previous request was in the past, cancelled automatically. New request submitted.",
            ephemeral=True
        )
        return

    view = ReplaceView(
        user_id=interaction.user.id,
        payload={
            "meetup_dt": meetup_dt,
            "from_location": from_location.value,
            "to_location": to_location.value,
            "buffer_minutes": buffer.value,
        }
    )
    await interaction.response.send_message(
        "You already have a hailshare request. Replace it?",
        view=view,
        ephemeral=True
    )


@bot.tree.command(name="my_request", description="Show and optionally cancel your current request")
async def my_request(interaction: discord.Interaction):
    if interaction.guild is None or interaction.guild.id != GUILD_ID:
        await interaction.response.send_message("Use this command in the target server only.", ephemeral=True)
        return

    r = db.latest_current_request(interaction.user.id)
    if not r:
        await interaction.response.send_message("You don't have any current request.", ephemeral=True)
        return

    view = CancelView(interaction.user.id, r["id"])
    await interaction.response.send_message(
        f"{request_text(r)}\n\nDo you want to cancel it?",
        view=view,
        ephemeral=True
    )


@bot.tree.command(name="leave_trio", description="Leave your active trio private channel")
async def leave_cmd(interaction: discord.Interaction):
    if interaction.guild is None or interaction.guild.id != GUILD_ID:
        await interaction.response.send_message("Use this command in the target server only.", ephemeral=True)
        return

    guild = interaction.guild
    user = interaction.user

    if not db.try_acquire_lock("matching_engine", ttl_seconds=10):
        await interaction.response.send_message("System is busy, please retry in a few seconds.", ephemeral=True)
        return

    ch = await find_user_active_trio_channel(guild, user.id)
    if not ch:
        await interaction.response.send_message("You are not in an active trio channel.", ephemeral=True)
        return

    try:
        await ch.set_permissions(user, overwrite=None, reason="User used /leave")
        final_members = [m.id for m in ch.members]

        meetup = parse_channel_meetup(ch.name)
        route = db.get_channel_route(ch.id)
        route_from = route[0] if route else None
        route_to = route[1] if route else None

        if meetup:
            db.upsert_channel(ch.id, ch.name, meetup, route_from=route_from, route_to=route_to)

        db.sync_channel_members_incremental(ch.id, final_members)
        db.add_channel_event(ch.id, "update", final_members)

        await interaction.response.send_message(f"You left {ch.mention}.", ephemeral=True)

    except Exception as e:
        await interaction.response.send_message(f"Failed to leave channel: {e}", ephemeral=True)

@tasks.loop(minutes=3)
async def cleanup_task():
    await bot.wait_until_ready()
    db.cancel_past_current_requests()

    guild = bot.get_guild(GUILD_ID)
    if not guild:
        return

    now = datetime.now(TZ)
    for ch in guild.text_channels:
        if not ch.name.startswith(CHANNEL_PREFIX):
            continue
        meetup = parse_channel_meetup(ch.name)
        if not meetup:
            continue
        if meetup + timedelta(minutes=MAX_BUFFER) < now:
            try:
                await ch.delete(reason="hailshare expired channel cleanup")
            except Exception:
                pass


@tasks.loop(minutes=1)
async def fill_existing_channels_task():
    await bot.wait_until_ready()
    if not db.try_acquire_lock("matching_engine", ttl_seconds=50):
        return

    guild = bot.get_guild(GUILD_ID)
    if not guild:
        return

    for ch in guild.text_channels:
        if not ch.name.startswith(CHANNEL_PREFIX):
            continue

        members = ch.members
        if len(members) >= 3 or len(members) == 0:
            continue

        meetup = parse_channel_meetup(ch.name)
        if meetup is None:
            continue

        route = db.get_channel_route(ch.id)
        if route is None:
            inferred = None
            for m in members:
                rr = db.conn.execute(
                    """
                    SELECT from_location, to_location
                    FROM requests
                    WHERE user_id=? AND status='matched'
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    (str(m.id),),
                ).fetchone()
                if rr:
                    inferred = (rr["from_location"], rr["to_location"])
                    break
            route = inferred

        if route is None:
            continue

        route_from, route_to = route
        candidates = db.list_current_requests()
        existing_ids = {m.id for m in members}
        selected = None

        for r in candidates:
            uid = int(r["user_id"])
            if uid in existing_ids:
                continue
            if r["from_location"] != route_from or r["to_location"] != route_to:
                continue
            if not await user_eligible_for_channel(guild, uid):
                continue

            r_dt = datetime.fromisoformat(r["meetup_dt"])
            if not within_user_buffer(r_dt, meetup, int(r["buffer_minutes"])):
                continue

            selected = r
            break

        if not selected:
            continue

        member = guild.get_member(int(selected["user_id"]))
        if not member:
            continue

        try:
            await ch.set_permissions(
                member,
                view_channel=True,
                send_messages=True,
                read_message_history=True
            )
            db.set_matched(selected["id"])

            final_members = [m.id for m in ch.members]
            db.upsert_channel(ch.id, ch.name, meetup, route_from=route_from, route_to=route_to)
            db.sync_channel_members_incremental(ch.id, final_members)
            db.add_channel_event(ch.id, "update", final_members)
        except Exception:
            continue


@tasks.loop(minutes=1)
async def create_channels_task():
    await bot.wait_until_ready()
    if not db.try_acquire_lock("matching_engine", ttl_seconds=50):
        return

    guild = bot.get_guild(GUILD_ID)
    if not guild:
        return

    category = guild.get_channel(HAILSHARE_CATEGORY_ID) if HAILSHARE_CATEGORY_ID else None

    current = db.list_current_requests()
    if not current:
        return

    groups: Dict[Tuple[str, str], List[sqlite3.Row]] = {}
    for r in current:
        route_key = (r["from_location"], r["to_location"])
        groups.setdefault(route_key, []).append(r)

    for key, rows in groups.items():
        if len(rows) < 3:
            continue
        rows = sorted(rows, key=lambda x: x["created_at"])
        used = set()
        for i in range(len(rows)):
            if i in used:
                continue
        
            a = rows[i]
            a_dt = datetime.fromisoformat(a["meetup_dt"])
            a_buf = int(a["buffer_minutes"])
        
            found = None
            for j in range(i + 1, len(rows)):
                if j in used:
                    continue
                b = rows[j]
                b_dt = datetime.fromisoformat(b["meetup_dt"])
                b_buf = int(b["buffer_minutes"])
        
                for k in range(j + 1, len(rows)):
                    if k in used:
                        continue
                    c = rows[k]
                    c_dt = datetime.fromisoformat(c["meetup_dt"])
                    c_buf = int(c["buffer_minutes"])
        
                    dts = sorted([a_dt, b_dt, c_dt])
                    median_dt = dts[1]
        
                    if not within_user_buffer(a_dt, median_dt, a_buf):
                        continue
                    if not within_user_buffer(b_dt, median_dt, b_buf):
                        continue
                    if not within_user_buffer(c_dt, median_dt, c_buf):
                        continue
        
                    found = (j, k, median_dt)
                    break
                if found:
                    break
        
            if not found:
                continue
        
            j, k, median_dt = found
            trio = [a, rows[j], rows[k]]
            user_ids = [int(x["user_id"]) for x in trio]
        
            if any([not await user_eligible_for_channel(guild, uid) for uid in user_ids]):
                continue
        
            members = [guild.get_member(uid) for uid in user_ids]
            if any(m is None for m in members):
                continue
        
            channel_name = build_channel_name(median_dt)
            overwrites = {guild.default_role: discord.PermissionOverwrite(view_channel=False)}
            for m in members:
                overwrites[m] = discord.PermissionOverwrite(
                    view_channel=True, send_messages=True, read_message_history=True
                )
        
            try:
                ch = await guild.create_text_channel(
                    name=channel_name,
                    category=category if isinstance(category, discord.CategoryChannel) else None,
                    overwrites=overwrites,
                    reason="hailshare matched trio with per-user buffer"
                )
        
                greeting = (
                    "🚕 **Hailshare matched!**\n"
                    f"- Request date: {median_dt.strftime('%Y-%m-%d')}\n"
                    f"- Median time: {median_dt.strftime('%H:%M')} (UTC+7)\n"
                    f"- From: {trio[0]['from_location']}\n"
                    f"- To: {trio[0]['to_location']}\n"
                    "\nPlease coordinate your meetup in this private channel (e.g. exact meetup point, who does car-hailing and who pay by cash, how to recognize each other, etc.). Should you decide to cancel, use /leave_trio.\n"
                    f"\nThis channel will be active until {MAX_BUFFER} minutes after the meetup time. After that, it may be deleted or archived.\n"
                )
                await ch.send(greeting)
        
                db.upsert_channel(ch.id, ch.name, median_dt, route_from=trio[0]['from_location'], route_to=trio[0]['to_location'])
                db.replace_channel_members(ch.id, user_ids)
                db.add_channel_event(ch.id, "create", user_ids)
        
                for x in trio:
                    db.set_matched(x["id"])
        
                used.update({i, j, k})
        
            except Exception as e:
                print(f"Error creating channel: {e}")
                continue


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} ({bot.user.id})")
    if GUILD_ID:
        guild_obj = discord.Object(id=GUILD_ID)
        try:
            await bot.tree.sync(guild=guild_obj)
            print(f"Synced slash commands to guild {GUILD_ID}")
        except Exception as e:
            print("Sync error:", e)

    if not cleanup_task.is_running():
        cleanup_task.start()
    if not fill_existing_channels_task.is_running():
        fill_existing_channels_task.start()
    if not create_channels_task.is_running():
        create_channels_task.start()
    keep_alive()


if __name__ == "__main__":
    #start_health_check()
    bot.run(BOT_TOKEN)
