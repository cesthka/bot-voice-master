import discord
from discord.ext import commands, tasks
import os
import sys
import sqlite3
import json
import logging
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

# ========================= CONFIG =========================
BOT_TOKEN = os.environ.get("TOKEN")
if not BOT_TOKEN:
    print("[ERREUR CRITIQUE] La variable d'environnement TOKEN n'est pas définie.")
    sys.exit(1)

PARIS_TZ = ZoneInfo("Europe/Paris")
DEFAULT_BUYER_IDS = [923200874669563914, 142365250803466240]
DEFAULT_PREFIX = "="

# Limites de laisses par rang : {rang: nombre_max}
DEFAULT_LEASH_LIMITS = {"1": 1, "2": 2, "3": 5, "4": 999}

# Volume persistant Railway : DATA_DIR doit pointer vers un dossier persistant
DATA_DIR = os.environ.get("DATA_DIR")
if not DATA_DIR:
    print("[ERREUR CRITIQUE] DATA_DIR non défini. Configure DATA_DIR=/data dans Railway.")
    sys.exit(1)
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "vm_bot.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)
log = logging.getLogger("vm")

# Cache du prefix (évite d'ouvrir SQLite à chaque message)
_prefix_cache = {"value": None}


# ========================= DATABASE =========================

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()

    c.execute("""CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY, value TEXT
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS ranks (
        user_id TEXT PRIMARY KEY, rank INTEGER NOT NULL
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS leashes (
        target_id TEXT PRIMARY KEY,
        owner_id TEXT NOT NULL,
        original_nick TEXT
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS private_vcs (
        channel_id TEXT PRIMARY KEY,
        owner_id TEXT NOT NULL,
        guild_id TEXT NOT NULL
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS vc_access (
        channel_id TEXT,
        user_id TEXT,
        PRIMARY KEY (channel_id, user_id)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS log_channels (
        guild_id TEXT PRIMARY KEY, channel_id TEXT NOT NULL
    )""")

    # Système =auto (peak vocal quotidien / palmarès)
    c.execute("""CREATE TABLE IF NOT EXISTS auto_channels (
        guild_id TEXT PRIMARY KEY, channel_id TEXT NOT NULL
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS auto_peaks (
        guild_id TEXT PRIMARY KEY,
        day TEXT NOT NULL,
        peak INTEGER NOT NULL,
        message_id TEXT
    )""")

    c.execute("INSERT OR IGNORE INTO config VALUES ('prefix', ?)", (DEFAULT_PREFIX,))
    c.execute("INSERT OR REPLACE INTO config VALUES ('buyer_ids', ?)",
              (json.dumps([str(i) for i in DEFAULT_BUYER_IDS]),))
    c.execute("INSERT OR IGNORE INTO config VALUES ('leash_limits', ?)",
              (json.dumps(DEFAULT_LEASH_LIMITS),))

    conn.commit()
    conn.close()


def get_config(key):
    conn = get_db()
    row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else None


def set_config(key, value):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO config VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()
    if key == "prefix":
        _prefix_cache["value"] = str(value)


# ---- Limites de laisses ----
def get_leash_limits():
    raw = get_config("leash_limits")
    if not raw:
        return dict(DEFAULT_LEASH_LIMITS)
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return dict(DEFAULT_LEASH_LIMITS)


def get_leash_limit_for_rank(rank):
    return int(get_leash_limits().get(str(rank), 0))


def set_leash_limit_for_rank(rank, limit):
    limits = get_leash_limits()
    limits[str(rank)] = int(limit)
    set_config("leash_limits", json.dumps(limits))


def get_prefix_cached():
    if _prefix_cache["value"] is None:
        _prefix_cache["value"] = get_config("prefix") or DEFAULT_PREFIX
    return _prefix_cache["value"]


def get_rank_db(user_id):
    buyer_ids_raw = get_config("buyer_ids")
    if buyer_ids_raw:
        buyer_ids = json.loads(buyer_ids_raw)
        if str(user_id) in buyer_ids:
            return 4
    conn = get_db()
    row = conn.execute("SELECT rank FROM ranks WHERE user_id = ?", (str(user_id),)).fetchone()
    conn.close()
    return row["rank"] if row else 0


def set_rank_db(user_id, rank):
    conn = get_db()
    if rank == 0:
        conn.execute("DELETE FROM ranks WHERE user_id = ?", (str(user_id),))
    else:
        conn.execute("INSERT OR REPLACE INTO ranks VALUES (?, ?)", (str(user_id), rank))
    conn.commit()
    conn.close()


def get_ranks_by_level(level):
    conn = get_db()
    rows = conn.execute("SELECT user_id FROM ranks WHERE rank = ?", (level,)).fetchall()
    conn.close()
    return [r["user_id"] for r in rows]


def get_log_channel(guild_id):
    conn = get_db()
    row = conn.execute("SELECT channel_id FROM log_channels WHERE guild_id = ?", (str(guild_id),)).fetchone()
    conn.close()
    return row["channel_id"] if row else None


def set_log_channel(guild_id, channel_id):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO log_channels VALUES (?, ?)", (str(guild_id), str(channel_id)))
    conn.commit()
    conn.close()


# ---- Leashes ----
def add_leash(target_id, owner_id, original_nick):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO leashes VALUES (?, ?, ?)",
                 (str(target_id), str(owner_id), original_nick))
    conn.commit()
    conn.close()


def remove_leash(target_id):
    conn = get_db()
    conn.execute("DELETE FROM leashes WHERE target_id = ?", (str(target_id),))
    conn.commit()
    conn.close()


def get_leash(target_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM leashes WHERE target_id = ?", (str(target_id),)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_leashes_by_owner(owner_id):
    conn = get_db()
    rows = conn.execute("SELECT * FROM leashes WHERE owner_id = ?", (str(owner_id),)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---- Private VCs ----
def add_private_vc(channel_id, owner_id, guild_id):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO private_vcs VALUES (?, ?, ?)",
                 (str(channel_id), str(owner_id), str(guild_id)))
    conn.commit()
    conn.close()


def remove_private_vc(channel_id):
    conn = get_db()
    conn.execute("DELETE FROM private_vcs WHERE channel_id = ?", (str(channel_id),))
    conn.execute("DELETE FROM vc_access WHERE channel_id = ?", (str(channel_id),))
    conn.commit()
    conn.close()


def get_private_vc(channel_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM private_vcs WHERE channel_id = ?", (str(channel_id),)).fetchone()
    conn.close()
    return dict(row) if row else None


def add_vc_access(channel_id, user_id):
    conn = get_db()
    conn.execute("INSERT OR IGNORE INTO vc_access VALUES (?, ?)", (str(channel_id), str(user_id)))
    conn.commit()
    conn.close()


def remove_vc_access(channel_id, user_id):
    conn = get_db()
    conn.execute("DELETE FROM vc_access WHERE channel_id = ? AND user_id = ?",
                 (str(channel_id), str(user_id)))
    conn.commit()
    conn.close()


def get_vc_access(channel_id):
    conn = get_db()
    rows = conn.execute("SELECT user_id FROM vc_access WHERE channel_id = ?", (str(channel_id),)).fetchall()
    conn.close()
    return [r["user_id"] for r in rows]


# ---- Auto peak ----
def set_auto_channel(guild_id, channel_id):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO auto_channels VALUES (?, ?)", (str(guild_id), str(channel_id)))
    # On repart à zéro sur le suivi du peak quand on (re)définit le salon
    conn.execute("DELETE FROM auto_peaks WHERE guild_id = ?", (str(guild_id),))
    conn.commit()
    conn.close()


def get_auto_channel(guild_id):
    conn = get_db()
    row = conn.execute("SELECT channel_id FROM auto_channels WHERE guild_id = ?", (str(guild_id),)).fetchone()
    conn.close()
    return row["channel_id"] if row else None


def remove_auto_channel(guild_id):
    conn = get_db()
    conn.execute("DELETE FROM auto_channels WHERE guild_id = ?", (str(guild_id),))
    conn.execute("DELETE FROM auto_peaks WHERE guild_id = ?", (str(guild_id),))
    conn.commit()
    conn.close()


def get_auto_peak(guild_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM auto_peaks WHERE guild_id = ?", (str(guild_id),)).fetchone()
    conn.close()
    return dict(row) if row else None


def set_auto_peak(guild_id, day, peak, message_id):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO auto_peaks VALUES (?, ?, ?, ?)",
                 (str(guild_id), day, int(peak), str(message_id) if message_id else None))
    conn.commit()
    conn.close()


# ========================= HELPERS =========================

def rank_name(level):
    return {4: "Buyer", 3: "Sys", 2: "Owner", 1: "Whitelist", 0: "Aucun"}[level]


def has_min_rank(user_id, minimum):
    return get_rank_db(user_id) >= minimum


def embed_color():
    return 0x2b2d31


def success_embed(title, desc=""):
    return discord.Embed(title=title, description=desc, color=0x43b581)


def error_embed(title, desc=""):
    return discord.Embed(title=title, description=desc, color=0xf04747)


def info_embed(title, desc=""):
    return discord.Embed(title=title, description=desc, color=embed_color())


# ---- Date FR ----
JOURS_FR = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
MOIS_FR = ["janvier", "février", "mars", "avril", "mai", "juin",
           "juillet", "août", "septembre", "octobre", "novembre", "décembre"]


def french_now():
    return datetime.now(PARIS_TZ)


def french_day_key(now=None):
    now = now or french_now()
    return now.strftime("%Y-%m-%d")


def french_day_label(now=None):
    now = now or french_now()
    return f"{JOURS_FR[now.weekday()]} {now.day} {MOIS_FR[now.month - 1]} {now.year}"


def is_public_vc(channel):
    """Vérifie si une voc est publique (accessible @everyone)"""
    if not isinstance(channel, discord.VoiceChannel):
        return False
    everyone = channel.guild.default_role
    perms = channel.permissions_for(everyone)
    return perms.connect and not channel.user_limit == 1


# ---- Stats vocales ----
def compute_stats(guild):
    total_members = guild.member_count
    total_boosts = guild.premium_subscription_count

    all_vc_members = []
    for vc in guild.voice_channels:
        all_vc_members.extend(vc.members)

    online = sum(1 for m in guild.members if m.status != discord.Status.offline and not m.bot)
    total_in_vc = sum(1 for m in all_vc_members if not m.bot)
    streaming = sum(1 for m in all_vc_members if m.voice and m.voice.self_stream and not m.bot)

    return {
        "members": total_members,
        "online": online,
        "in_vc": total_in_vc,
        "streaming": streaming,
        "boosts": total_boosts,
    }


def build_stats_embed(guild, peak_day_label=None):
    s = compute_stats(guild)

    if peak_day_label:
        header = f"# 🏆 Peak du {peak_day_label}"
    else:
        header = f"# {guild.name} Statistiques !"

    em = discord.Embed(color=embed_color())
    em.description = (
        f"{header}\n\n"
        f"*Membres* **: {s['members']:,}**\n"
        f"*En ligne* **: {s['online']:,}**\n"
        f"*En Vocal* **: {s['in_vc']:,}**\n"
        f"*En stream* **: {s['streaming']:,}**\n"
        f"*Boost* **: {s['boosts']:,}**"
    )
    if guild.icon:
        em.set_thumbnail(url=guild.icon.url)
    return em


# ========================= RESOLVE USER (ex-membres OK) =========================

async def resolve_user_or_id(ctx, user_input):
    """
    Retourne (display_obj, user_id) — marche même si la personne n'est plus sur le serveur.
    """
    if not user_input:
        return None, None

    raw = user_input.strip()
    cleaned = raw.strip("<@!>")

    user_id = None
    try:
        user_id = int(cleaned)
    except ValueError:
        try:
            m = await commands.MemberConverter().convert(ctx, raw)
            return m, m.id
        except commands.CommandError:
            pass
        try:
            u = await commands.UserConverter().convert(ctx, raw)
            return u, u.id
        except commands.CommandError:
            return None, None

    if ctx.guild:
        member = ctx.guild.get_member(user_id)
        if member:
            return member, user_id

    try:
        user = await bot.fetch_user(user_id)
        return user, user_id
    except discord.NotFound:
        return None, user_id
    except discord.HTTPException as e:
        log.warning(f"resolve_user_or_id: fetch_user({user_id}) a échoué : {e}")
        return None, user_id


def format_user_display(display_obj, user_id):
    if display_obj is not None:
        return f"{display_obj.mention} (`{display_obj.id}`)"
    return f"<@{user_id}> (`{user_id}`) *(hors serveur)*"


async def resolve_member(ctx, user_input):
    """
    Résout un MEMBRE présent sur le serveur via mention, <@id>, ID brut ou nom.
    Retourne le Member, ou None s'il n'est pas (ou plus) sur le serveur.
    Utilisé pour les commandes qui agissent réellement en vocal (mv, join, find...).
    """
    if not user_input or not ctx.guild:
        return None
    raw = user_input.strip()
    cleaned = raw.strip("<@!>")

    # 1) ID brut ou mention <@id>
    try:
        uid = int(cleaned)
        return ctx.guild.get_member(uid)  # None si pas sur le serveur
    except ValueError:
        pass

    # 2) Nom / pseudo via le converter classique
    try:
        return await commands.MemberConverter().convert(ctx, raw)
    except commands.CommandError:
        return None


# ========================= BOT SETUP =========================

init_db()
intents = discord.Intents.all()


def get_prefix(bot, message):
    return get_prefix_cached()


bot = commands.Bot(command_prefix=get_prefix, intents=intents, help_command=None)


# ========================= LOG =========================

async def send_log(guild, action, author, target_display=None, target_id=None, desc=None, color=0x2b2d31):
    channel_id = get_log_channel(guild.id)
    if not channel_id:
        return
    channel = guild.get_channel(int(channel_id))
    if not channel:
        return
    em = discord.Embed(title=f"📋 {action}", color=color)
    em.add_field(name="Modérateur", value=f"{author.mention} (`{author.id}`)", inline=True)
    if target_id is not None:
        em.add_field(name="Cible", value=format_user_display(target_display, target_id), inline=True)
    if desc:
        em.add_field(name="Détail", value=desc, inline=False)
    try:
        await channel.send(embed=em)
    except discord.HTTPException as e:
        log.warning(f"send_log: échec d'envoi : {e}")


# ========================= EVENTS =========================

@bot.event
async def on_ready():
    log.info(f"Bot connecté : {bot.user} ({bot.user.id})")
    log.info(f"Prefix : {get_prefix_cached()}")
    if not leash_follow.is_running():
        leash_follow.start()
    if not auto_peak_loop.is_running():
        auto_peak_loop.start()


@bot.event
async def on_voice_state_update(member, before, after):
    # Auto-unpv : si le proprio d'une voc privée quitte CE salon, elle redevient publique
    if before.channel and before.channel != after.channel:
        pvc = get_private_vc(before.channel.id)
        if pvc and str(member.id) == pvc["owner_id"]:
            try:
                await before.channel.set_permissions(member.guild.default_role, connect=None)
            except discord.HTTPException:
                pass
            remove_private_vc(before.channel.id)
            await send_log(member.guild, "Salon auto-public", member,
                           desc=f"Le proprio a quitté : {before.channel.name}", color=0x43b581)

    # Leash follow : si le owner rejoint une voc, les leashs le rejoignent
    leashes = get_leashes_by_owner(member.id)
    if leashes and after.channel and after.channel != before.channel:
        guild = member.guild
        for leash in leashes:
            target = guild.get_member(int(leash["target_id"]))
            if target and target.voice and target.voice.channel != after.channel:
                try:
                    await target.move_to(after.channel)
                except discord.HTTPException:
                    pass

    # Vérif accès voc privée
    if after.channel:
        pvc = get_private_vc(after.channel.id)
        if pvc:
            allowed = get_vc_access(after.channel.id)
            allowed.append(pvc["owner_id"])
            if str(member.id) not in allowed:
                try:
                    await member.move_to(None)
                    try:
                        await member.send(embed=error_embed("🔒 Salon privé", "Tu n'as pas accès à ce salon vocal."))
                    except discord.HTTPException:
                        pass
                except discord.HTTPException:
                    pass


@tasks.loop(seconds=5)
async def leash_follow():
    """Boucle de suivi des leashs"""
    for guild in bot.guilds:
        conn = get_db()
        rows = conn.execute("SELECT * FROM leashes").fetchall()
        conn.close()
        for row in rows:
            owner = guild.get_member(int(row["owner_id"]))
            target = guild.get_member(int(row["target_id"]))
            if owner and target:
                if owner.voice and target.voice:
                    if owner.voice.channel != target.voice.channel:
                        try:
                            await target.move_to(owner.voice.channel)
                        except discord.HTTPException:
                            pass


@tasks.loop(seconds=30)
async def auto_peak_loop():
    """
    Palmarès quotidien : poste l'embed de stats au pic de vocaux du jour.
    - Dès qu'un nouveau record du jour est atteint -> supprime l'ancien message du jour, en reposte un.
    - À minuit (heure FR) on change de jour : l'ancien message reste (archive), on repart à zéro.
    """
    today = french_day_key()
    for guild in bot.guilds:
        ch_id = get_auto_channel(guild.id)
        if not ch_id:
            continue
        channel = guild.get_channel(int(ch_id))
        if channel is None:
            continue

        stats = compute_stats(guild)
        count = stats["in_vc"]
        peak = get_auto_peak(guild.id)

        # Nouveau jour (ou première fois) : on garde l'ancien message, on repart à zéro
        if peak is None or peak["day"] != today:
            if count > 0:
                try:
                    msg = await channel.send(embed=build_stats_embed(guild, peak_day_label=french_day_label()))
                    set_auto_peak(guild.id, today, count, msg.id)
                except discord.HTTPException:
                    pass
            else:
                set_auto_peak(guild.id, today, 0, None)
            continue

        # Même jour : on ne reposte QUE si on bat le record du jour
        if count > peak["peak"]:
            if peak["message_id"]:
                try:
                    old = await channel.fetch_message(int(peak["message_id"]))
                    await old.delete()
                except discord.HTTPException:
                    pass
            try:
                msg = await channel.send(embed=build_stats_embed(guild, peak_day_label=french_day_label()))
                set_auto_peak(guild.id, today, count, msg.id)
            except discord.HTTPException:
                pass


@auto_peak_loop.before_loop
async def before_auto_peak():
    await bot.wait_until_ready()


# ========================= HELP SYSTEM (filtré par rang) =========================

# Rangs : 0 = Aucun, 1 = WL, 2 = Owner, 3 = Sys, 4 = Buyer

HELP_CATEGORIES = {
    "vocal": {
        "emoji": "🎙️",
        "label": "Vocal",
        "title": "Vocal",
        "subtitle": "Gérer les membres en vocal (déplacer, trouver, stats).",
        "sections": [
            ("👥", "Gérer les membres", [
                ("mv @user [#salon]", "Déplacer un user (ta voc si #salon omis)", 1),
                ("join @user",        "Te déplacer dans la voc d'un membre",      1),
                ("bringall",          "Ramener tout le monde dans ta voc",        1),
            ]),
            ("🔍", "Voir & stats", [
                ("find @user",        "Trouver un user en vocal",                 1),
                ("voc / vc",          "Stats vocales du serveur",                 0),
            ]),
        ],
    },
    "prive": {
        "emoji": "🔒",
        "label": "Salons Privés",
        "title": "Salons Privés",
        "subtitle": "Rendre ta voc privée et gérer les accès.",
        "sections": [
            ("🔒", "Privatiser", [
                ("pv",               "Rendre ta voc actuelle privée",       1),
                ("unpv",             "Retirer le privé de ta voc",          1),
                ("unpv <id_salon>",  "Retirer le privé d'un autre salon",   1),
            ]),
            ("👤", "Accès", [
                ("acces @user",      "Donner accès à ta voc privée",        1),
            ]),
        ],
    },
    "laisse": {
        "emoji": "🐕",
        "label": "Laisse",
        "title": "Laisse",
        "subtitle": "Système de laisse : force un membre à te suivre en vocal.",
        "sections": [
            ("🐕", "Gérer la laisse", [
                ("laisse @user / id", "Mettre OU retirer la laisse (toggle)", 1),
                ("laisse",            "Voir ta liste de laisses",             1),
            ]),
        ],
    },
    "perms": {
        "emoji": "👥",
        "label": "Permissions",
        "title": "Permissions",
        "subtitle": "Gérer les rangs du bot (wl, owner, sys).",
        "sections": [
            ("✨", "Whitelist (Owner+)", [
                ("wl @user / unwl @user",       "Gérer la whitelist",   2),
                ("wl",                          "Lister les WL",        2),
            ]),
            ("⭐", "Owner (Sys+)", [
                ("owner @user / unowner @user", "Gérer les owners",     3),
                ("owner",                       "Lister les owners",    3),
            ]),
            ("🔧", "Sys (Buyer)", [
                ("sys @user / unsys @user",     "Gérer les sys",        4),
                ("sys",                         "Lister les sys",       4),
            ]),
        ],
    },
    "system": {
        "emoji": "🛠️",
        "label": "Système",
        "title": "Système",
        "subtitle": "Configuration du bot (prefix, logs, limites, auto).",
        "sections": [
            ("⚙️", "Buyer only", [
                ("prefix [nouveau]",  "Changer le prefix",                 4),
                ("setlog #salon",     "Salon de logs",                     4),
                ("limite",            "Modifier les limites de laisses",   4),
                ("auto #salon / off", "Peak vocal quotidien (palmarès)",   4),
            ]),
        ],
    },
    "hierarchy": {
        "emoji": "📋",
        "label": "Hiérarchie",
        "title": "Hiérarchie",
        "subtitle": "Les différents rangs du bot et leurs pouvoirs.",
        "min_rank": 1,
        "items": [],
    },
}


def _vm_accessible_sections(category_key, rank):
    cat = HELP_CATEGORIES.get(category_key, {})
    result = []
    for section in cat.get("sections", []):
        emoji, title, items = section
        visible = [(syn, desc) for (syn, desc, mr) in items if rank >= mr]
        if visible:
            result.append((emoji, title, visible))
    return result


def _vm_accessible_items(category_key, rank):
    cat = HELP_CATEGORIES.get(category_key, {})
    result = []
    for section in cat.get("sections", []):
        _e, _t, items = section
        for (syn, desc, mr) in items:
            if rank >= mr:
                result.append((syn, desc))
    return result


def help_category_visible(category_key, rank):
    cat = HELP_CATEGORIES.get(category_key, {})
    if "min_rank" in cat:
        return rank >= cat["min_rank"]
    return len(_vm_accessible_items(category_key, rank)) > 0


def _vm_apply_thumbnail(em, guild):
    if guild and getattr(guild, "icon", None):
        try:
            em.set_thumbnail(url=guild.icon.url)
        except (AttributeError, TypeError):
            pass


def build_vm_category_embed(category_key, rank, guild=None):
    p = get_prefix_cached()
    cat = HELP_CATEGORIES[category_key]
    emoji = cat.get("emoji", "📋")
    title = cat.get("title", "Commandes")
    subtitle = cat.get("subtitle", "")

    em = discord.Embed(
        title=f"{emoji}  {title}",
        description=subtitle if subtitle else None,
        color=embed_color(),
    )
    _vm_apply_thumbnail(em, guild)

    sections = _vm_accessible_sections(category_key, rank)
    if not sections:
        em.add_field(
            name="⛔ Aucune commande accessible",
            value="Tu n'as pas les permissions pour cette catégorie.",
            inline=False,
        )
    else:
        for s_emoji, s_title, s_items in sections:
            cmd_lines = [f"`{p}{syntax}` — {desc}" for syntax, desc in s_items]
            em.add_field(
                name=f"{s_emoji} {s_title}",
                value="\n".join(cmd_lines),
                inline=False,
            )

    if category_key == "laisse":
        em.add_field(
            name="💡 Fonctionnement",
            value=(
                "`=laisse @user` met la laisse. Refais `=laisse @user` pour la **retirer**.\n"
                "La personne suit **automatiquement** le propriétaire en vocal.\n"
                "Son pseudo devient : `pseudo (🐕 de [toi])`"
            ),
            inline=False,
        )

    return em


def build_vm_hierarchy_embed(rank, guild=None):
    em = discord.Embed(
        title="📋  Hiérarchie",
        description="Les différents rangs du bot et leurs pouvoirs.",
        color=embed_color(),
    )
    _vm_apply_thumbnail(em, guild)

    levels = [
        (4, "👑", "Buyer",      "Accès total, gère les Sys"),
        (3, "🔧", "Sys",        "Gère Owner/WL, gère les unpv/unleash de tout le monde"),
        (2, "⭐", "Owner",       "Gère les WL"),
        (1, "✨", "Whitelist",   "Accès aux commandes vocales, privé et laisse"),
        (0, "👤", "Aucun",       "Peut voir les stats vocales uniquement"),
    ]
    for lvl, icon, name, desc in levels:
        marker = "  ← **toi**" if lvl == rank else ""
        em.add_field(
            name=f"{icon} {name}{marker}",
            value=desc,
            inline=False,
        )

    em.add_field(
        name="ℹ️ Règle importante",
        value="Un rang ne peut **jamais** agir sur quelqu'un de rang égal ou supérieur.",
        inline=False,
    )
    return em


def build_vm_home_embed(rank, guild=None):
    p = get_prefix_cached()
    rank_label = rank_name(rank)

    em = discord.Embed(
        title="🎙️  Panel d'aide — Voice Master",
        description=(
            f"Bot de **gestion vocale** pour Meira.\n"
            f"**Prefix :** `{p}` ・ **Ton rang :** {rank_label}\n\n"
            f"*Choisis une catégorie ci-dessous pour voir ses commandes.*"
        ),
        color=embed_color(),
    )
    _vm_apply_thumbnail(em, guild)

    category_descs = {
        "vocal":      "Déplacer, trouver, stats vocales",
        "prive":      "Salons privés, accès",
        "laisse":     "Mettre/retirer des laisses",
        "perms":      "Gérer les rangs (wl, owner, sys)",
        "system":     "Config du bot (prefix, logs, limites, auto)",
        "hierarchy":  "Qui peut faire quoi",
    }

    user_keys  = ["vocal", "prive", "laisse"]
    admin_keys = ["perms", "system", "hierarchy"]

    user_lines = []
    for key in user_keys:
        if help_category_visible(key, rank):
            cat = HELP_CATEGORIES[key]
            user_lines.append(f"{cat['emoji']} **{cat['label']}** — {category_descs[key]}")
    if user_lines:
        em.add_field(name="🎮 Pour toi", value="\n".join(user_lines), inline=False)

    admin_lines = []
    for key in admin_keys:
        if help_category_visible(key, rank):
            cat = HELP_CATEGORIES[key]
            admin_lines.append(f"{cat['emoji']} **{cat['label']}** — {category_descs[key]}")
    if admin_lines:
        em.add_field(name="🛠️ Staff & Admin", value="\n".join(admin_lines), inline=False)

    return em


def build_vm_embed_for(key, rank, guild=None):
    if key == "home":
        return build_vm_home_embed(rank, guild=guild)
    if key == "hierarchy":
        return build_vm_hierarchy_embed(rank, guild=guild)
    return build_vm_category_embed(key, rank, guild=guild)


class HelpDropdown(discord.ui.Select):
    def __init__(self, rank, guild=None):
        self.rank = rank
        self.guild = guild
        options = [discord.SelectOption(label="Accueil", emoji="🏠", value="home")]
        for key, cat in HELP_CATEGORIES.items():
            if help_category_visible(key, rank):
                options.append(discord.SelectOption(
                    label=cat["label"], emoji=cat["emoji"], value=key
                ))
        super().__init__(
            placeholder="📂 Choisis une catégorie...",
            min_values=1, max_values=1, options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        key = self.values[0]
        if key != "home" and not help_category_visible(key, self.rank):
            return await interaction.response.send_message(
                "Tu n'as pas accès à cette catégorie.", ephemeral=True
            )
        await interaction.response.edit_message(
            embed=build_vm_embed_for(key, self.rank, guild=self.guild),
            view=self.view,
        )


class HelpView(discord.ui.View):
    def __init__(self, author_id, rank, guild=None):
        super().__init__(timeout=120)
        self.author_id = author_id
        self.rank = rank
        self.guild = guild
        self.add_item(HelpDropdown(rank, guild=guild))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "Ce menu n'est pas à toi. Fais `=help` pour voir le tien.", ephemeral=True
            )
            return False
        return True

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


@bot.command(name="help")
async def _help(ctx):
    rank = get_rank_db(ctx.author.id)
    view = HelpView(ctx.author.id, rank, guild=ctx.guild)
    await ctx.send(embed=build_vm_home_embed(rank, guild=ctx.guild), view=view)


# ========================= SYSTÈME =========================

@bot.command(name="prefix")
async def _prefix(ctx, new_prefix: str = None):
    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le **Buyer** peut changer le prefix."))
    if not new_prefix:
        return await ctx.send(embed=info_embed("Prefix actuel", f"`{get_prefix_cached()}`"))
    set_config("prefix", new_prefix)
    await ctx.send(embed=success_embed("✅ Prefix modifié", f"Nouveau prefix : `{new_prefix}`"))


@bot.command(name="setlog")
async def _setlog(ctx, channel: discord.TextChannel = None):
    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le **Buyer** peut définir le salon de logs."))
    if not channel:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un salon."))
    set_log_channel(ctx.guild.id, channel.id)
    await ctx.send(embed=success_embed("✅ Logs configurés", f"Les logs seront envoyés dans {channel.mention}."))


# ---- =auto (palmarès vocal quotidien) ----

@bot.command(name="auto")
async def _auto(ctx, arg: str = None):
    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le **Buyer** peut gérer l'auto-peak."))

    # Statut
    if arg is None:
        ch_id = get_auto_channel(ctx.guild.id)
        if ch_id:
            return await ctx.send(embed=info_embed(
                "📊 Auto-peak activé",
                f"Le peak vocal du jour est posté dans <#{ch_id}>.\n"
                f"`{get_prefix_cached()}auto off` pour désactiver."
            ))
        return await ctx.send(embed=info_embed(
            "📊 Auto-peak désactivé",
            f"Usage : `{get_prefix_cached()}auto #salon` pour l'activer."
        ))

    # Désactivation
    if arg.lower() in ("off", "stop", "disable"):
        remove_auto_channel(ctx.guild.id)
        return await ctx.send(embed=success_embed("✅ Auto-peak désactivé", "Le palmarès quotidien est arrêté."))

    # Activation
    try:
        channel = await commands.TextChannelConverter().convert(ctx, arg)
    except commands.CommandError:
        return await ctx.send(embed=error_embed("❌ Salon introuvable", "Mentionne un salon texte valide."))

    set_auto_channel(ctx.guild.id, channel.id)
    await ctx.send(embed=success_embed(
        "✅ Auto-peak activé",
        f"Chaque jour, le **pic de vocaux** sera posté automatiquement dans {channel.mention}.\n"
        f"Le record du jour est mis à jour quand il est battu, et un nouveau message est créé à minuit (heure FR)."
    ))
    await send_log(ctx.guild, "Auto-peak configuré", ctx.author, desc=f"Salon : {channel.name}", color=0x43b581)


# ---- =limite (éditeur interactif) ----

RANK_LEVELS_EDIT = [(4, "Buyer"), (3, "Sys"), (2, "Owner"), (1, "Whitelist")]


def build_limit_embed(guild=None, editable=True):
    limits = get_leash_limits()
    em = discord.Embed(
        title="📊 Limites de laisses",
        description=("Sélectionne un rang dans le menu pour modifier sa limite."
                     if editable else "Limites de laisses par rang."),
        color=embed_color(),
    )
    for lvl, name in RANK_LEVELS_EDIT:
        em.add_field(name=name, value=f"**{limits.get(str(lvl), 0)}** laisse(s)", inline=True)
    _vm_apply_thumbnail(em, guild)
    return em


class LimitModal(discord.ui.Modal):
    def __init__(self, rank_level, author_id, guild):
        super().__init__(title=f"Limite — {rank_name(rank_level)}")
        self.rank_level = rank_level
        self.author_id = author_id
        self.guild = guild
        self.value_input = discord.ui.TextInput(
            label="Nombre de laisses",
            placeholder="Ex: 3",
            default=str(get_leash_limit_for_rank(rank_level)),
            required=True,
            max_length=4,
        )
        self.add_item(self.value_input)

    async def on_submit(self, interaction: discord.Interaction):
        raw = self.value_input.value.strip()
        if not raw.isdigit():
            return await interaction.response.send_message("❌ Entre un nombre entier positif.", ephemeral=True)
        set_leash_limit_for_rank(self.rank_level, int(raw))
        await interaction.response.edit_message(
            embed=build_limit_embed(self.guild),
            view=LimitView(self.author_id, self.guild),
        )


class LimitSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label=name, value=str(lvl), description=f"Modifier la limite {name}")
            for lvl, name in RANK_LEVELS_EDIT
        ]
        super().__init__(placeholder="Choisis un rang à modifier...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        lvl = int(self.values[0])
        await interaction.response.send_modal(LimitModal(lvl, self.view.author_id, self.view.guild))


class LimitView(discord.ui.View):
    def __init__(self, author_id, guild=None):
        super().__init__(timeout=120)
        self.author_id = author_id
        self.guild = guild
        self.add_item(LimitSelect())

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Ce menu n'est pas à toi.", ephemeral=True)
            return False
        return True

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


@bot.command(name="limite")
async def _limite(ctx):
    if not has_min_rank(ctx.author.id, 4):
        # Lecture seule pour les non-Buyer
        return await ctx.send(embed=build_limit_embed(ctx.guild, editable=False))
    await ctx.send(embed=build_limit_embed(ctx.guild), view=LimitView(ctx.author.id, ctx.guild))


# ========================= RANGS (avec résolution par ID / ex-membres) =========================

@bot.command(name="sys")
async def _sys(ctx, *, user_input: str = None):
    if user_input is None:
        if not has_min_rank(ctx.author.id, 4):
            return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le **Buyer** peut voir la liste sys."))
        ids = get_ranks_by_level(3)
        if not ids:
            return await ctx.send(embed=info_embed("📋 Liste Sys", "Aucun utilisateur sys."))
        return await ctx.send(embed=info_embed(f"📋 Liste Sys ({len(ids)})", "\n".join([f"<@{uid}>" for uid in ids])))

    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le **Buyer** peut ajouter des sys."))

    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    if get_rank_db(uid) == 3:
        return await ctx.send(embed=error_embed("Déjà Sys", f"{format_user_display(display, uid)} est déjà sys."))
    set_rank_db(uid, 3)
    await ctx.send(embed=success_embed("✅ Sys ajouté", f"{format_user_display(display, uid)} a été ajouté en **sys**."))
    await send_log(ctx.guild, "Sys ajouté", ctx.author, display, uid, color=0x43b581)


@bot.command(name="unsys")
async def _unsys(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 4):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le **Buyer** peut retirer des sys."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))

    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    if get_rank_db(uid) != 3:
        return await ctx.send(embed=error_embed("Pas Sys", f"{format_user_display(display, uid)} n'est pas sys."))
    set_rank_db(uid, 0)
    await ctx.send(embed=success_embed("✅ Sys retiré", f"{format_user_display(display, uid)} a été retiré des **sys**."))
    await send_log(ctx.guild, "Sys retiré", ctx.author, display, uid, color=0xfaa61a)


@bot.command(name="owner")
async def _owner(ctx, *, user_input: str = None):
    if user_input is None:
        if not has_min_rank(ctx.author.id, 3):
            return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
        ids = get_ranks_by_level(2)
        if not ids:
            return await ctx.send(embed=info_embed("📋 Liste Owner", "Aucun owner."))
        return await ctx.send(embed=info_embed(f"📋 Liste Owner ({len(ids)})", "\n".join([f"<@{uid}>" for uid in ids])))

    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis pour ajouter des owners."))

    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    if get_rank_db(uid) >= 3:
        return await ctx.send(embed=error_embed("❌ Erreur", f"{format_user_display(display, uid)} a un rang supérieur ou égal."))
    set_rank_db(uid, 2)
    await ctx.send(embed=success_embed("✅ Owner ajouté", f"{format_user_display(display, uid)} a été ajouté en **owner**."))
    await send_log(ctx.guild, "Owner ajouté", ctx.author, display, uid, color=0x43b581)


@bot.command(name="unowner")
async def _unowner(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Sys+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))

    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    if get_rank_db(uid) != 2:
        return await ctx.send(embed=error_embed("Pas Owner", f"{format_user_display(display, uid)} n'est pas owner."))
    set_rank_db(uid, 0)
    await ctx.send(embed=success_embed("✅ Owner retiré", f"{format_user_display(display, uid)} a été retiré des **owners**."))
    await send_log(ctx.guild, "Owner retiré", ctx.author, display, uid, color=0xfaa61a)


@bot.command(name="wl")
async def _wl(ctx, *, user_input: str = None):
    if user_input is None:
        if not has_min_rank(ctx.author.id, 2):
            return await ctx.send(embed=error_embed("❌ Permission refusée", "**Owner+** requis."))
        ids = get_ranks_by_level(1)
        if not ids:
            return await ctx.send(embed=info_embed("📋 Whitelist", "Aucun utilisateur whitelisté."))
        return await ctx.send(embed=info_embed(f"📋 Whitelist ({len(ids)})", "\n".join([f"<@{uid}>" for uid in ids])))

    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Owner+** requis pour ajouter des wl."))

    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    if get_rank_db(uid) >= 2:
        return await ctx.send(embed=error_embed("❌ Erreur", f"{format_user_display(display, uid)} a un rang supérieur ou égal."))
    set_rank_db(uid, 1)
    await ctx.send(embed=success_embed("✅ Whitelist ajouté", f"{format_user_display(display, uid)} a été ajouté à la **whitelist**."))
    await send_log(ctx.guild, "Whitelist ajouté", ctx.author, display, uid, color=0x43b581)


@bot.command(name="unwl")
async def _unwl(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 2):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Owner+** requis."))
    if not user_input:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))

    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    if get_rank_db(uid) != 1:
        return await ctx.send(embed=error_embed("Pas WL", f"{format_user_display(display, uid)} n'est pas whitelisté."))
    set_rank_db(uid, 0)
    await ctx.send(embed=success_embed("✅ Whitelist retiré", f"{format_user_display(display, uid)} a été retiré de la **whitelist**."))
    await send_log(ctx.guild, "Whitelist retiré", ctx.author, display, uid, color=0xfaa61a)


# ========================= VOCAL =========================

@bot.command(name="mv")
async def _mv(ctx, member_input: str = None, channel: discord.VoiceChannel = None):
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Whitelist+** requis."))
    if member_input is None:
        return await ctx.send(embed=error_embed("Argument manquant", f"Usage : `{get_prefix_cached()}mv @user [#salon]`"))

    member = await resolve_member(ctx, member_input)
    if member is None:
        return await ctx.send(embed=error_embed("❌ Introuvable", "Cet utilisateur n'est pas sur le serveur (mention, ID ou nom)."))
    if not member.voice:
        return await ctx.send(embed=error_embed("❌ Pas en vocal", f"{member.mention} n'est pas dans une voc."))

    # Salon facultatif : si non fourni, on déplace dans la voc de l'auteur
    if channel is None:
        if not ctx.author.voice:
            return await ctx.send(embed=error_embed("❌ Pas en vocal", "Tu dois être dans une voc, ou préciser un `#salon`."))
        channel = ctx.author.voice.channel

    try:
        await member.move_to(channel)
        await ctx.send(embed=success_embed("✅ Déplacé", f"{member.mention} a été déplacé dans **{channel.name}**."))
        await send_log(ctx.guild, "Move", ctx.author, member, member.id, desc=f"→ {channel.name}", color=0x43b581)
    except discord.Forbidden:
        await ctx.send(embed=error_embed("❌ Permission manquante", "Je n'ai pas la permission de déplacer ce membre."))


@bot.command(name="join")
async def _join(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Whitelist+** requis."))
    if user_input is None:
        return await ctx.send(embed=error_embed("Argument manquant", f"Usage : `{get_prefix_cached()}join @user`"))

    member = await resolve_member(ctx, user_input)
    if member is None:
        return await ctx.send(embed=error_embed("❌ Introuvable", "Cet utilisateur n'est pas sur le serveur (mention, ID ou nom)."))
    if not member.voice:
        return await ctx.send(embed=error_embed("❌ Pas en vocal", f"{member.mention} n'est dans aucune voc."))
    if not ctx.author.voice:
        return await ctx.send(embed=error_embed("❌ Pas en vocal", "Tu dois déjà être dans une voc pour que je puisse te déplacer."))

    try:
        await ctx.author.move_to(member.voice.channel)
        await ctx.send(embed=success_embed("✅ Rejoint", f"Tu as rejoint {member.mention} dans **{member.voice.channel.name}**."))
        await send_log(ctx.guild, "Join", ctx.author, member, member.id, desc=f"→ {member.voice.channel.name}", color=0x43b581)
    except discord.Forbidden:
        await ctx.send(embed=error_embed("❌ Permission manquante", "Je n'ai pas la permission de te déplacer."))


@bot.command(name="find")
async def _find(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Whitelist+** requis."))
    if user_input is None:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))

    member = await resolve_member(ctx, user_input)
    if member is None:
        return await ctx.send(embed=error_embed("❌ Introuvable", "Cet utilisateur n'est pas sur le serveur (mention, ID ou nom)."))
    if not member.voice:
        return await ctx.send(embed=error_embed("❌ Pas en vocal", f"{member.mention} n'est actuellement dans aucune voc."))

    vc = member.voice.channel
    members_in_vc = [m.mention for m in vc.members]
    em = discord.Embed(title="🔍 Localisation vocale", color=embed_color())
    em.add_field(name="Utilisateur", value=member.mention, inline=True)
    em.add_field(name="Salon", value=f"{vc.mention}", inline=True)
    em.add_field(name="Membres présents", value=", ".join(members_in_vc) if members_in_vc else "Aucun", inline=False)
    await ctx.send(embed=em)


@bot.command(name="voc", aliases=["vc"])
async def _voc(ctx):
    await ctx.send(embed=build_stats_embed(ctx.guild))


@bot.command(name="bringall")
async def _bringall(ctx):
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Whitelist+** requis."))
    if not ctx.author.voice:
        return await ctx.send(embed=error_embed("❌ Pas en vocal", "Tu dois être dans une voc pour utiliser cette commande."))

    target_vc = ctx.author.voice.channel
    guild = ctx.guild

    public_vcs = [ch for ch in guild.voice_channels if is_public_vc(ch) and ch != target_vc]

    moved = 0
    failed = 0
    for vc in public_vcs:
        for member in vc.members:
            if member == ctx.author:
                continue
            try:
                await member.move_to(target_vc)
                moved += 1
            except discord.HTTPException:
                failed += 1

    em = success_embed("✅ BringAll", f"**{moved}** membre(s) déplacé(s) dans {target_vc.mention}.")
    if failed:
        em.add_field(name="⚠️ Échecs", value=f"{failed} membre(s) n'ont pas pu être déplacés.", inline=False)
    await ctx.send(embed=em)
    await send_log(ctx.guild, "BringAll", ctx.author, desc=f"{moved} membres → {target_vc.name}", color=0x43b581)


# ========================= PRIVÉ =========================

@bot.command(name="pv")
async def _pv(ctx):
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Whitelist+** requis."))
    if not ctx.author.voice:
        return await ctx.send(embed=error_embed("❌ Pas en vocal", "Tu dois être dans une voc pour la rendre privée."))

    vc = ctx.author.voice.channel
    if get_private_vc(vc.id):
        return await ctx.send(embed=error_embed("Déjà privé", "Ce salon est déjà privé."))

    try:
        await vc.set_permissions(ctx.guild.default_role, connect=False)
        await vc.set_permissions(ctx.author, connect=True)
        add_private_vc(vc.id, ctx.author.id, ctx.guild.id)
        await ctx.send(embed=success_embed(
            "🔒 Salon privé",
            f"{vc.mention} est maintenant **privé**.\n"
            f"Utilise `{get_prefix_cached()}acces @user` pour donner l'accès.\n"
            f"*Le salon redeviendra public automatiquement quand tu le quitteras.*"
        ))
        await send_log(ctx.guild, "Salon privé", ctx.author, desc=f"Salon : {vc.name}", color=0xfaa61a)
    except discord.Forbidden:
        await ctx.send(embed=error_embed("❌ Permission manquante", "Je n'ai pas la permission de modifier ce salon."))


@bot.command(name="unpv")
async def _unpv(ctx, channel_id: str = None):
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Whitelist+** requis."))

    if channel_id:
        try:
            vc = ctx.guild.get_channel(int(channel_id))
        except ValueError:
            return await ctx.send(embed=error_embed("❌ ID invalide", "L'ID fourni n'est pas valide."))
    else:
        if not ctx.author.voice:
            return await ctx.send(embed=error_embed("❌ Pas en vocal", "Tu dois être dans une voc ou fournir un ID de salon."))
        vc = ctx.author.voice.channel

    if not vc:
        return await ctx.send(embed=error_embed("❌ Salon introuvable", "Je n'ai pas trouvé ce salon."))

    pvc = get_private_vc(vc.id)
    if not pvc:
        return await ctx.send(embed=error_embed("Pas privé", "Ce salon n'est pas privé."))

    if str(ctx.author.id) != pvc["owner_id"] and not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le propriétaire du salon ou un **Sys+** peut retirer le privé."))

    try:
        await vc.set_permissions(ctx.guild.default_role, connect=None)
        remove_private_vc(vc.id)
        await ctx.send(embed=success_embed("🔓 Salon public", f"{vc.mention} est maintenant **public**."))
        await send_log(ctx.guild, "Salon rendu public", ctx.author, desc=f"Salon : {vc.name}", color=0x43b581)
    except discord.Forbidden:
        await ctx.send(embed=error_embed("❌ Permission manquante", "Je n'ai pas la permission de modifier ce salon."))


@bot.command(name="acces")
async def _acces(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Whitelist+** requis."))
    if user_input is None:
        return await ctx.send(embed=error_embed("Argument manquant", "Mention, ID ou nom requis."))
    if not ctx.author.voice:
        return await ctx.send(embed=error_embed("❌ Pas en vocal", "Tu dois être dans ta voc privée."))

    vc = ctx.author.voice.channel
    pvc = get_private_vc(vc.id)
    if not pvc:
        return await ctx.send(embed=error_embed("Pas privé", "Ce salon n'est pas privé."))
    if str(ctx.author.id) != pvc["owner_id"] and not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le propriétaire peut donner l'accès."))

    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    add_vc_access(vc.id, uid)
    member = ctx.guild.get_member(uid)
    if member:
        try:
            await vc.set_permissions(member, connect=True)
        except discord.Forbidden:
            pass
    await ctx.send(embed=success_embed(
        "✅ Accès accordé",
        f"{format_user_display(display, uid)} peut maintenant rejoindre {vc.mention}."
    ))


# ========================= LAISSE (toggle + liste) =========================

@bot.command(name="laisse")
async def _laisse(ctx, *, user_input: str = None):
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Whitelist+** requis."))

    # === Sans argument : afficher ma liste de laisses ===
    if user_input is None:
        leashes = get_leashes_by_owner(ctx.author.id)
        limit = get_leash_limit_for_rank(get_rank_db(ctx.author.id))
        if not leashes:
            return await ctx.send(embed=info_embed(
                "🐕 Tes laisses",
                f"Tu n'as personne en laisse. *(0/{limit})*"
            ))
        lines = [f"• <@{l['target_id']}>" for l in leashes]
        return await ctx.send(embed=info_embed(
            f"🐕 Tes laisses ({len(leashes)}/{limit})",
            "\n".join(lines)
        ))

    display, uid = await resolve_user_or_id(ctx, user_input)
    if uid is None:
        return await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Mention, ID ou nom requis."))

    leash = get_leash(uid)

    # === Déjà en laisse -> on retire (toggle) ===
    if leash:
        if str(ctx.author.id) != leash["owner_id"] and not has_min_rank(ctx.author.id, 3):
            return await ctx.send(embed=error_embed(
                "❌ Permission refusée",
                "Seul le propriétaire de la laisse ou un **Sys+** peut la retirer."
            ))

        remove_leash(uid)
        member = ctx.guild.get_member(uid)
        if member:
            try:
                await member.edit(nick=leash["original_nick"] if leash["original_nick"] != member.name else None)
            except discord.Forbidden:
                pass

        await ctx.send(embed=success_embed("✅ Laisse retirée", f"{format_user_display(display, uid)} est libre !"))
        await send_log(ctx.guild, "Laisse retirée", ctx.author, display, uid, color=0x43b581)
        return

    # === Pas en laisse -> on met la laisse ===
    member = ctx.guild.get_member(uid)
    if member is None:
        return await ctx.send(embed=error_embed("❌ Pas sur le serveur", "La personne doit être sur le serveur pour être mise en laisse."))
    if member == ctx.author:
        return await ctx.send(embed=error_embed("❌ Erreur", "Tu ne peux pas te mettre toi-même en laisse."))

    # Vérif limite selon le rang
    rank = get_rank_db(ctx.author.id)
    limit = get_leash_limit_for_rank(rank)
    current = len(get_leashes_by_owner(ctx.author.id))
    if current >= limit:
        return await ctx.send(embed=error_embed(
            "❌ Limite atteinte",
            f"Ton rang (**{rank_name(rank)}**) est limité à **{limit}** laisse(s). "
            f"Tu en as déjà **{current}**."
        ))

    original_nick = member.nick or member.name
    new_nick = f"{member.name} (🐕 de {ctx.author.display_name})"
    if len(new_nick) > 32:
        new_nick = new_nick[:32]

    add_leash(member.id, ctx.author.id, original_nick)

    try:
        await member.edit(nick=new_nick)
    except discord.Forbidden:
        pass

    await ctx.send(embed=success_embed(
        "🐕 En laisse !",
        f"{member.mention} est maintenant en laisse de {ctx.author.mention}.\n"
        f"Il suivra automatiquement dans les vocs.\n"
        f"*Refais `{get_prefix_cached()}laisse {member.mention}` pour la retirer.*"
    ))
    await send_log(ctx.guild, "Laisse", ctx.author, member, member.id, color=0xfaa61a)


# ========================= ERROR HANDLING =========================

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandInvokeError):
        error = error.original

    if isinstance(error, (commands.MemberNotFound, commands.UserNotFound)):
        await ctx.send(embed=error_embed("❌ Utilisateur introuvable", "Impossible de trouver cet utilisateur."))
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=error_embed(
            "❌ Argument manquant",
            f"Il te manque l'argument : `{error.param.name}`."
        ))
    elif isinstance(error, commands.BadArgument):
        await ctx.send(embed=error_embed("❌ Argument invalide", str(error)))
    elif isinstance(error, commands.ChannelNotFound):
        await ctx.send(embed=error_embed("❌ Salon introuvable", "Impossible de trouver ce salon."))
    elif isinstance(error, commands.CommandNotFound):
        pass
    else:
        log.error(
            f"Erreur non gérée '{ctx.command}' par {ctx.author} : {error}\n"
            + "".join(traceback.format_exception(type(error), error, error.__traceback__))
        )
        try:
            await ctx.send(embed=error_embed(
                "❌ Erreur interne",
                "Une erreur inattendue est survenue. Les logs ont été générés."
            ))
        except discord.HTTPException:
            pass


# ========================= RUN =========================
if __name__ == "__main__":
    try:
        log.info("Démarrage de Voice Master...")
        bot.run(BOT_TOKEN, log_handler=None)
    except KeyboardInterrupt:
        log.info("Arrêt demandé par l'utilisateur.")
    except Exception as e:
        log.error(f"Erreur fatale au démarrage : {e}", exc_info=True)
        sys.exit(1)
