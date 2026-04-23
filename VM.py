import discord
from discord.ext import commands, tasks
import os

# Volume persistant : DATA_DIR doit pointer vers un dossier persistant (volume Railway)
DATA_DIR = os.environ.get("DATA_DIR")
if not DATA_DIR:
    print("[ERREUR CRITIQUE] DATA_DIR non défini. Configure DATA_DIR=/data dans Railway.")
    import sys as _sys_exit; _sys_exit.exit(1)
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "vm_bot.db")
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
DEFAULT_BUYER_IDS = [1312375517927706630, 1312375955737542676, 1173948561881317389]
DEFAULT_PREFIX = "="

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

    c.execute("INSERT OR IGNORE INTO config VALUES ('prefix', ?)", (DEFAULT_PREFIX,))
    c.execute("INSERT OR IGNORE INTO config VALUES ('buyer_ids', ?)",
              (json.dumps([str(i) for i in DEFAULT_BUYER_IDS]),))

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


# ========================= HELPERS =========================

def rank_name(level):
    return {4: "Buyer", 3: "Sys", 2: "Owner", 1: "Whitelist", 0: "Aucun"}[level]


def has_min_rank(user_id, minimum):
    return get_rank_db(user_id) >= minimum


def embed_color():
    return 0x2b2d31


def success_embed(title, desc=""):
    em = discord.Embed(title=title, description=desc, color=0x43b581)
    em.set_footer(text="Voice Master")
    return em


def error_embed(title, desc=""):
    em = discord.Embed(title=title, description=desc, color=0xf04747)
    em.set_footer(text="Voice Master")
    return em


def info_embed(title, desc=""):
    em = discord.Embed(title=title, description=desc, color=embed_color())
    em.set_footer(text="Voice Master")
    return em


def get_french_time():
    now = datetime.now(PARIS_TZ)
    JOURS_FR = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
    MOIS_FR = ["janvier", "février", "mars", "avril", "mai", "juin",
               "juillet", "août", "septembre", "octobre", "novembre", "décembre"]
    return f"{JOURS_FR[now.weekday()]} {now.day} {MOIS_FR[now.month - 1]} {now.year} — {now.strftime('%Hh%M')}"


def is_public_vc(channel):
    """Vérifie si une voc est publique (accessible @everyone)"""
    if not isinstance(channel, discord.VoiceChannel):
        return False
    everyone = channel.guild.default_role
    perms = channel.permissions_for(everyone)
    return perms.connect and not channel.user_limit == 1


# ========================= RESOLVE USER (ex-membres OK) =========================

async def resolve_user_or_id(ctx, user_input):
    """
    Retourne (display_obj, user_id) — marche même si la personne n'est plus sur le serveur.
    Utilisé pour les commandes de rang (sys/owner/wl) qui agissent juste sur la DB.
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
    em.set_footer(text=get_french_time())
    try:
        await channel.send(embed=em)
    except discord.HTTPException as e:
        log.warning(f"send_log: échec d'envoi : {e}")


# ========================= EVENTS =========================

@bot.event
async def on_ready():
    log.info(f"Bot connecté : {bot.user} ({bot.user.id})")
    log.info(f"Prefix : {get_prefix_cached()}")
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="les vocales"))
    if not leash_follow.is_running():
        leash_follow.start()


@bot.event
async def on_voice_state_update(member, before, after):
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


# ========================= HELP DYNAMIQUE =========================

# Rangs : 0 = Aucun, 1 = WL, 2 = Owner, 3 = Sys, 4 = Buyer

HELP_CATEGORIES = {
    "vocal": {
        "emoji": "🎙️",
        "label": "Vocal",
        "title": "🎙️  Vocal",
        "items": [
            ("voc / vc",         "Stats vocales du serveur",     0),
            ("mv @user #salon",  "Déplacer un user en voc",      1),
            ("find @user",       "Trouver un user en voc",       1),
            ("bringall",         "Ramener tout le monde dans ta voc", 1),
        ],
    },
    "prive": {
        "emoji": "🔒",
        "label": "Salons Privés",
        "title": "🔒  Salons Privés",
        "items": [
            ("pv",            "Rendre ta voc actuelle privée",      1),
            ("unpv",          "Retirer le privé de ta voc",         1),
            ("unpv <id>",     "Retirer le privé d'un autre salon",  1),
            ("acces @user",   "Donner accès à ta voc privée",       1),
        ],
    },
    "laisse": {
        "emoji": "🐕",
        "label": "Laisse",
        "title": "🐕  Laisse",
        "items": [
            ("laisse @user",                   "Mettre quelqu'un en laisse", 1),
            ("unleash / unlaisse @user",       "Retirer la laisse",          1),
        ],
    },
    "perms": {
        "emoji": "👥",
        "label": "Permissions",
        "title": "👥  Permissions",
        "items": [
            ("wl @user / unwl @user",       "Gérer la whitelist", 2),
            ("owner @user / unowner @user", "Gérer les owners",   3),
            ("sys @user / unsys @user",     "Gérer les sys",      4),
        ],
    },
    "system": {
        "emoji": "🛠️",
        "label": "Système",
        "title": "🛠️  Système",
        "items": [
            ("setlog #salon",    "Salon de logs",     4),
            ("prefix [nouveau]", "Changer le prefix", 4),
        ],
    },
    "hierarchy": {
        "emoji": "📋",
        "label": "Hiérarchie",
        "title": "📋  Hiérarchie",
        "min_rank": 1,  # Visible à partir de WL
        "items": [],
    },
}


def help_accessible_items(key, rank):
    cat = HELP_CATEGORIES.get(key, {})
    return [(s, d) for (s, d, mr) in cat.get("items", []) if rank >= mr]


def help_category_visible(key, rank):
    cat = HELP_CATEGORIES.get(key, {})
    if "min_rank" in cat:
        return rank >= cat["min_rank"]
    return len(help_accessible_items(key, rank)) > 0


def build_help_category_embed(key, rank):
    p = get_prefix_cached()
    cat = HELP_CATEGORIES[key]
    em = discord.Embed(title=cat["title"], color=embed_color())
    items = help_accessible_items(key, rank)
    if not items:
        em.description = "*Aucune commande accessible à ton rang.*"
    else:
        max_syntax = max(len(f"{p}{syntax}") for syntax, _ in items)
        lines = [
            f"{p}{syntax}".ljust(max_syntax + 2) + f"→ {desc}"
            for syntax, desc in items
        ]
        em.description = "```\n" + "\n".join(lines) + "\n```"
    em.set_footer(text="Made by gp ・ Voice Master")
    return em


def build_help_hierarchy_embed(rank):
    em = discord.Embed(title="📋  Hiérarchie", color=embed_color())
    lines = ["```\nBuyer > Sys > Owner > Whitelist > Aucun\n```\n"]
    levels = [
        (4, "👑 **Buyer**",     "Accès total, `=prefix`, `=setlog`, `=sys`/`=unsys`"),
        (3, "🔧 **Sys**",       "`=owner`/`=unowner`, gère les `unpv`/`unleash` de tout le monde"),
        (2, "⭐ **Owner**",     "`=wl`/`=unwl`"),
        (1, "✨ **Whitelist**", "Accès aux commandes vocales, privé et laisse"),
        (0, "👤 **Aucun**",     "Peut voir les stats vocales uniquement"),
    ]
    for lvl, name, desc in levels:
        marker = " ← **toi**" if lvl == rank else ""
        lines.append(f"> {name} — {desc}{marker}")
    lines.append("")
    lines.append("ℹ️ Un rang ne peut **jamais** agir sur quelqu'un de rang égal ou supérieur.")
    em.description = "\n".join(lines)
    em.set_footer(text="Made by gp ・ Voice Master")
    return em


def build_help_home_embed(rank):
    p = get_prefix_cached()
    em = discord.Embed(color=embed_color())
    em.set_author(name="Voice Master ─ Panel d'aide")

    rank_label = rank_name(rank)
    intro = (
        f"```\n🕐  {get_french_time()}\n```\n"
        f"Bienvenue sur **Voice Master**.\n\n"
        f"**Prefix :** `{p}` ・ **Ton rang :** {rank_label}\n\n"
    )

    category_descriptions = {
        "vocal":     "Commandes vocales (move, find, stats)",
        "prive":     "Gérer les salons privés",
        "laisse":    "Système de laisse",
        "perms":     "Attribuer les rangs",
        "system":    "Configuration du bot",
        "hierarchy": "Qui peut faire quoi",
    }
    visible = []
    for key, lbl in category_descriptions.items():
        if help_category_visible(key, rank):
            cat = HELP_CATEGORIES[key]
            visible.append(f"> {cat['emoji']} **{cat['label']}** — {lbl}")

    em.description = intro + ("\n".join(visible) if visible else "*Aucune catégorie disponible.*")
    em.set_footer(text="Made by gp ・ Voice Master")
    return em


def build_help_embed_for(key, rank):
    if key == "home":
        return build_help_home_embed(rank)
    if key == "hierarchy":
        return build_help_hierarchy_embed(rank)
    return build_help_category_embed(key, rank)


class HelpDropdown(discord.ui.Select):
    def __init__(self, user_rank):
        self.user_rank = user_rank
        options = [discord.SelectOption(label="Accueil", emoji="🏠", value="home")]
        for key, cat in HELP_CATEGORIES.items():
            if help_category_visible(key, user_rank):
                options.append(discord.SelectOption(
                    label=cat["label"], emoji=cat["emoji"], value=key
                ))
        super().__init__(
            placeholder="📂 Choisis une catégorie...",
            min_values=1, max_values=1, options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        key = self.values[0]
        if key != "home" and not help_category_visible(key, self.user_rank):
            return await interaction.response.send_message(
                "Tu n'as pas accès à cette catégorie.", ephemeral=True
            )
        await interaction.response.edit_message(
            embed=build_help_embed_for(key, self.user_rank), view=self.view
        )


class HelpView(discord.ui.View):
    def __init__(self, author_id, user_rank):
        super().__init__(timeout=120)
        self.author_id = author_id
        self.user_rank = user_rank
        self.add_item(HelpDropdown(user_rank))

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
    view = HelpView(ctx.author.id, rank)
    await ctx.send(embed=build_help_home_embed(rank), view=view)


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
async def _mv(ctx, member: discord.Member = None, channel: discord.VoiceChannel = None):
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Whitelist+** requis."))
    if member is None or channel is None:
        return await ctx.send(embed=error_embed("Arguments manquants", f"Usage : `{get_prefix_cached()}mv @user #salon`"))
    if not member.voice:
        return await ctx.send(embed=error_embed("❌ Pas en vocal", f"{member.mention} n'est pas dans une voc."))
    try:
        await member.move_to(channel)
        await ctx.send(embed=success_embed("✅ Déplacé", f"{member.mention} a été déplacé dans **{channel.name}**."))
        await send_log(ctx.guild, "Move", ctx.author, member, member.id, desc=f"→ {channel.name}", color=0x43b581)
    except discord.Forbidden:
        await ctx.send(embed=error_embed("❌ Permission manquante", "Je n'ai pas la permission de déplacer ce membre."))


@bot.command(name="find")
async def _find(ctx, member: discord.Member = None):
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Whitelist+** requis."))
    if member is None:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un utilisateur."))
    if not member.voice:
        return await ctx.send(embed=error_embed("❌ Pas en vocal", f"{member.mention} n'est actuellement dans aucune voc."))
    vc = member.voice.channel
    members_in_vc = [m.mention for m in vc.members]
    em = discord.Embed(title="🔍 Localisation vocale", color=embed_color())
    em.add_field(name="Utilisateur", value=member.mention, inline=True)
    em.add_field(name="Salon", value=f"{vc.mention}", inline=True)
    em.add_field(name="Membres présents", value=", ".join(members_in_vc) if members_in_vc else "Aucun", inline=False)
    em.set_footer(text="Voice Master")
    await ctx.send(embed=em)


@bot.command(name="voc", aliases=["vc"])
async def _voc(ctx):
    guild = ctx.guild
    total_members = guild.member_count
    total_boosts = guild.premium_subscription_count

    all_vc_members = []
    for vc in guild.voice_channels:
        all_vc_members.extend(vc.members)

    total_in_vc = len(all_vc_members)
    streaming = sum(1 for m in all_vc_members if m.voice and m.voice.self_stream)
    on_cam = sum(1 for m in all_vc_members if m.voice and m.voice.self_video)
    active = sum(1 for m in all_vc_members if m.voice and not m.voice.self_mute and not m.voice.mute)
    muted = total_in_vc - active

    em = discord.Embed(title=f"🎙️ Stats vocales — {guild.name}", color=embed_color())
    if guild.icon:
        em.set_thumbnail(url=guild.icon.url)
    em.add_field(name="👥 Membres total", value=f"`{total_members}`", inline=True)
    em.add_field(name="🚀 Boosts", value=f"`{total_boosts}`", inline=True)
    em.add_field(name="\u200b", value="\u200b", inline=True)
    em.add_field(name="🎙️ En voc", value=f"`{total_in_vc}`", inline=True)
    em.add_field(name="🔊 Actifs (non mute)", value=f"`{active}`", inline=True)
    em.add_field(name="🔇 Mute", value=f"`{muted}`", inline=True)
    em.add_field(name="📺 En stream", value=f"`{streaming}`", inline=True)
    em.add_field(name="📷 En cam", value=f"`{on_cam}`", inline=True)
    em.set_footer(text=f"Voice Master ・ {get_french_time()}")
    await ctx.send(embed=em)


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
            f"Utilise `{get_prefix_cached()}acces @user` pour donner l'accès."
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
async def _acces(ctx, member: discord.Member = None):
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Whitelist+** requis."))
    if member is None:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un utilisateur."))
    if not ctx.author.voice:
        return await ctx.send(embed=error_embed("❌ Pas en vocal", "Tu dois être dans ta voc privée."))

    vc = ctx.author.voice.channel
    pvc = get_private_vc(vc.id)
    if not pvc:
        return await ctx.send(embed=error_embed("Pas privé", "Ce salon n'est pas privé."))
    if str(ctx.author.id) != pvc["owner_id"] and not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le propriétaire peut donner l'accès."))

    add_vc_access(vc.id, member.id)
    try:
        await vc.set_permissions(member, connect=True)
    except discord.Forbidden:
        pass
    await ctx.send(embed=success_embed("✅ Accès accordé", f"{member.mention} peut maintenant rejoindre {vc.mention}."))


# ========================= LAISSE =========================

@bot.command(name="laisse")
async def _laisse(ctx, member: discord.Member = None):
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Whitelist+** requis."))
    if member is None:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un utilisateur."))
    if member == ctx.author:
        return await ctx.send(embed=error_embed("❌ Erreur", "Tu ne peux pas te mettre toi-même en laisse."))
    if get_leash(member.id):
        return await ctx.send(embed=error_embed("Déjà en laisse", f"{member.mention} est déjà en laisse."))

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
        f"Il suivra automatiquement dans les vocs."
    ))
    await send_log(ctx.guild, "Laisse", ctx.author, member, member.id, color=0xfaa61a)


@bot.command(name="unleash", aliases=["unlaisse", "delaisse"])
async def _unleash(ctx, member: discord.Member = None):
    if not has_min_rank(ctx.author.id, 1):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "**Whitelist+** requis."))
    if member is None:
        return await ctx.send(embed=error_embed("Argument manquant", "Mentionne un utilisateur."))

    leash = get_leash(member.id)
    if not leash:
        return await ctx.send(embed=error_embed("Pas en laisse", f"{member.mention} n'est pas en laisse."))

    if str(ctx.author.id) != leash["owner_id"] and not has_min_rank(ctx.author.id, 3):
        return await ctx.send(embed=error_embed("❌ Permission refusée", "Seul le propriétaire de la laisse ou un **Sys+** peut la retirer."))

    remove_leash(member.id)

    try:
        await member.edit(nick=leash["original_nick"] if leash["original_nick"] != member.name else None)
    except discord.Forbidden:
        pass

    await ctx.send(embed=success_embed("✅ Laisse retirée", f"{member.mention} est libre !"))
    await send_log(ctx.guild, "Laisse retirée", ctx.author, member, member.id, color=0x43b581)


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
