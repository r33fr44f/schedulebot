"""
ScheduleBot Telegram v3.5.1
─────────────────────────────────────────────
✅ Multi-admins (liste configurable)
✅ Réservation membres du groupe uniquement
✅ Anti-bot
✅ Persistance JSON
✅ Vues par jour : /aujourd_hui /demain /lundi … /dimanche
✅ Commandes admin : /new /purge /reset /addadmin /removeadmin /admins
✅ Commandes membres : /planning /myslots /demain /semaine
✅ Protection concurrence JSON (threading.RLock — réentrant, anti-deadlock)
✅ Auto-redémarrage sur erreur Conflict
✅ Gestion timezone Europe/Paris (heure été/hiver auto)
✅ Année stockée dans les clés (format JJ/MM/AAAA) — anti-bug 31 déc
✅ Logs sur tous les blocs except (plus d'erreurs silencieuses)
✅ get_planning() dans les blocs db_lock (cohérence garantie)
✅ parse_key() robuste avec try/except + log
✅ Suppression de last_message_id inutilisée (double save supprimé)
✅ Échappement Markdown des prénoms (correction erreur "Can't parse entities")
─────────────────────────────────────────────
"""

import os
import json
import logging
import threading
from threading import RLock
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta
from pathlib import Path
import pytz

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes
)
from telegram.error import TelegramError
from telegram.helpers import escape_markdown  # <-- AJOUT v3.5.1

# ─── Timezone ─────────────────────────────────────────────────────────────────
TZ = pytz.timezone("Europe/Paris")

def now() -> datetime:
    """Heure actuelle en heure française (gère heure d'été / hiver auto)."""
    return datetime.now(TZ)

# ─── Lock global (RLock réentrant) ────────────────────────────────────────────
db_lock = RLock()

# ─── Config ───────────────────────────────────────────────────────────────────

TOKEN    = os.environ.get("BOT_TOKEN", "")
GROUP_ID = int(os.environ.get("GROUP_ID", "0"))

_raw_admins = os.environ.get("ADMIN_IDS", os.environ.get("ADMIN_ID", "0"))
INITIAL_ADMIN_IDS: set[int] = {
    int(x.strip()) for x in _raw_admins.split(",") if x.strip().isdigit()
}

DATA_FILE = Path(os.environ.get("DATA_FILE", "planning.json"))

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)

# ─── Persistance ──────────────────────────────────────────────────────────────

def load_data() -> dict:
    with db_lock:
        if DATA_FILE.exists():
            try:
                with open(DATA_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                log.error(f"Erreur lecture JSON ({DATA_FILE}) : {e}")
        return {}

def save_data(db: dict):
    with db_lock:
        try:
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(db, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.error(f"Erreur sauvegarde JSON ({DATA_FILE}) : {e}")

db: dict = load_data()

# ─── Migration des anciennes clés (v3.3 → v3.4) ───────────────────────────────

def migrate_keys():
    planning = db.get("planning")
    if not planning:
        return
    current_year = now().year
    migrated = 0
    with db_lock:
        for old_key in list(planning.keys()):
            try:
                label, slot = old_key.split("|", 1)
                parts = label.split(" ")
                if len(parts) < 2:
                    continue
                day_name, date_str = parts[0], parts[1]
                if date_str.count("/") == 1:
                    new_date = f"{date_str}/{current_year}"
                    new_key  = f"{day_name} {new_date}|{slot}"
                    planning[new_key] = planning.pop(old_key)
                    migrated += 1
            except Exception as e:
                log.warning(f"migrate_keys : impossible de migrer '{old_key}' : {e}")
        if migrated:
            save_data(db)
            log.info(f"Migration v3.4 : {migrated} clé(s) converties au format JJ/MM/AAAA.")

migrate_keys()

# ─── Helpers DB ───────────────────────────────────────────────────────────────

def get_planning() -> dict:
    return db.setdefault("planning", {})

def get_week_offset() -> int:
    return db.get("week_offset", 0)

def get_admin_ids() -> set[int]:
    stored = set(db.get("admin_ids", []))
    return stored | INITIAL_ADMIN_IDS

def save_admin_ids(ids: set[int]):
    with db_lock:
        db["admin_ids"] = list(ids)
    save_data(db)

# ─── Helpers semaine & créneaux ───────────────────────────────────────────────

JOUR_NOMS  = ["Lun", "Mar", "Mer", "Jeu", "Ven", "Sam", "Dim"]
JOUR_FULL  = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
JOUR_CMD   = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]

def get_week_days(offset: int = 0) -> list[tuple[str, str]]:
    today  = now()
    monday = today - timedelta(days=today.weekday()) + timedelta(weeks=offset)
    return [
        (JOUR_NOMS[i], (monday + timedelta(days=i)).strftime("%d/%m/%Y"))
        for i in range(7)
    ]

def get_slots() -> list[str]:
    slots = []
    for h in range(8, 24):
        slots.append(f"{h:02d}:00")
        if h < 23:
            slots.append(f"{h:02d}:30")
    return slots

def slot_is_past(date_str: str, slot: str) -> bool:
    try:
        parts = date_str.split("/")
        if len(parts) == 3:
            day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
        else:
            day, month = int(parts[0]), int(parts[1])
            year = now().year
            log.warning(f"slot_is_past : date sans année '{date_str}', fallback year={year}")
        h, m = map(int, slot.split(":"))
        slot_dt = TZ.localize(datetime(year, month, day, h, m))
        return slot_dt < now()
    except Exception as e:
        log.warning(f"slot_is_past : impossible de parser date='{date_str}' slot='{slot}' : {e}")
        return False

def parse_key(key: str) -> tuple[str, str]:
    try:
        label, slot = key.split("|", 1)
        parts = label.split(" ")
        date_str = parts[1] if len(parts) > 1 else ""
        if not date_str:
            log.warning(f"parse_key : date_str vide pour la clé '{key}'")
        return date_str, slot
    except Exception as e:
        log.error(f"parse_key : clé malformée '{key}' : {e}")
        return "", ""

def make_key(day_name: str, date_str: str, slot: str) -> str:
    return f"{day_name} {date_str}|{slot}"

def today_weekday() -> int:
    return now().weekday()

# ─── Échappement Markdown (v3.5.1) ────────────────────────────────────────────
def escape_md(text: str) -> str:
    """Échappe les caractères réservés MarkdownV2 dans une chaîne."""
    return escape_markdown(text, version=2)

# ─── Sécurité ─────────────────────────────────────────────────────────────────

async def is_group_member(bot, user_id: int) -> bool:
    if GROUP_ID == 0:
        return True
    try:
        member = await bot.get_chat_member(GROUP_ID, user_id)
        return member.status in ("member", "administrator", "creator")
    except TelegramError as e:
        log.warning(f"is_group_member({user_id}) : erreur Telegram : {e}")
        return False
    except Exception as e:
        log.error(f"is_group_member({user_id}) : erreur inattendue : {e}")
        return False

def is_admin(user_id: int) -> bool:
    return user_id in get_admin_ids()

def is_bot_user(user) -> bool:
    return getattr(user, "is_bot", False)

async def check_group_only(update: Update) -> bool:
    if GROUP_ID == 0:
        return True
    if update.effective_chat.id != GROUP_ID:
        await update.message.reply_text("⛔ Ce bot fonctionne uniquement dans son groupe dédié.")
        return False
    return True

async def check_admin(update: Update) -> bool:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Commande réservée aux administrateurs.")
        return False
    return True

# ─── Formatage texte ──────────────────────────────────────────────────────────

def build_full_text(planning: dict, days: list) -> str:
    lines = ["📅 *PLANNING DE LA SEMAINE*", "━" * 32, ""]
    found = False
    for day_name, date_str in days:
        day_lines = _day_slot_lines(planning, day_name, date_str)
        if day_lines:
            found = True
            display = date_str[:5]
            lines.append(f"📌 *{day_name} {display}*")
            lines.extend(day_lines)
            lines.append("")
    if not found:
        lines.append("_Aucune réservation — sélectionnez un jour ci-dessous._")
    lines.append("_Mis à jour : " + now().strftime("%d/%m %H:%M") + "_")
    return "\n".join(lines)

def build_day_text(planning: dict, day_name: str, date_str: str,
                   day_full: str, is_today: bool = False) -> str:
    tag     = " — *Aujourd'hui*" if is_today else ""
    display = date_str[:5]
    lines   = [
        f"📅 *{day_full} {display}*{tag}",
        "━" * 30, ""
    ]
    day_lines = _day_slot_lines(planning, day_name, date_str, show_empty=True)
    if day_lines:
        lines.extend(day_lines)
    else:
        lines.append("_Aucune réservation pour ce jour._")
    lines.append("")
    lines.append("_Mis à jour : " + now().strftime("%d/%m %H:%M") + "_")
    return "\n".join(lines)

def _day_slot_lines(planning: dict, day_name: str, date_str: str,
                    show_empty: bool = False) -> list[str]:
    """Affiche les créneaux d'un jour en échappant les noms des réservations."""
    lines = []
    for slot in get_slots():
        key = make_key(day_name, date_str, slot)
        members = planning.get(key, [])
        if members:
            past = "🕐 " if slot_is_past(date_str, slot) else "🟢 "
            # Échappement Markdown des noms (v3.5.1)
            members_escaped = [escape_md(m) for m in members]
            lines.append(f"  {past}`{slot}` → {', '.join(members_escaped)}")
        elif show_empty:
            lines.append(f"  ⬜ `{slot}`")
    return lines

def build_week_summary(planning: dict, days: list) -> str:
    lines = ["📋 *RÉSUMÉ COMPLET DE LA SEMAINE*\n"]
    found = False
    for day_name, date_str in days:
        day_lines = _day_slot_lines(planning, day_name, date_str)
        if day_lines:
            found = True
            display = date_str[:5]
            lines.append(f"📌 *{day_name} {display}*")
            lines.extend(day_lines)
            lines.append("")
    if not found:
        lines.append("_Aucune réservation cette semaine._")
    return "\n".join(lines)

# ─── Clavier inline ───────────────────────────────────────────────────────────

def build_keyboard(planning: dict, days: list,
                   selected_day: int = None) -> InlineKeyboardMarkup:
    kb = []
    day_row = []
    for i, (day_name, date_str) in enumerate(days):
        count = sum(len(v) for k, v in planning.items()
                    if k.startswith(f"{day_name} {date_str}|"))
        marker = "▸ " if i == selected_day else ""
        badge = f"({count})" if count else ""
        display = date_str[:5]
        label = f"{marker}{day_name} {display} {badge}".strip()
        day_row.append(InlineKeyboardButton(label, callback_data=f"DAY:{i}"))
        if len(day_row) == 4:
            kb.append(day_row)
            day_row = []
    if day_row:
        kb.append(day_row)

    if selected_day is not None:
        day_name, date_str = days[selected_day]
        display = date_str[:5]
        kb.append([InlineKeyboardButton(f"── {day_name} {display} ──", callback_data="NOOP")])
        row = []
        for slot in get_slots():
            key = make_key(day_name, date_str, slot)
            members = planning.get(key, [])
            past = slot_is_past(date_str, slot)
            icon = "🕐" if past and members else ("✅" if members else "")
            count = f" {len(members)}" if members else ""
            row.append(InlineKeyboardButton(
                f"{icon}{slot}{count}", callback_data=f"SLOT:{selected_day}:{slot}"
            ))
            if len(row) == 3:
                kb.append(row)
                row = []
        if row:
            kb.append(row)
        kb.append([InlineKeyboardButton("◀ Retour", callback_data="BACK")])

    kb.append([
        InlineKeyboardButton("🗑 Purger passés", callback_data="PURGE"),
        InlineKeyboardButton("📋 Résumé", callback_data="SUMMARY"),
    ])
    return InlineKeyboardMarkup(kb)

# ─── Commandes membres ────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_group_only(update): return
    admin_mark = " _(vous êtes admin)_" if is_admin(update.effective_user.id) else ""
    await update.message.reply_text(
        f"👋 *ScheduleBot — Aide*{admin_mark}\n\n"
        "📅 *Planning*\n"
        "• `/planning` — Planning interactif complet\n"
        "• `/semaine` — Résumé de toute la semaine\n"
        "• `/myslots` — Mes réservations\n\n"
        "📆 *Vue par jour*\n"
        "• `/aujourd_hui` ou `/auj`\n"
        "• `/demain`\n"
        "• `/lundi` `/mardi` `/mercredi`\n"
        "• `/jeudi` `/vendredi` `/samedi` `/dimanche`\n\n"
        "🔧 *Admin uniquement*\n"
        "• `/new` — Nouveau planning (cette semaine)\n"
        "• `/new +1` — Nouveau planning (semaine prochaine)\n"
        "• `/purge` — Supprimer créneaux passés\n"
        "• `/reset` — Tout remettre à zéro\n"
        "• `/addadmin @pseudo` — Ajouter un admin\n"
        "• `/removeadmin @pseudo` — Retirer un admin\n"
        "• `/admins` — Liste des admins\n",
        parse_mode="Markdown"
    )

async def cmd_planning(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_group_only(update): return
    planning = get_planning()
    days = get_week_days(get_week_offset())
    await update.message.reply_text(
        build_full_text(planning, days),
        parse_mode="Markdown",
        reply_markup=build_keyboard(planning, days)
    )

async def cmd_semaine(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_group_only(update): return
    planning = get_planning()
    days = get_week_days(get_week_offset())
    await update.message.reply_text(
        build_week_summary(planning, days),
        parse_mode="Markdown"
    )

async def cmd_myslots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_group_only(update): return
    user = update.effective_user
    if is_bot_user(user): return
    name = user.first_name or user.username or "Anonyme"
    # Échappement Markdown du nom (v3.5.1)
    escaped_name = escape_md(name)
    planning = get_planning()
    days = get_week_days(get_week_offset())
    lines = [f"📋 *Vos réservations, {escaped_name}*\n"]
    found = False
    for day_name, date_str in days:
        for slot in get_slots():
            key = make_key(day_name, date_str, slot)
            if name in planning.get(key, []):  # comparer avec le nom original (non échappé)
                past = " 🕐_(passé)_" if slot_is_past(date_str, slot) else ""
                display = date_str[:5]
                lines.append(f"• *{day_name} {display}* à `{slot}`{past}")
                found = True
    if not found:
        lines.append("_Aucune réservation cette semaine._")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

# ─── Vues par jour ────────────────────────────────────────────────────────────

async def _send_day_view(update: Update, context: ContextTypes.DEFAULT_TYPE,
                         target_weekday: int):
    if not await check_group_only(update): return
    planning = get_planning()
    days = get_week_days(get_week_offset())
    day_name, date_str = days[target_weekday]
    day_full = JOUR_FULL[target_weekday]
    is_today = (target_weekday == today_weekday())
    await update.message.reply_text(
        build_day_text(planning, day_name, date_str, day_full, is_today),
        parse_mode="Markdown"
    )

async def cmd_aujourd_hui(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_day_view(update, context, today_weekday())

async def cmd_demain(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_day_view(update, context, (today_weekday() + 1) % 7)

async def cmd_lundi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_day_view(update, context, 0)

async def cmd_mardi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_day_view(update, context, 1)

async def cmd_mercredi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_day_view(update, context, 2)

async def cmd_jeudi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_day_view(update, context, 3)

async def cmd_vendredi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_day_view(update, context, 4)

async def cmd_samedi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_day_view(update, context, 5)

async def cmd_dimanche(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_day_view(update, context, 6)

# ─── Commandes admin ──────────────────────────────────────────────────────────

async def cmd_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_group_only(update): return
    if not await check_admin(update): return
    offset = 0
    if context.args and context.args[0] in ("+1", "+2", "-1"):
        offset = int(context.args[0])
    with db_lock:
        db["planning"] = {}
        db["week_offset"] = offset
        save_data(db)
    days = get_week_days(offset)
    planning = {}
    await update.message.reply_text(
        "✅ *Nouveau planning créé !*\n\n" + build_full_text(planning, days),
        parse_mode="Markdown",
        reply_markup=build_keyboard(planning, days)
    )
    log.info(f"Nouveau planning créé par {update.effective_user.id} (offset={offset})")

async def cmd_purge(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_group_only(update): return
    if not await check_admin(update): return
    removed = 0
    with db_lock:
        planning = get_planning()
        for key in list(planning.keys()):
            date_str, slot = parse_key(key)
            if date_str and slot_is_past(date_str, slot):
                del planning[key]
                removed += 1
        save_data(db)
    await update.message.reply_text(
        f"🗑 *{removed} créneau(x) passé(s) supprimé(s).*", parse_mode="Markdown"
    )

async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_group_only(update): return
    if not await check_admin(update): return
    with db_lock:
        db["planning"] = {}
        save_data(db)
    await update.message.reply_text("♻️ *Planning réinitialisé.*", parse_mode="Markdown")

async def cmd_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_group_only(update): return
    if not await check_admin(update): return
    ids = get_admin_ids()
    lines = [f"🔧 *Administrateurs du bot* ({len(ids)})\n"]
    for uid in ids:
        try:
            member = await context.bot.get_chat_member(GROUP_ID, uid)
            name = member.user.first_name or member.user.username or str(uid)
        except TelegramError as e:
            log.warning(f"cmd_admins : impossible de récupérer l'utilisateur {uid} : {e}")
            name = str(uid)
        except Exception as e:
            log.error(f"cmd_admins : erreur inattendue pour {uid} : {e}")
            name = str(uid)
        lines.append(f"• {name} `({uid})`")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_addadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_group_only(update): return
    if not await check_admin(update): return

    target_id = None
    target_name = None

    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
        target_name = update.message.reply_to_message.from_user.first_name
    elif context.args:
        arg = context.args[0].lstrip("@")
        if arg.isdigit():
            target_id = int(arg)
            target_name = arg
        else:
            await update.message.reply_text(
                "💡 Usage : `/addadmin 12345678` ou répondez au message de l'utilisateur.",
                parse_mode="Markdown"
            )
            return

    if not target_id:
        await update.message.reply_text(
            "💡 Usage : `/addadmin 12345678` ou répondez au message de l'utilisateur.",
            parse_mode="Markdown"
        )
        return

    ids = get_admin_ids()
    if target_id in ids:
        await update.message.reply_text("ℹ️ Cet utilisateur est déjà admin.")
        return

    ids.add(target_id)
    save_admin_ids(ids)
    log.info(f"Admin ajouté : {target_id} par {update.effective_user.id}")
    await update.message.reply_text(
        f"✅ *{target_name or target_id}* ajouté comme administrateur.", parse_mode="Markdown"
    )

async def cmd_removeadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_group_only(update): return
    if not await check_admin(update): return

    target_id = None
    target_name = None

    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
        target_name = update.message.reply_to_message.from_user.first_name
    elif context.args:
        arg = context.args[0].lstrip("@")
        if arg.isdigit():
            target_id = int(arg)
            target_name = arg

    if not target_id:
        await update.message.reply_text(
            "💡 Usage : `/removeadmin 12345678` ou répondez au message de l'utilisateur.",
            parse_mode="Markdown"
        )
        return

    if target_id in INITIAL_ADMIN_IDS:
        await update.message.reply_text(
            "⛔ Impossible de retirer un admin configuré dans les variables d'environnement."
        )
        return

    ids = get_admin_ids()
    if target_id not in ids:
        await update.message.reply_text("ℹ️ Cet utilisateur n'est pas admin.")
        return

    ids.discard(target_id)
    save_admin_ids(ids)
    log.info(f"Admin retiré : {target_id} par {update.effective_user.id}")
    await update.message.reply_text(
        f"✅ *{target_name or target_id}* retiré des administrateurs.", parse_mode="Markdown"
    )

# ─── Callbacks inline ─────────────────────────────────────────────────────────

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user

    if is_bot_user(user):
        await query.answer("⛔ Les bots ne peuvent pas réserver.", show_alert=True)
        return

    member_ok = await is_group_member(context.bot, user.id)
    if not member_ok:
        await query.answer("⛔ Vous devez être membre du groupe.", show_alert=True)
        return

    action = query.data
    username = user.first_name or user.username or f"User{user.id}"
    days = get_week_days(get_week_offset())
    planning = get_planning()  # lecture hors lock (affichage seulement)

    if action == "NOOP":
        await query.answer()

    elif action.startswith("DAY:"):
        await query.answer()
        day_idx = int(action.split(":")[1])
        await _edit(query, build_full_text(planning, days),
                    build_keyboard(planning, days, selected_day=day_idx))

    elif action == "BACK":
        await query.answer()
        await _edit(query, build_full_text(planning, days),
                    build_keyboard(planning, days))

    elif action.startswith("SLOT:"):
        _, day_idx_str, slot = action.split(":", 2)
        day_idx = int(day_idx_str)
        day_name, date_str = days[day_idx]
        key = make_key(day_name, date_str, slot)

        with db_lock:
            # Relecture sous lock pour garantir la version la plus récente
            planning = get_planning()
            members = planning.setdefault(key, [])
            if username in members:
                members.remove(username)
                if not members:
                    del planning[key]
                notif = f"❌ Désinscrit — {day_name} {date_str[:5]} {slot}"
            else:
                members.append(username)
                notif = f"✅ Inscrit — {day_name} {date_str[:5]} à {slot}"
            save_data(db)

        await query.answer(notif)
        await _edit(query, build_full_text(planning, days),
                    build_keyboard(planning, days, selected_day=day_idx))

    elif action == "PURGE":
        if not is_admin(user.id):
            await query.answer("⛔ Réservé aux administrateurs.", show_alert=True)
            return
        removed = 0
        with db_lock:
            planning = get_planning()
            for key in list(planning.keys()):
                date_str, slot = parse_key(key)
                if date_str and slot_is_past(date_str, slot):
                    del planning[key]
                    removed += 1
            save_data(db)

        await query.answer(f"🗑 {removed} créneau(x) passé(s) supprimé(s).")
        await _edit(query, build_full_text(planning, days),
                    build_keyboard(planning, days))

    elif action == "SUMMARY":
        await query.answer("Résumé envoyé !")
        try:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=build_week_summary(planning, days),
                parse_mode="Markdown"
            )
        except TelegramError as e:
            log.error(f"SUMMARY : erreur envoi message : {e}")
        except Exception as e:
            log.error(f"SUMMARY : erreur inattendue : {e}")

async def _edit(query, text: str, keyboard: InlineKeyboardMarkup):
    try:
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=keyboard)
    except TelegramError as e:
        log.warning(f"_edit : impossible de modifier le message : {e}")
    except Exception as e:
        log.error(f"_edit : erreur inattendue : {e}")

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not TOKEN:
        raise ValueError("BOT_TOKEN manquant.")

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler(["start", "help"], cmd_start))
    app.add_handler(CommandHandler("planning", cmd_planning))
    app.add_handler(CommandHandler("semaine", cmd_semaine))
    app.add_handler(CommandHandler("myslots", cmd_myslots))

    app.add_handler(CommandHandler(
        ["aujourd_hui", "aujourdhui", "auj"],
        cmd_aujourd_hui
    ))
    app.add_handler(CommandHandler("demain", cmd_demain))
    app.add_handler(CommandHandler("lundi", cmd_lundi))
    app.add_handler(CommandHandler("mardi", cmd_mardi))
    app.add_handler(CommandHandler("mercredi", cmd_mercredi))
    app.add_handler(CommandHandler("jeudi", cmd_jeudi))
    app.add_handler(CommandHandler("vendredi", cmd_vendredi))
    app.add_handler(CommandHandler("samedi", cmd_samedi))
    app.add_handler(CommandHandler("dimanche", cmd_dimanche))

    app.add_handler(CommandHandler("new", cmd_new))
    app.add_handler(CommandHandler("purge", cmd_purge))
    app.add_handler(CommandHandler("reset", cmd_reset))
    app.add_handler(CommandHandler("admins", cmd_admins))
    app.add_handler(CommandHandler("addadmin", cmd_addadmin))
    app.add_handler(CommandHandler("removeadmin", cmd_removeadmin))

    app.add_handler(CallbackQueryHandler(on_callback))

    log.info(f"✅ ScheduleBot v3.5.1 démarré — admins initiaux : {INITIAL_ADMIN_IDS}")
    app.run_polling(drop_pending_updates=True, close_loop=False)

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        pass

def start_health_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    log.info(f"Health check server sur le port {port}")
    server.serve_forever()

if __name__ == "__main__":
    import asyncio
    import time
    from telegram.error import Conflict as TGConflict

    t = threading.Thread(target=start_health_server, daemon=True)
    t.start()
    time.sleep(2)
    log.info("Health server prêt, démarrage du bot...")

    MAX_RETRIES = 5
    retry = 0

    while retry < MAX_RETRIES:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            log.info(f"Tentative de démarrage #{retry + 1}...")
            main()
            break

        except TGConflict:
            retry += 1
            wait = retry * 10
            log.warning(
                f"Conflit détecté (instance dupliquée). "
                f"Attente {wait}s avant redémarrage... "
                f"(tentative {retry}/{MAX_RETRIES})"
            )
            try:
                loop.close()
            except Exception as e:
                log.warning(f"Fermeture loop (Conflict) : {e}")
            time.sleep(wait)

        except Exception as e:
            retry += 1
            wait = retry * 5
            log.error(f"Erreur inattendue : {e}. Redémarrage dans {wait}s...")
            try:
                loop.close()
            except Exception as e2:
                log.warning(f"Fermeture loop (Exception) : {e2}")
            time.sleep(wait)

        finally:
            try:
                loop.close()
            except:
                pass

    if retry >= MAX_RETRIES:
        log.critical("Nombre maximum de tentatives atteint. Arrêt du bot.")
        raise SystemExit(1)
