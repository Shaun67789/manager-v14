from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Form, Response, BackgroundTasks, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import uvicorn
import os
import re
import asyncio
from typing import Optional

from database import db
from bot_manager import bot_manager
import telebot.types as tg_types


# ─────────────────────────────────────────────────────────────
# HELPER: parse Telegram message links
# ─────────────────────────────────────────────────────────────

def parse_telegram_link(link: str):
    """
    Parse a Telegram message link and return (chat_id, message_id).
    Supports:
      https://t.me/c/1234567890/42       → private supergroup
      https://t.me/groupusername/42      → public group
    Returns (None, None) on failure.
    """
    link = link.strip()
    # Private supergroup: t.me/c/<channel_id>/<msg_id>
    m = re.match(r'https?://t\.me/c/(\d+)/(\d+)', link)
    if m:
        chat_id = int('-100' + m.group(1))
        msg_id  = int(m.group(2))
        return chat_id, msg_id
    # Public group: t.me/<username>/<msg_id>
    m = re.match(r'https?://t\.me/([a-zA-Z]\w+)/(\d+)', link)
    if m:
        username = '@' + m.group(1)
        msg_id   = int(m.group(2))
        return username, msg_id
    return None, None


# ─────────────────────────────────────────────────────────────
# APP SETUP
# ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(application: FastAPI):
    if db.get_config().get("is_running", False):
        bot_manager.start_bot()
    yield
    bot_manager.stop_bot()


app = FastAPI(lifespan=lifespan, title="GroupBot Dashboard", version="8.0")

os.makedirs("templates", exist_ok=True)
templates = Jinja2Templates(directory="templates")


# ─────────────────────────────────────────────────────────────
# MAIN DASHBOARD
# ─────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, search: Optional[str] = None):
    config = db.get_config()
    stats  = db.get_all_stats()

    if search:
        users, groups = db.search_items(search)
    else:
        users  = db.get_all_users()
        groups = db.get_all_groups()

    context = {
        "request":        request,
        "config":         config,
        "stats":          stats,
        "users":          users,
        "groups":         groups,
        "search_query":   search,
        "logs":           db.get_recent_logs(50),
        "warnings_board": db.get_warnings_leaderboard(),
    }
    return templates.TemplateResponse(request=request, name="dashboard.html", context=context)


# ─────────────────────────────────────────────────────────────
# API — LIVE STATS
# ─────────────────────────────────────────────────────────────

@app.get("/api/stats")
async def api_stats():
    try:
        stats  = db.get_all_stats()
        config = db.get_config()
        stats["is_running"] = config.get("is_running", False)
        stats["bot_alive"]  = False
        if bot_manager.bot and bot_manager.thread and bot_manager.thread.is_alive():
            stats["bot_alive"] = True
        return stats
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/logs")
async def api_logs(limit: int = 50):
    return db.get_recent_logs(limit)


@app.get("/api/groups")
async def api_groups():
    return db.get_all_groups()


@app.get("/api/users")
async def api_users():
    return JSONResponse(content=db.get_all_users())


@app.get("/api/warnings")
async def api_warnings():
    return JSONResponse(content=db.get_warnings_leaderboard())


# ─────────────────────────────────────────────────────────────
# API — DATABASE DOWNLOAD
# ─────────────────────────────────────────────────────────────

@app.get("/api/download_db")
async def download_db():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(BASE_DIR, "data", "manager.db")
    if not os.path.exists(db_path):
        return Response(content="Database file not found", status_code=404)
    with open(db_path, "rb") as f:
        content = f.read()
    return Response(
        content=content,
        media_type="application/x-sqlite3",
        headers={"Content-Disposition": "attachment; filename=manager.db"}
    )


@app.post("/api/upload_db")
async def upload_db(file: UploadFile = File(...)):
    if not file.filename.endswith(".db"):
        return RedirectResponse(url="/?error=InvalidFileType", status_code=303)
    
    content = await file.read()
    success = db.replace_database(content)
    
    if success:
        db.log_event("📦 Database file manually uploaded & replaced by admin.")
        return RedirectResponse(url="/?success=Database_Successfully_Replaced", status_code=303)
    else:
        return RedirectResponse(url="/?error=Database_Upload_Failed", status_code=303)


# ─────────────────────────────────────────────────────────────
# API — BOT TOGGLE
# ─────────────────────────────────────────────────────────────

@app.post("/api/toggle")
async def toggle_bot():
    config     = db.get_config()
    new_status = not config.get("is_running", False)
    db.update_config("is_running", new_status)

    if new_status:
        bot_manager.start_bot()
        db.log_event("🚀 Bot launched via dashboard")
    else:
        bot_manager.stop_bot()
        db.log_event("🛑 Bot stopped via dashboard")

    return RedirectResponse(url="/", status_code=303)


# ─────────────────────────────────────────────────────────────
# API — SETTINGS
# ─────────────────────────────────────────────────────────────

@app.post("/api/settings")
async def update_settings(
    bot_token:        str = Form(""),
    owner_username:   str = Form(""),
    support_channel:  str = Form(""),
):
    owner_username   = owner_username.strip().replace("@", "")
    support_channel  = support_channel.strip().replace("@", "")
    token            = bot_token.strip()

    if token:
        db.update_config("bot_token", token)
    if owner_username:
        db.update_config("owner_username", owner_username)
    if support_channel is not None:
        db.update_config("support_channel", support_channel)

    db.log_event(f"⚙️ Config updated: owner={owner_username} support={support_channel}")

    config = db.get_config()
    if config.get("is_running", False):
        bot_manager.restart_bot()

    return RedirectResponse(url="/?success=SettingsSaved", status_code=303)


# ─────────────────────────────────────────────────────────────
# API — BROADCAST
# ─────────────────────────────────────────────────────────────

@app.post("/api/broadcast")
async def broadcast(
    message: str = Form(""),
    target:  str = Form("groups"),
):
    if not bot_manager.bot:
        return RedirectResponse(url="/?error=BotNotRunning", status_code=303)

    try:
        targets = list(
            db.get_all_users().keys() if target == "users"
            else db.get_all_groups().keys()
        )
        count = 0
        for tid in targets:
            try:
                bot_manager.bot.send_message(
                    int(tid),
                    f"📢 <b>BROADCAST</b>\n\n{message}",
                    parse_mode="HTML"
                )
                count += 1
                await asyncio.sleep(0.05)
            except Exception:
                pass

        db.log_event(f"📢 Broadcast sent to {count} {target}")
        return RedirectResponse(url=f"/?success=BroadcastSentTo{count}", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/?error=BroadcastFailed:{str(e).replace(' ','_')}", status_code=303)


# ─────────────────────────────────────────────────────────────
# API — GROUP SCANNER
# ─────────────────────────────────────────────────────────────

@app.post("/api/scan_group")
async def scan_group(group_id: str = Form("")):
    if not bot_manager.bot:
        return RedirectResponse(url="/?error=BotNotRunning", status_code=303)
    try:
        chat_id      = int(group_id.strip())
        chat         = bot_manager.bot.get_chat(chat_id)
        member_count = bot_manager.bot.get_chat_member_count(chat_id)
        bot_member   = bot_manager.bot.get_chat_member(chat_id, bot_manager.bot.get_me().id)

        db.ensure_group(chat_id, name=getattr(chat, 'title', 'Unknown'))
        db.update_group_setting(chat_id, 'member_count', member_count)

        info = (f"Title: {chat.title} | Type: {chat.type} | "
                f"Members: {member_count} | Bot Status: {bot_member.status}")
        db.log_event(f"🔍 Scanned: {chat.title} ({chat_id}) — {member_count} members")
        return RedirectResponse(url=f"/?success=ScanComplete:_{info}", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/?error=ScanFailed:{str(e).replace(' ','_')}", status_code=303)


# ─────────────────────────────────────────────────────────────
# API — MESSAGE LINK CONTROL (delete / pin / unpin)
# ─────────────────────────────────────────────────────────────

@app.post("/api/message_action")
async def message_action_api(
    message_link: str = Form(""),
    action:       str = Form(""),
):
    if not bot_manager.bot:
        return JSONResponse({"ok": False, "error": "Bot is not running"}, status_code=400)

    chat_id, msg_id = parse_telegram_link(message_link.strip())
    if chat_id is None:
        return JSONResponse({"ok": False, "error": "Invalid message link. Use https://t.me/c/... or https://t.me/username/..."}, status_code=400)

    try:
        if action == "delete":
            bot_manager.bot.delete_message(chat_id, msg_id)
            label = "deleted"
        elif action == "pin":
            bot_manager.bot.pin_chat_message(chat_id, msg_id)
            label = "pinned"
        elif action == "unpin":
            bot_manager.bot.unpin_chat_message(chat_id, msg_id)
            label = "unpinned"
        else:
            return JSONResponse({"ok": False, "error": "Invalid action. Use delete/pin/unpin"}, status_code=400)

        db.log_event(f"🔗 Remote {label}: {message_link}")
        return JSONResponse({"ok": True, "action": label})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ─────────────────────────────────────────────────────────────
# API — ANTISPAM TOGGLE PER GROUP (from website)
# ─────────────────────────────────────────────────────────────

@app.post("/api/antispam_toggle")
async def antispam_toggle_group(chat_id: str = Form("")):
    group = db.get_group(chat_id.strip())
    if not group:
        return JSONResponse({"ok": False, "error": "Group not found"}, status_code=404)
    new_status = not group.get("antispam", False)
    db.update_group_setting(chat_id.strip(), "antispam", new_status)
    db.log_event(f"🛡️ Antispam toggled {'ON' if new_status else 'OFF'} for group {chat_id} via dashboard")
    return JSONResponse({"ok": True, "antispam": new_status})


# ─────────────────────────────────────────────────────────────
# API — SEND MESSAGE TO A SPECIFIC GROUP
# ─────────────────────────────────────────────────────────────

@app.post("/api/group_message")
async def group_message(
    chat_id: str = Form(""),
    message: str = Form(""),
):
    if not bot_manager.bot:
        return JSONResponse({"ok": False, "error": "Bot is not running"}, status_code=400)
    if not chat_id.strip() or not message.strip():
        return JSONResponse({"ok": False, "error": "chat_id and message are required"}, status_code=400)
    try:
        bot_manager.bot.send_message(int(chat_id.strip()), message.strip(), parse_mode="HTML")
        db.log_event(f"📨 Direct message sent to group {chat_id}")
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ─────────────────────────────────────────────────────────────
# API — INTELLIGENCE SYNC
# ─────────────────────────────────────────────────────────────

@app.post("/api/sync_data")
async def sync_data(background_tasks: BackgroundTasks):
    if not bot_manager.bot:
        return RedirectResponse(url="/?error=BotNotRunning", status_code=303)
    background_tasks.add_task(perform_sync)
    return RedirectResponse(url="/?success=SyncStartedInBackground", status_code=303)


async def perform_sync():
    try:
        groups = list(db.get_all_groups().keys())
        for gid in groups:
            try:
                chat         = bot_manager.bot.get_chat(int(gid))
                member_count = bot_manager.bot.get_chat_member_count(int(gid))
                db.ensure_group(gid, name=getattr(chat, 'title', None))
                db.update_group_setting(gid, 'member_count', member_count)
                db.log_event(f"✅ Synced: {getattr(chat, 'title', gid)}")
            except Exception as e:
                db.log_event(f"⚠️ Sync failed ({gid}): {str(e)[:60]}")
            await asyncio.sleep(0.5)
        db.log_event(f"🧠 Sync complete — {len(groups)} groups")
    except Exception as e:
        import logging
        logging.error(f"Sync Error: {e}")


# ─────────────────────────────────────────────────────────────
# API — MANUAL ADD
# ─────────────────────────────────────────────────────────────

@app.post("/api/manual_add")
async def manual_add(
    type: str = Form(""),
    id:   str = Form(""),
    name: str = Form(""),
):
    try:
        int(id.strip())
    except ValueError:
        return RedirectResponse(url="/?error=InvalidIDFormat", status_code=303)

    try:
        if type == "user":
            db.ensure_user(id.strip(), name=name or "Unknown")
            db.log_event(f"➕ Manually added user {id} ({name})")
        elif type == "group":
            db.ensure_group(id.strip(), name=name or "Unknown Group")
            db.log_event(f"➕ Manually added group {id} ({name})")
        else:
            return RedirectResponse(url="/?error=InvalidType", status_code=303)
        return RedirectResponse(url="/?success=AddedManually", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/?error=AddFailed:{str(e).replace(' ','_')}", status_code=303)


# ─────────────────────────────────────────────────────────────
# API — DELETE ENTRY
# ─────────────────────────────────────────────────────────────

@app.post("/api/delete_entry")
async def delete_entry(
    type: str = Form(""),
    id:   str = Form(""),
):
    try:
        if type == "user":
            db.delete_user(id)
            db.log_event(f"🗑️ Deleted user {id}")
        elif type == "group":
            db.delete_group(id)
            db.log_event(f"🗑️ Deleted group {id}")
        return RedirectResponse(url="/?success=DeletedSuccessfully", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/?error=DeleteFailed:{str(e).replace(' ','_')}", status_code=303)


# ─────────────────────────────────────────────────────────────
# API — RESET WARNINGS
# ─────────────────────────────────────────────────────────────

@app.post("/api/reset_warnings")
async def reset_warnings(user_id: str = Form("")):
    try:
        db.reset_warnings(user_id.strip())
        db.log_event(f"🔄 Warnings reset for user {user_id}")
        return RedirectResponse(url="/?success=WarningsReset", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/?error=ResetFailed:{str(e).replace(' ','_')}", status_code=303)


# ─────────────────────────────────────────────────────────────
# API — CLEAR LOGS
# ─────────────────────────────────────────────────────────────

@app.post("/api/clear_logs")
async def clear_logs():
    db.clear_logs()
    db.log_event("🧹 Logs cleared by admin")
    return RedirectResponse(url="/?success=LogsCleared", status_code=303)


# ─────────────────────────────────────────────────────────────
# API — REMOTE USER ACTION (ban/kick/mute/unmute — no promote)
# ─────────────────────────────────────────────────────────────

@app.post("/api/remote_action")
async def remote_action(
    group_id: str = Form(""),
    user_id:  str = Form(""),
    action:   str = Form(""),
):
    if not bot_manager.bot:
        return JSONResponse({"ok": False, "error": "Bot not running"}, status_code=400)

    try:
        chat_id   = int(group_id.strip())
        target_id = int(user_id.strip())
    except ValueError:
        return JSONResponse({"ok": False, "error": "Invalid group or user ID"}, status_code=400)

    try:
        if action == "ban":
            bot_manager.bot.ban_chat_member(chat_id, target_id)
            label = "banned"
            db.log_event(f"🔨 Remote ban: user {target_id} in group {chat_id}")
        elif action == "kick":
            bot_manager.bot.ban_chat_member(chat_id, target_id)
            bot_manager.bot.unban_chat_member(chat_id, target_id)
            label = "kicked"
            db.log_event(f"👢 Remote kick: user {target_id} in group {chat_id}")
        elif action == "mute":
            bot_manager.bot.restrict_chat_member(
                chat_id, target_id,
                permissions=tg_types.ChatPermissions(can_send_messages=False)
            )
            label = "muted"
            db.log_event(f"🔇 Remote mute: user {target_id} in group {chat_id}")
        elif action == "unmute":
            bot_manager.bot.restrict_chat_member(
                chat_id, target_id,
                permissions=tg_types.ChatPermissions(
                    can_send_messages=True, can_send_audios=True, can_send_documents=True,
                    can_send_photos=True, can_send_videos=True, can_send_video_notes=True,
                    can_send_voice_notes=True, can_send_polls=True,
                    can_send_other_messages=True, can_add_web_page_previews=True,
                )
            )
            label = "unmuted"
            db.log_event(f"🔊 Remote unmute: user {target_id} in group {chat_id}")
        elif action == "warn":
            warnings = db.add_warning(target_id)
            label    = f"warned ({warnings}/3)"
            if warnings >= 3:
                bot_manager.bot.ban_chat_member(chat_id, target_id)
                db.reset_warnings(target_id)
                label = "banned (3 warnings)"
            db.log_event(f"⚠️ Remote warn: user {target_id} in group {chat_id}")
        elif action == "promote":
            bot_manager.bot.promote_chat_member(chat_id, target_id,
                can_change_info=True, can_post_messages=True, can_edit_messages=True,
                can_delete_messages=True, can_invite_users=True, can_restrict_members=True,
                can_pin_messages=True, can_promote_members=False)
            label = "promoted to admin"
            db.log_event(f"⏫ Remote promote: user {target_id} in group {chat_id}")
        elif action == "demote":
            bot_manager.bot.promote_chat_member(chat_id, target_id,
                can_change_info=False, can_post_messages=False, can_edit_messages=False,
                can_delete_messages=False, can_invite_users=False, can_restrict_members=False,
                can_pin_messages=False, can_promote_members=False)
            label = "demoted from admin"
            db.log_event(f"⏬ Remote demote: user {target_id} in group {chat_id}")
        else:
            return JSONResponse({"ok": False, "error": "Invalid action"}, status_code=400)

        return JSONResponse({"ok": True, "action": label})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ─────────────────────────────────────────────────────────────
# API — GROUP LOCK / UNLOCK
# ─────────────────────────────────────────────────────────────

@app.post("/api/group_lock")
async def group_lock(
    chat_id: str = Form(""),
    action:  str = Form("lock"),
):
    if not bot_manager.bot:
        return JSONResponse({"ok": False, "error": "Bot not running"}, status_code=400)
    try:
        cid = int(chat_id.strip())
        if action == "lock":
            bot_manager.bot.set_chat_permissions(cid, tg_types.ChatPermissions(can_send_messages=False))
            db.log_event(f"🔒 Remote lock: group {cid}")
            return JSONResponse({"ok": True, "action": "locked"})
        elif action == "unlock":
            bot_manager.bot.set_chat_permissions(cid, tg_types.ChatPermissions(
                can_send_messages=True, can_send_media_messages=True,
                can_send_other_messages=True, can_add_web_page_previews=True
            ))
            db.log_event(f"🔓 Remote unlock: group {cid}")
            return JSONResponse({"ok": True, "action": "unlocked"})
        else:
            return JSONResponse({"ok": False, "error": "Use lock or unlock"}, status_code=400)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
