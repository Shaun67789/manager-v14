import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from database import db
import logging
import time as _time
import collections

logger = logging.getLogger(__name__)

# ── Rate-limit tracker: {(chat_id, user_id): deque of timestamps} ──
_msg_timestamps = collections.defaultdict(collections.deque)
_SPAM_MAX    = 5    # max messages
_SPAM_WINDOW = 3.0  # seconds


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def is_admin(bot, chat_id, user_id):
    try:
        member = bot.get_chat_member(chat_id, user_id)
        return member.status in ['administrator', 'creator']
    except Exception:
        return False


def is_owner(username):
    config = db.get_config()
    owner = config.get("owner_username", "")
    if not username or not owner:
        return False
    return username.lower().replace("@", "") == owner.lower()


def can_act_on(bot, chat_id, executor_id, executor_username, target_id, target_username):
    if is_owner(executor_username):
        return True
    if is_owner(target_username):
        return False
    if is_admin(bot, chat_id, target_id):
        return False
    return True


def get_target_user(message):
    if message.reply_to_message:
        return message.reply_to_message.from_user
    parts = message.text.split()
    if len(parts) > 1:
        target = parts[1].replace("@", "")
        try:
            return int(target)
        except ValueError:
            return None
    return None


def build_antispam_markup(chat_id, is_on):
    markup = InlineKeyboardMarkup()
    markup.row(
        InlineKeyboardButton(
            "✅ ON" if is_on else "🔘 ON",
            callback_data=f"antispam:on:{chat_id}"
        ),
        InlineKeyboardButton(
            "🔘 OFF" if is_on else "❌ OFF",
            callback_data=f"antispam:off:{chat_id}"
        )
    )
    return markup


# ─────────────────────────────────────────────────────────────
# HANDLER REGISTRATION
# ─────────────────────────────────────────────────────────────

def register_handlers(bot):

    # ── Group tracking & Welcome ──
    @bot.message_handler(content_types=['left_chat_member', 'new_chat_members'])
    def track_group(message):
        bot_id = bot.get_me().id

        if message.content_type == 'new_chat_members':
            for member in message.new_chat_members:
                if member.id == bot_id:
                    db.add_group(message.chat.id, name=getattr(message.chat, 'title', None))
                    db.log_event(f"✅ Bot added to group: {message.chat.title} ({message.chat.id})")
                else:
                    group = db.get_group(message.chat.id)
                    if not group:
                        continue
                    welcome_text    = group.get("welcome_message", "Welcome, {name}! 👋")
                    welcome_type    = group.get("welcome_type", "text")
                    welcome_file_id = group.get("welcome_file_id", "")
                    
                    name_html = f"<b>{member.first_name}</b>"
                    text = (welcome_text
                            .replace("{name}", name_html)
                            .replace("{id}", str(member.id)))
                    try:
                        if welcome_type == "photo" and welcome_file_id:
                            bot.send_photo(message.chat.id, welcome_file_id,
                                           caption=text, parse_mode="HTML")
                        elif welcome_type == "gif" and welcome_file_id:
                            bot.send_animation(message.chat.id, welcome_file_id,
                                               caption=text, parse_mode="HTML")
                        else:
                            bot.send_message(message.chat.id, text, parse_mode="HTML")
                    except Exception:
                        try:
                            bot.send_message(message.chat.id, text, parse_mode="HTML")
                        except Exception:
                            pass

        elif message.content_type == 'left_chat_member':
            if message.left_chat_member.id == bot_id:
                db.remove_group(message.chat.id)
                db.log_event(f"❌ Bot removed from group: {message.chat.title} ({message.chat.id})")

    # ── /start ──
    @bot.message_handler(commands=['start'])
    def cmd_start(message):
        uname = getattr(message.from_user, 'username', None)
        db.ensure_user(message.from_user.id, name=message.from_user.first_name, username=uname)
        db.log_event(f"User {message.from_user.first_name} ({message.from_user.id}) sent /start")
        if message.chat.type in ['group', 'supergroup']:
            db.ensure_group(message.chat.id, name=getattr(message.chat, 'title', None))

        config          = db.get_config()
        owner           = config.get("owner_username", "OwnerUser123")
        support_channel = config.get("support_channel", "").strip().replace("@", "")
        bot_username    = bot.get_me().username

        markup = InlineKeyboardMarkup()

        # ── Row 1: Add Me to Your Group (most prominent, full-width) ──
        markup.row(
            InlineKeyboardButton(
                "➕ Add Me to Your Group",
                url=f"https://t.me/{bot_username}?startgroup=true"
            )
        )

        # ── Row 2: Commands & Help + Support Channel ──
        row2 = [InlineKeyboardButton("📜 Commands & Help", callback_data="show_help")]
        if support_channel:
            row2.append(InlineKeyboardButton("📢 Support Channel", url=f"https://t.me/{support_channel}"))
        markup.row(*row2)

        # ── Row 3: Contact Owner ──
        markup.row(InlineKeyboardButton("👤 Contact Owner", url=f"https://t.me/{owner}"))

        first = message.from_user.first_name
        text = (
            f"<b>✨ Hey {first}! Welcome to the Ultimate Group Manager</b>\n\n"
            "I'm your <b>premium all-in-one</b> Telegram group moderation bot. "
            "Add me to your group and take full control of your community.\n\n"
            "🛡️ <b>Core Features:</b>\n"
            "  ⚡ <b>Anti-Spam</b> — Flood control + invite link blocking\n"
            "  🤬 <b>Bad Words Filter</b> — Auto-delete + 3-strike warn/ban\n"
            "  🎉 <b>Smart Welcome</b> — Greet with photo, GIF or custom text\n"
            "  🔨 <b>Full Moderation</b> — Ban, kick, mute, warn, promote\n"
            "  📌 <b>Admin Tools</b> — Pin, lock, filters, rules & more\n"
            "  🌍 <b>Web Dashboard</b> — Remote control from anywhere\n\n"
            "<i>👇 Tap <b>Add Me to Your Group</b> to get started!</i>"
        )
        bot.reply_to(message, text, reply_markup=markup, parse_mode="HTML")

    @bot.callback_query_handler(func=lambda call: call.data == "show_help")
    def help_callback(call):
        cmd_help(call.message)
        bot.answer_callback_query(call.id)

    # ── /help ──
    @bot.message_handler(commands=['help'])
    def cmd_help(message):
        text = ("<b>🛠️ Bot Commands List</b>\n\n"
                "<b>🛡️ Moderation:</b>\n"
                "• /ban - Permanent ban\n"
                "• /unban - Revoke ban\n"
                "• /kick - Remove user (can rejoin)\n"
                "• /mute - Silence user\n"
                "• /unmute - Restore chat\n"
                "• /warn - Formal warning (3 = ban)\n\n"
                "<b>👮 Admin Tools:</b>\n"
                "• /lock - Lock group (Admins only)\n"
                "• /unlock - Unlock group\n"
                "• /promote - Give admin rights\n"
                "• /demote - Remove admin rights\n"
                "• /settitle - Change group name\n"
                "• /setdesc - Change group description\n"
                "• /pin - Pin message\n"
                "• /unpin - Unpin all\n"
                "• /del - Delete message\n"
                "• /report - Alert group admins\n"
                "• /link - Get group invite link\n\n"
                "<b>🤖 Automation & Auto-Mod:</b>\n"
                "• /setwelcome - Set greeting (reply to text/photo/GIF)\n"
                "• /setrules - Set group rules\n"
                "• /rules - Show group rules\n"
                "• /addfilter - Create auto-reply\n"
                "• /removefilter - Delete auto-reply\n"
                "• /filters - List active filters\n"
                "• /addbadword - Add word to auto-mod\n"
                "• /delbadword - Remove auto-mod word\n"
                "• /antispam - Toggle Anti-Spam (inline buttons)\n\n"
                "<b>📊 Information:</b>\n"
                "• /info - User profile & status\n"
                "• /admins - List all group admins\n"
                "• /send - Bot sends a message\n"
                "• /start - Bot introduction")

        if message.chat.type == 'private':
            bot.send_message(message.chat.id, text, parse_mode="HTML")
        else:
            bot.reply_to(message, "📥 Help sent to your private messages!")
            try:
                bot.send_message(message.from_user.id, text, parse_mode="HTML")
            except Exception:
                bot.reply_to(message, "⚠️ Please start me in private chat first so I can DM you the help menu.")

    # ── /info ──
    @bot.message_handler(commands=['info'])
    def cmd_info(message):
        db.ensure_user(message.from_user.id, name=message.from_user.first_name)
        target = get_target_user(message)

        if not target:
            target_user = message.from_user
        elif isinstance(target, int):
            user_data = db.get_user(target)
            name = user_data.get("name", "Unknown") if user_data else "Unknown"
            class TempUser:
                id = target
                first_name = name
                username = None
            target_user = TempUser()
        else:
            target_user = target

        user_data = db.get_user(target_user.id)
        warnings = user_data.get("warnings", 0) if user_data else 0
        role = "Member"
        status_text = "N/A"

        if message.chat.type in ['group', 'supergroup']:
            try:
                member_info = bot.get_chat_member(message.chat.id, target_user.id)
                if member_info.status == 'creator':
                    role = "👑 Owner / Creator"
                elif member_info.status == 'administrator':
                    role = "🛡️ Administrator"
                elif member_info.status == 'restricted':
                    role = "⚠️ Restricted User"
                if hasattr(target_user, 'username') and target_user.username:
                    status_text = f"@{target_user.username}"
            except Exception:
                pass

        if is_owner(getattr(target_user, 'username', None)):
            role = "⭐ Global Owner"

        text = (f"<b>👤 User Intelligence</b>\n\n"
                f"<b>📛 Name:</b> {target_user.first_name}\n"
                f"<b>🆔 ID:</b> <code>{target_user.id}</code>\n"
                f"<b>🎭 Role:</b> {role}\n"
                f"<b>🌐 Username:</b> {status_text}\n"
                f"<b>⚠️ Warnings:</b> {warnings}/3\n"
                f"<b>📅 First Seen:</b> {'Recorded' if user_data else 'New Arrival'}")
        bot.reply_to(message, text, parse_mode="HTML")

    # ── /admins ──
    @bot.message_handler(commands=['admins'])
    def cmd_admins(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        try:
            admins = bot.get_chat_administrators(message.chat.id)
            text = f"<b>🛡️ Administrators in {message.chat.title}</b>\n\n"
            for admin in admins:
                if admin.user.is_bot:
                    continue
                symbol = "👑" if admin.status == 'creator' else "🛡️"
                text += f"{symbol} {admin.user.first_name} (<code>{admin.user.id}</code>)\n"
            bot.reply_to(message, text, parse_mode="HTML")
        except Exception as e:
            bot.reply_to(message, f"Failed to fetch admin list: {e}")

    # ── /ban ──
    @bot.message_handler(commands=['ban'])
    def cmd_ban(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        target = get_target_user(message)
        if not target:
            return bot.reply_to(message, "Reply to a user or provide their ID to ban.")
        t_id = target if isinstance(target, int) else target.id
        t_uname = None if isinstance(target, int) else target.username
        if not can_act_on(bot, message.chat.id, message.from_user.id, message.from_user.username, t_id, t_uname):
            return bot.reply_to(message, "⚠️ Cannot perform this action on this user (Admin/Owner protection).")
        try:
            bot.ban_chat_member(message.chat.id, t_id)
            db.log_event(f"Admin {message.from_user.id} banned {t_id} in {message.chat.id}")
            bot.reply_to(message, f"🔨 User <code>{t_id}</code> has been <b>banned</b>.", parse_mode="HTML")
        except Exception as e:
            bot.reply_to(message, f"Failed to ban: {str(e)}")

    # ── /kick ──
    @bot.message_handler(commands=['kick'])
    def cmd_kick(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        target = get_target_user(message)
        if not target:
            return bot.reply_to(message, "Reply to a user or provide their ID to kick.")
        t_id = target if isinstance(target, int) else target.id
        t_uname = None if isinstance(target, int) else target.username
        if not can_act_on(bot, message.chat.id, message.from_user.id, message.from_user.username, t_id, t_uname):
            return
        try:
            bot.ban_chat_member(message.chat.id, t_id)
            bot.unban_chat_member(message.chat.id, t_id)
            bot.reply_to(message, f"👢 User <code>{t_id}</code> has been <b>kicked</b> from the group.", parse_mode="HTML")
        except Exception as e:
            bot.reply_to(message, f"Failed to kick: {str(e)}")

    # ── /mute ──
    @bot.message_handler(commands=['mute'])
    def cmd_mute(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        target = get_target_user(message)
        if not target:
            return bot.reply_to(message, "Reply to a user or provide their ID.")
        t_id = target if isinstance(target, int) else target.id
        if not can_act_on(bot, message.chat.id, message.from_user.id, message.from_user.username, t_id, getattr(target, 'username', None)):
            return
        try:
            bot.restrict_chat_member(
                message.chat.id, t_id,
                permissions=telebot.types.ChatPermissions(can_send_messages=False)
            )
            bot.reply_to(message, f"🔇 User <code>{t_id}</code> has been <b>muted</b>.", parse_mode="HTML")
        except Exception as e:
            bot.reply_to(message, str(e))

    # ── /unmute ──
    @bot.message_handler(commands=['unmute'])
    def cmd_unmute(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        target = get_target_user(message)
        if not target:
            return bot.reply_to(message, "Reply to a user or provide their ID.")
        t_id = target if isinstance(target, int) else target.id
        try:
            bot.restrict_chat_member(
                message.chat.id, t_id,
                permissions=telebot.types.ChatPermissions(
                    can_send_messages=True, can_send_audios=True, can_send_documents=True,
                    can_send_photos=True, can_send_videos=True, can_send_video_notes=True,
                    can_send_voice_notes=True, can_send_polls=True,
                    can_send_other_messages=True, can_add_web_page_previews=True,
                )
            )
            bot.reply_to(message, f"🔊 User <code>{t_id}</code> has been <b>unmuted</b>.", parse_mode="HTML")
        except Exception as e:
            bot.reply_to(message, str(e))

    # ── /unban ──
    @bot.message_handler(commands=['unban'])
    def cmd_unban(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        target = get_target_user(message)
        if not target:
            return bot.reply_to(message, "Reply to a user or provide their ID.")
        t_id = target if isinstance(target, int) else target.id
        try:
            bot.unban_chat_member(message.chat.id, t_id, only_if_banned=True)
            bot.reply_to(message, f"🕊️ User <code>{t_id}</code> has been <b>unbanned</b>.", parse_mode="HTML")
        except Exception as e:
            bot.reply_to(message, str(e))

    # ── /warn ──
    @bot.message_handler(commands=['warn'])
    def cmd_warn(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        target = get_target_user(message)
        if not target:
            return bot.reply_to(message, "Reply to a user or provide their ID.")
        t_id = target if isinstance(target, int) else target.id
        if not can_act_on(bot, message.chat.id, message.from_user.id, message.from_user.username, t_id, getattr(target, 'username', None)):
            return
        warnings = db.add_warning(t_id, getattr(target, 'first_name', 'Unknown'))
        if warnings >= 3:
            try:
                bot.ban_chat_member(message.chat.id, t_id)
                db.reset_warnings(t_id)
                bot.reply_to(message, f"⚠️ User <code>{t_id}</code> reached 3 warnings and was <b>banned</b>.", parse_mode="HTML")
            except Exception as e:
                bot.reply_to(message, f"⚠️ 3 warnings reached, but I couldn't ban them: {e}")
        else:
            bot.reply_to(message, f"⚠️ User <code>{t_id}</code> warned. (<b>{warnings}/3</b>)", parse_mode="HTML")

    # ── /del ──
    @bot.message_handler(commands=['del'])
    def cmd_del(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        if not message.reply_to_message:
            return bot.reply_to(message, "Reply to a message to delete it.")
        try:
            bot.delete_message(message.chat.id, message.reply_to_message.message_id)
            bot.delete_message(message.chat.id, message.message_id)
        except Exception:
            pass

    # ── /promote ──
    @bot.message_handler(commands=['promote'])
    def cmd_promote(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        target = get_target_user(message)
        if not target:
            return bot.reply_to(message, "Reply to a user to promote them.")
        t_id = target if isinstance(target, int) else target.id
        try:
            bot.promote_chat_member(message.chat.id, t_id,
                can_change_info=True, can_post_messages=True, can_edit_messages=True,
                can_delete_messages=True, can_invite_users=True, can_restrict_members=True,
                can_pin_messages=True, can_promote_members=False)
            bot.reply_to(message, f"⏫ User <code>{t_id}</code> promoted to <b>Admin</b>!", parse_mode="HTML")
        except Exception as e:
            bot.reply_to(message, f"Failed to promote: {e}")

    # ── /demote ──
    @bot.message_handler(commands=['demote'])
    def cmd_demote(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        target = get_target_user(message)
        if not target:
            return bot.reply_to(message, "Reply to a user to demote them.")
        t_id = target if isinstance(target, int) else target.id
        if not can_act_on(bot, message.chat.id, message.from_user.id, message.from_user.username, t_id, getattr(target, 'username', None)):
            return bot.reply_to(message, "⚠️ Cannot demote this user.")
        try:
            bot.promote_chat_member(message.chat.id, t_id,
                can_change_info=False, can_post_messages=False, can_edit_messages=False,
                can_delete_messages=False, can_invite_users=False, can_restrict_members=False,
                can_pin_messages=False, can_promote_members=False)
            bot.reply_to(message, f"⏬ User <code>{t_id}</code> has been <b>demoted</b>.", parse_mode="HTML")
        except Exception as e:
            bot.reply_to(message, f"Failed to demote: {e}")

    # ── /pin ──
    @bot.message_handler(commands=['pin'])
    def cmd_pin(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        if not message.reply_to_message:
            return bot.reply_to(message, "Reply to a message to pin it.")
        try:
            bot.pin_chat_message(message.chat.id, message.reply_to_message.message_id)
            bot.reply_to(message, "📌 Message pinned!")
        except Exception as e:
            bot.reply_to(message, str(e))

    # ── /unpin ──
    @bot.message_handler(commands=['unpin'])
    def cmd_unpin(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        try:
            bot.unpin_all_chat_messages(message.chat.id)
            bot.reply_to(message, "📌 All messages unpinned!")
        except Exception as e:
            bot.reply_to(message, str(e))

    # ── /report ──
    @bot.message_handler(commands=['report'])
    def cmd_report(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not message.reply_to_message:
            return bot.reply_to(message, "Reply to a message to report it to admins.")
        try:
            admins = bot.get_chat_administrators(message.chat.id)
            chat_link_id = abs(message.chat.id) % (10 ** 10)
            for admin in admins:
                if not admin.user.is_bot:
                    try:
                        bot.send_message(
                            admin.user.id,
                            f"🚨 <b>Report from {message.chat.title}</b>\n\n"
                            f"Reported by: {message.from_user.first_name}\n"
                            f"Message: <a href='https://t.me/c/{chat_link_id}/{message.reply_to_message.message_id}'>View Message</a>",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass
            bot.reply_to(message, "🚨 Admins have been notified.")
        except Exception:
            pass

    # ── /setwelcome — UPGRADED (reply-to-set: text / photo+caption / GIF+caption) ──
    @bot.message_handler(commands=['setwelcome'])
    def cmd_setwelcome(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return

        # ── Reply-based method ──
        if message.reply_to_message:
            m = message.reply_to_message
            if m.photo:
                file_id = m.photo[-1].file_id
                caption = m.caption or "Welcome, {name}! 👋"
                db.update_group_setting(message.chat.id, "welcome_type", "photo")
                db.update_group_setting(message.chat.id, "welcome_file_id", file_id)
                db.update_group_setting(message.chat.id, "welcome_message", caption)
                bot.reply_to(message,
                    "✅ <b>Welcome image set!</b>\n\n"
                    "New members will be greeted with your photo + caption.\n"
                    "<i>Use {name} or {id} for personalization.</i>",
                    parse_mode="HTML")
                return

            elif m.animation:
                file_id = m.animation.file_id
                caption = m.caption or "Welcome, {name}! 👋"
                db.update_group_setting(message.chat.id, "welcome_type", "gif")
                db.update_group_setting(message.chat.id, "welcome_file_id", file_id)
                db.update_group_setting(message.chat.id, "welcome_message", caption)
                bot.reply_to(message,
                    "✅ <b>Welcome GIF set!</b>\n\n"
                    "New members will be greeted with your animated GIF + caption.\n"
                    "<i>Use {name} or {id} for personalization.</i>",
                    parse_mode="HTML")
                return

            elif m.text:
                db.update_group_setting(message.chat.id, "welcome_type", "text")
                db.update_group_setting(message.chat.id, "welcome_file_id", "")
                db.update_group_setting(message.chat.id, "welcome_message", m.text)
                bot.reply_to(message,
                    "✅ <b>Welcome text set!</b>\n"
                    "<i>Use {name} or {id} for personalization.</i>",
                    parse_mode="HTML")
                return
            else:
                bot.reply_to(message,
                    "❌ Unsupported type. Reply to a <b>text</b>, <b>photo+caption</b>, or <b>GIF+caption</b>.",
                    parse_mode="HTML")
                return

        # ── Inline text method: /setwelcome <text> ──
        parts = message.text.split(None, 1)
        if len(parts) > 1:
            db.update_group_setting(message.chat.id, "welcome_type", "text")
            db.update_group_setting(message.chat.id, "welcome_file_id", "")
            db.update_group_setting(message.chat.id, "welcome_message", parts[1])
            bot.reply_to(message,
                "✅ <b>Welcome message updated!</b>\n<i>Use {name} or {id} to personalize.</i>",
                parse_mode="HTML")
        else:
            bot.reply_to(message,
                "💡 <b>How to set welcome:</b>\n\n"
                "1️⃣ <b>Text:</b> <code>/setwelcome Hello {name}!</code>\n"
                "2️⃣ <b>Image + Caption:</b> Send/upload a photo with caption, then <b>reply</b> to it with <code>/setwelcome</code>\n"
                "3️⃣ <b>GIF + Caption:</b> Send a GIF with caption, then <b>reply</b> to it with <code>/setwelcome</code>",
                parse_mode="HTML")


    # ── /setrules ──
    @bot.message_handler(commands=['setrules'])
    def cmd_setrules(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        parts = message.text.split(None, 1)
        if len(parts) > 1:
            db.update_group_setting(message.chat.id, "rules", parts[1])
            bot.reply_to(message, "✅ Rules updated!")

    # ── /rules ──
    @bot.message_handler(commands=['rules'])
    def cmd_rules(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        group = db.get_group(message.chat.id)
        if group:
            bot.reply_to(message, group.get("rules", "No rules set."))

    # ── /addfilter ──
    @bot.message_handler(commands=['addfilter'])
    def cmd_addfilter(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        parts = message.text.split(None, 1)
        if len(parts) < 2:
            return bot.reply_to(message, "Format: /addfilter <keyword> (reply to media/text)")
        keyword = parts[1].strip()
        if not message.reply_to_message:
            return bot.reply_to(message, "You must reply to the content you want to set as the filter response.")
        m = message.reply_to_message
        filter_data = {}
        if m.text:
            filter_data = {"type": "text", "text": m.text}
        elif m.photo:
            filter_data = {"type": "photo", "file_id": m.photo[-1].file_id, "caption": m.caption or ""}
        elif m.sticker:
            filter_data = {"type": "sticker", "file_id": m.sticker.file_id}
        elif m.animation:
            filter_data = {"type": "gif", "file_id": m.animation.file_id}
        else:
            return bot.reply_to(message, "Unsupported media type.")
        db.add_filter(message.chat.id, keyword, filter_data)
        bot.reply_to(message, f"✅ Filter '<code>{keyword}</code>' added!", parse_mode="HTML")

    # ── /removefilter ──
    @bot.message_handler(commands=['removefilter'])
    def cmd_removefilter(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        parts = message.text.split(None, 1)
        if len(parts) < 2:
            return bot.reply_to(message, "Format: /removefilter <keyword>")
        db.remove_filter(message.chat.id, parts[1].strip())
        bot.reply_to(message, f"✅ Filter '<code>{parts[1].strip()}</code>' removed.", parse_mode="HTML")

    # ── /filters ──
    @bot.message_handler(commands=['filters'])
    def cmd_list_filters(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        group = db.get_group(message.chat.id)
        if not group or not group.get("filters"):
            return bot.reply_to(message, "No filters defined for this group.")
        fts = group["filters"].keys()
        text = "<b>🔍 Active Group Filters:</b>\n\n" + "\n".join([f"• <code>{f}</code>" for f in fts])
        bot.reply_to(message, text, parse_mode="HTML")

    # ── /lock ──
    @bot.message_handler(commands=['lock'])
    def cmd_lock(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        try:
            bot.set_chat_permissions(message.chat.id, telebot.types.ChatPermissions(can_send_messages=False))
            bot.reply_to(message, "🔒 <b>Group Locked!</b> Regular members can no longer send messages.", parse_mode="HTML")
        except Exception as e:
            bot.reply_to(message, f"Failed to lock: {e}")

    # ── /unlock ──
    @bot.message_handler(commands=['unlock'])
    def cmd_unlock(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        try:
            bot.set_chat_permissions(message.chat.id, telebot.types.ChatPermissions(
                can_send_messages=True, can_send_media_messages=True,
                can_send_other_messages=True, can_add_web_page_previews=True))
            bot.reply_to(message, "🔓 <b>Group Unlocked!</b> Regular members can now speak.", parse_mode="HTML")
        except Exception as e:
            bot.reply_to(message, f"Failed to unlock: {e}")

    # ── /link ──
    @bot.message_handler(commands=['link'])
    def cmd_link(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        try:
            invite_link = bot.export_chat_invite_link(message.chat.id)
            bot.reply_to(message, f"🔗 <b>Invite Link:</b>\n{invite_link}", parse_mode="HTML")
        except Exception:
            bot.reply_to(message, "⚠️ Failed to fetch link. Ensure I have the 'Invite Users' admin permission.")

    # ── /addbadword ──
    @bot.message_handler(commands=['addbadword'])
    def cmd_addbadword(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        parts = message.text.split(None, 1)
        if len(parts) > 1:
            word = parts[1].strip().lower()
            group = db.get_group(message.chat.id)
            bad_words = group.get("bad_words", [])
            if word not in bad_words:
                bad_words.append(word)
                db.update_group_setting(message.chat.id, "bad_words", bad_words)
            bot.reply_to(message, f"🔇 <b>Auto-Mod:</b> Added '<code>{word}</code>' to the filter list.", parse_mode="HTML")
        else:
            bot.reply_to(message, "Format: /addbadword <word>")

    # ── /delbadword ──
    @bot.message_handler(commands=['delbadword'])
    def cmd_delbadword(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        parts = message.text.split(None, 1)
        if len(parts) > 1:
            word = parts[1].strip().lower()
            group = db.get_group(message.chat.id)
            bad_words = group.get("bad_words", [])
            if word in bad_words:
                bad_words.remove(word)
                db.update_group_setting(message.chat.id, "bad_words", bad_words)
                bot.reply_to(message, f"✅ Removed '<code>{word}</code>' from the filter.", parse_mode="HTML")
            else:
                bot.reply_to(message, "That word is not in the filter.")
        else:
            bot.reply_to(message, "Format: /delbadword <word>")

    # ── /antispam — UPGRADED with inline buttons ──
    @bot.message_handler(commands=['antispam'])
    def cmd_antispam(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        group = db.get_group(message.chat.id)
        current = group.get("antispam", False) if group else False
        status = "🟢 <b>ACTIVE</b>" if current else "🔴 <b>INACTIVE</b>"
        markup = build_antispam_markup(message.chat.id, current)
        text = (f"🛡️ <b>Anti-Spam Control Panel</b>\n\n"
                f"Status: {status}\n\n"
                f"<b>Protections enabled when ON:</b>\n"
                f"• 🔗 Block Telegram invite links\n"
                f"• ⚡ Rate limiting (max {_SPAM_MAX} msgs / {int(_SPAM_WINDOW)}s)\n"
                f"• Violations: warn → 3 warns → auto-ban\n\n"
                f"<i>Use the buttons below to toggle.</i>")
        bot.reply_to(message, text, reply_markup=markup, parse_mode="HTML")

    # ── Antispam inline callback ──
    @bot.callback_query_handler(func=lambda call: call.data.startswith("antispam:"))
    def antispam_toggle_callback(call):
        try:
            _, action, chat_id_str = call.data.split(":")
            chat_id = int(chat_id_str)
        except (ValueError, AttributeError):
            bot.answer_callback_query(call.id, "❌ Invalid data")
            return

        if not is_admin(bot, chat_id, call.from_user.id) and not is_owner(call.from_user.username):
            bot.answer_callback_query(call.id, "⚠️ Admin only!", show_alert=True)
            return

        new_status = (action == "on")
        db.update_group_setting(chat_id, "antispam", new_status)
        db.log_event(f"🛡️ Anti-Spam {'ON' if new_status else 'OFF'} in chat {chat_id} by {call.from_user.first_name}")

        status = "🟢 <b>ACTIVE</b>" if new_status else "🔴 <b>INACTIVE</b>"
        markup = build_antispam_markup(chat_id, new_status)
        text = (f"🛡️ <b>Anti-Spam Control Panel</b>\n\n"
                f"Status: {status}\n\n"
                f"<b>Protections enabled when ON:</b>\n"
                f"• 🔗 Block Telegram invite links\n"
                f"• ⚡ Rate limiting (max {_SPAM_MAX} msgs / {int(_SPAM_WINDOW)}s)\n"
                f"• Violations: warn → 3 warns → auto-ban\n\n"
                f"<i>Use the buttons below to toggle.</i>")
        try:
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id,
                                  reply_markup=markup, parse_mode="HTML")
        except Exception:
            pass
        bot.answer_callback_query(call.id, f"Anti-Spam {'Enabled ✅' if new_status else 'Disabled ❌'}")

    # ── /settitle ──
    @bot.message_handler(commands=['settitle'])
    def cmd_settitle(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        parts = message.text.split(None, 1)
        if len(parts) > 1:
            try:
                bot.set_chat_title(message.chat.id, parts[1])
                bot.reply_to(message, "✅ <b>Group Title Updated!</b>", parse_mode="HTML")
            except Exception:
                bot.reply_to(message, "⚠️ Failed. Ensure I have 'Change Group Info' admin rights.")
        else:
            bot.reply_to(message, "Format: /settitle <New Title>")

    # ── /setdesc ──
    @bot.message_handler(commands=['setdesc'])
    def cmd_setdesc(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        parts = message.text.split(None, 1)
        if len(parts) > 1:
            try:
                bot.set_chat_description(message.chat.id, parts[1])
                bot.reply_to(message, "✅ <b>Group Description Updated!</b>", parse_mode="HTML")
            except Exception:
                bot.reply_to(message, "⚠️ Failed. Ensure I have 'Change Group Info' admin rights.")
        else:
            bot.reply_to(message, "Format: /setdesc <New Description>")

    # ── /send ──
    @bot.message_handler(commands=['send'])
    def cmd_send_msg(message):
        if message.chat.type not in ['group', 'supergroup']:
            return
        if not is_admin(bot, message.chat.id, message.from_user.id) and not is_owner(message.from_user.username):
            return
        parts = message.text.split(None, 1)
        if len(parts) < 2:
            return bot.reply_to(message, "Format: /send <message>")
        try:
            bot.send_message(message.chat.id, parts[1])
            bot.delete_message(message.chat.id, message.message_id)
        except Exception as e:
            bot.reply_to(message, f"Error: {e}")

    # ── Register bot commands ──
    try:
        commands = [
            telebot.types.BotCommand("start",        "Bot introduction"),
            telebot.types.BotCommand("help",         "Full command list"),
            telebot.types.BotCommand("info",         "User profile & stats"),
            telebot.types.BotCommand("ban",          "Permanently ban a user"),
            telebot.types.BotCommand("kick",         "Remove user from group"),
            telebot.types.BotCommand("mute",         "Restrict user from talking"),
            telebot.types.BotCommand("unmute",       "Restore talking privileges"),
            telebot.types.BotCommand("warn",         "Issue a formal warning"),
            telebot.types.BotCommand("lock",         "Lock the group"),
            telebot.types.BotCommand("unlock",       "Unlock the group"),
            telebot.types.BotCommand("promote",      "Promote to Administrator"),
            telebot.types.BotCommand("demote",       "Remove Administrator rights"),
            telebot.types.BotCommand("link",         "Fetch group invite link"),
            telebot.types.BotCommand("settitle",     "Change Group Title"),
            telebot.types.BotCommand("setdesc",      "Change Group Description"),
            telebot.types.BotCommand("addbadword",   "Add word to Auto-Mod filter"),
            telebot.types.BotCommand("delbadword",   "Remove word from filter"),
            telebot.types.BotCommand("antispam",     "Toggle Anti-Spam (inline buttons)"),
            telebot.types.BotCommand("pin",          "Pin a message"),
            telebot.types.BotCommand("unpin",        "Unpin all messages"),
            telebot.types.BotCommand("del",          "Delete a replied message"),
            telebot.types.BotCommand("rules",        "Show group rules"),
            telebot.types.BotCommand("setrules",     "Set group rules"),
            telebot.types.BotCommand("setwelcome",   "Set welcome (reply to text/photo/GIF)"),
            telebot.types.BotCommand("addfilter",    "Add auto-reply filter"),
            telebot.types.BotCommand("removefilter", "Remove auto-reply filter"),
            telebot.types.BotCommand("filters",      "List active filters"),
            telebot.types.BotCommand("admins",       "List all group admins"),
            telebot.types.BotCommand("send",         "Bot sends a custom message"),
            telebot.types.BotCommand("report",       "Alert admins about a message"),
        ]
        bot.set_my_commands(commands)
    except Exception as e:
        logger.error(f"Failed to register commands: {e}")

    # ── CATCH-ALL: rate-limit spam + link spam + bad words + filters ──
    @bot.message_handler(
        func=lambda m: True,
        content_types=['text', 'photo', 'video', 'sticker', 'animation', 'document', 'voice', 'audio']
    )
    def all_messages(message):
        if message.chat.type not in ['group', 'supergroup']:
            return

        # Track in DB
        db.ensure_group(message.chat.id, name=getattr(message.chat, 'title', None))
        db.increment_messages(message.chat.id)
        uname = getattr(message.from_user, 'username', None)
        db.ensure_user(message.from_user.id, name=message.from_user.first_name, username=uname)

        group = db.get_group(message.chat.id)
        if not group:
            return

        user_is_admin = is_admin(bot, message.chat.id, message.from_user.id)
        user_is_owner = is_owner(message.from_user.username)

        # ── ⚡ Rate-limit anti-spam (applies to ALL message types) ──
        if not user_is_admin and not user_is_owner:
            now = _time.time()
            key = (message.chat.id, message.from_user.id)
            dq  = _msg_timestamps[key]
            dq.append(now)
            # Evict timestamps outside window
            while dq and dq[0] < now - _SPAM_WINDOW:
                dq.popleft()

            if len(dq) > _SPAM_MAX:
                try:
                    bot.delete_message(message.chat.id, message.message_id)
                    warnings = db.add_warning(message.from_user.id, message.from_user.first_name)
                    dq.clear()  # reset counter after action
                    if warnings >= 3:
                        bot.ban_chat_member(message.chat.id, message.from_user.id)
                        db.reset_warnings(message.from_user.id)
                        bot.send_message(
                            message.chat.id,
                            f"⚡ <b>{message.from_user.first_name}</b> was <b>banned</b> for spamming "
                            f"(3 flood warnings triggered).",
                            parse_mode="HTML"
                        )
                        db.log_event(f"⚡ Spam-ban: {message.from_user.id} in {message.chat.id}")
                    else:
                        bot.send_message(
                            message.chat.id,
                            f"⚡ <b>{message.from_user.first_name}</b>, slow down! "
                            f"Flood detected. Warning <b>{warnings}/3</b>",
                            parse_mode="HTML"
                        )
                except Exception:
                    pass
                return

        # ── Text-only checks beyond this point ──
        if not message.text:
            return

        text_lower = message.text.lower()

        # ── 🔗 Invite link protection ──
        antispam_active = group.get("antispam", False)
        if antispam_active and not user_is_admin and not user_is_owner:
            if ("t.me/" in text_lower or "telegram.me/" in text_lower
                    or "telegram.dog/" in text_lower or "joinchat" in text_lower):
                try:
                    bot.delete_message(message.chat.id, message.message_id)
                    bot.send_message(
                        message.chat.id,
                        f"🚫 <b>Anti-Spam</b>: {message.from_user.first_name}, "
                        f"Telegram invite links are <b>not allowed</b> here.",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass
                return

        # ── 🤬 Bad words — delete for ALL users, warn + potential ban for non-admins ──
        bad_words = group.get("bad_words", [])
        for bw in bad_words:
            if bw in text_lower:
                # Always delete the message
                try:
                    bot.delete_message(message.chat.id, message.message_id)
                except Exception:
                    pass

                # Only warn non-admins / non-owners
                if not user_is_admin and not user_is_owner:
                    try:
                        warnings = db.add_warning(message.from_user.id, message.from_user.first_name)
                        if warnings >= 3:
                            bot.ban_chat_member(message.chat.id, message.from_user.id)
                            db.reset_warnings(message.from_user.id)
                            bot.send_message(
                                message.chat.id,
                                f"⚠️ <b>{message.from_user.first_name}</b> was <b>banned</b> "
                                f"for reaching 3 bad-language warnings.",
                                parse_mode="HTML"
                            )
                        else:
                            bot.send_message(
                                message.chat.id,
                                f"⚠️ <b>{message.from_user.first_name}</b>, watch your language! "
                                f"Warning <b>{warnings}/3</b>",
                                parse_mode="HTML"
                            )
                    except Exception:
                        pass
                return  # stop processing after bad word match

        # ── 🔍 Auto-reply filters ──
        filters = group.get("filters", {})
        for trigger, f_data in filters.items():
            if trigger in text_lower:
                ftype = f_data.get("type")
                try:
                    if ftype == "text":
                        bot.reply_to(message, f_data.get("text"))
                    elif ftype == "photo":
                        bot.send_photo(message.chat.id, f_data.get("file_id"),
                                       caption=f_data.get("caption"),
                                       reply_to_message_id=message.message_id)
                    elif ftype == "sticker":
                        bot.send_sticker(message.chat.id, f_data.get("file_id"),
                                         reply_to_message_id=message.message_id)
                    elif ftype == "gif":
                        bot.send_animation(message.chat.id, f_data.get("file_id"),
                                           reply_to_message_id=message.message_id)
                except Exception:
                    pass
                break
