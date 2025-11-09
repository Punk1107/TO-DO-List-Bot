# rate_limiter.py - Rate limiting and security utilities

# ==========================
# Imports
# ==========================
import os
import time
import json
import asyncio
import logging
from collections import defaultdict, deque
from datetime import datetime
from functools import wraps
from typing import Optional
from logging.handlers import RotatingFileHandler

import discord
from discord.ext import commands

# ==========================
# Constants / Config / Defaults
# ==========================
PERSIST_PATH_DEFAULT = os.path.join(os.path.dirname(__file__), "rate_limiter_state.json")

# ==========================
# Rate Limiter Data Structure
# ==========================
class RateLimiter:
    def __init__(self):
        self.user_commands = defaultdict(deque)
        self.user_tasks_created = defaultdict(deque)
        self.blocked_users = {}
        
        self.commands_per_minute = 30
        self.tasks_per_hour = 100
        self.block_duration = 300  # 5 minutes
        
        self.performance_stats = {
            'total_requests': 0,
            'blocked_requests': 0,
            'cleanup_runs': 0
        }

    # ==========================
    # Rate Limiting Checks
    # ==========================
    def is_rate_limited(self, user_id, command_type="command"):
        now = time.time()
        user_id = str(user_id)
        self.performance_stats['total_requests'] += 1

        if user_id in self.blocked_users:
            if now < self.blocked_users[user_id]:
                self.performance_stats['blocked_requests'] += 1
                return True
            else:
                del self.blocked_users[user_id]

        if command_type == "command":
            queue = self.user_commands[user_id]
            while queue and queue[0] < now - 60:
                queue.popleft()
            if len(queue) >= self.commands_per_minute:
                self.blocked_users[user_id] = now + self.block_duration
                logging.warning(f"User {user_id} rate limited for commands - {len(queue)} commands in last minute")
                self.performance_stats['blocked_requests'] += 1
                return True
            queue.append(now)

        elif command_type == "task":
            queue = self.user_tasks_created[user_id]
            while queue and queue[0] < now - 3600:
                queue.popleft()
            if len(queue) >= self.tasks_per_hour:
                self.blocked_users[user_id] = now + self.block_duration
                logging.warning(f"User {user_id} rate limited for task creation - {len(queue)} tasks in last hour")
                self.performance_stats['blocked_requests'] += 1
                return True
            queue.append(now)

        return False

    def get_remaining_time(self, user_id):
        user_id = str(user_id)
        if user_id in self.blocked_users:
            remaining = self.blocked_users[user_id] - time.time()
            return max(0, remaining)
        return 0

    def cleanup_old_entries(self):
        now = time.time()
        entries_cleaned = 0

        for uid in list(self.user_commands.keys()):
            q = self.user_commands[uid]
            while q and q[0] < now - 60:
                q.popleft()
                entries_cleaned += 1
            if not q:
                del self.user_commands[uid]

        for uid in list(self.user_tasks_created.keys()):
            q = self.user_tasks_created[uid]
            while q and q[0] < now - 3600:
                q.popleft()
                entries_cleaned += 1
            if not q:
                del self.user_tasks_created[uid]

        for uid in list(self.blocked_users.keys()):
            if now >= self.blocked_users[uid]:
                del self.blocked_users[uid]

        self.performance_stats['cleanup_runs'] += 1
        if entries_cleaned > 0:
            logging.info(f"Rate limiter cleanup: {entries_cleaned} entries cleaned")

    def get_stats(self):
        return {
            **self.performance_stats,
            'active_users': len(self.user_commands) + len(self.user_tasks_created),
            'blocked_users': len(self.blocked_users),
            'memory_usage': {
                'command_queues': sum(len(q) for q in self.user_commands.values()),
                'task_queues': sum(len(q) for q in self.user_tasks_created.values())
            }
        }

# ==========================
# Global RateLimiter Instance
# ==========================
rate_limiter = RateLimiter()

# ==========================
# Rate Limit Decorator
# ==========================
def rate_limit(command_type="command"):
    def decorator(func):
        @wraps(func)
        async def wrapper(interaction, *args, **kwargs):
            user_id = str(interaction.user.id)
            try:
                if rate_limiter.is_rate_limited(user_id, command_type):
                    remaining = rate_limiter.get_remaining_time(user_id)
                    if remaining > 0:
                        minutes = int(remaining // 60)
                        seconds = int(remaining % 60)
                        time_str = f"{minutes}m {seconds}s" if minutes else f"{seconds}s"
                        embed = discord.Embed(
                            title="⚠️ Rate Limited",
                            description=f"คุณใช้คำสั่งเร็วเกินไป กรุณารอ **{time_str}**",
                            color=discord.Color.red(),
                            timestamp=datetime.now()
                        )
                        embed.add_field(name="💡 เคล็ดลับ", value="ใช้คำสั่งอย่างสมเหตุสมผลเพื่อประสิทธิภาพที่ดีที่สุด", inline=False)
                        embed.set_footer(text="Rate Limiter v2.0")
                        await interaction.response.send_message(embed=embed, ephemeral=True)
                        return
                return await func(interaction, *args, **kwargs)
            except Exception as e:
                logging.error(f"Error in rate limit wrapper for user {user_id}: {e}")
                return await func(interaction, *args, **kwargs)
        return wrapper
    return decorator

# ==========================
# Security Validator
# ==========================
class SecurityValidator:
    DANGEROUS_PATTERNS = [
        '<script', 'javascript:', 'data:', 'vbscript:', 'onload=', 'onerror=',
        'eval(', 'setTimeout(', 'setInterval(', 'Function(', 'constructor',
        'prototype', '__proto__', 'innerHTML', 'outerHTML'
    ]

    @staticmethod
    def validate_task_name(task_name):
        if not task_name or len(task_name.strip()) == 0:
            return False, "ชื่องานไม่สามารถเป็นค่าว่างได้"
        task_name = task_name.strip()
        if len(task_name) > 200:
            return False, "ชื่องานยาวเกินไป (สูงสุด 200 ตัวอักษร)"
        t_lower = task_name.lower()
        for p in SecurityValidator.DANGEROUS_PATTERNS:
            if p in t_lower:
                logging.warning(f"Blocked potentially malicious task name: {task_name[:50]}...")
                return False, "ชื่องานมีเนื้อหาที่ไม่อนุญาต"
        special_count = sum(1 for c in task_name if not c.isalnum() and not c.isspace() and c not in '.,!?-_()[]{}')
        if special_count > len(task_name) * 0.3:
            return False, "ชื่องานมีอักขระพิเศษมากเกินไป"
        return True, ""

    @staticmethod
    def validate_category_name(category_name):
        if not category_name or len(category_name.strip()) == 0:
            return False, "ชื่อหมวดหมู่ไม่สามารถเป็นค่าว่างได้"
        category_name = category_name.strip()
        if len(category_name) > 50:
            return False, "ชื่อหมวดหมู่ยาวเกินไป (สูงสุด 50 ตัวอักษร)"
        reserved = ['admin', 'system', 'bot', 'null', 'undefined', 'default']
        if category_name.lower() in reserved:
            return False, "ชื่อหมวดหมู่นี้ถูกสงวนไว้"
        return True, ""

    @staticmethod
    def validate_color(color):
        if not color:
            return False, "สีไม่สามารถเป็นค่าว่างได้"
        color = color.strip()
        if not color.startswith('#') or len(color) != 7:
            return False, "รูปแบบสีไม่ถูกต้อง ใช้รูปแบบ #RRGGBB"
        try:
            hex_value = int(color[1:], 16)
            if hex_value == 0x000000:
                logging.info("User selected pure black color")
            elif hex_value == 0xFFFFFF:
                logging.info("User selected pure white color")
            return True, ""
        except ValueError:
            return False, "รูปแบบสีไม่ถูกต้อง"

    @staticmethod
    def validate_tags(tags):
        if not tags:
            return True, ""
        tags = tags.strip()
        if len(tags) > 200:
            return False, "แท็กยาวเกินไป (สูงสุด 200 ตัวอักษร)"
        tag_list = [t.strip() for t in tags.split(',') if t.strip()]
        for t in tag_list:
            if len(t) > 30:
                return False, f"แท็ก '{t}' ยาวเกินไป (สูงสุด 30 ตัวอักษร)"
            if any(p in t.lower() for p in SecurityValidator.DANGEROUS_PATTERNS):
                return False, f"แท็ก '{t}' มีเนื้อหาที่ไม่อนุญาต"
        if len(tag_list) > 10:
            return False, "มีแท็กมากเกินไป (สูงสุด 10 แท็ก)"
        return True, ""

    @staticmethod
    def sanitize_input(text):
        if not text:
            return ""
        for c in ['<', '>', '"', "'", '&', '\x00', '\r', '\n\n\n']:
            text = text.replace(c, '')
        return ' '.join(text.split()).strip()

# ==========================
# Audit Logger
# ==========================
class AuditLogger:
    def __init__(self):
        self.audit_logger = logging.getLogger('audit')
        handler = RotatingFileHandler('audit.log', maxBytes=10*1024*1024, backupCount=5)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.audit_logger.addHandler(handler)
        self.audit_logger.setLevel(logging.INFO)

        self.log_count = 0
        self.last_cleanup = time.time()

    def log_action(self, user_id, action, details=None):
        try:
            message = f"User {user_id} - {action}"
            if details:
                message += f" - {details}"
            self.audit_logger.info(message)
            self.log_count += 1
            if time.time() - self.last_cleanup > 3600:
                self._cleanup_old_logs()
        except Exception as e:
            logging.error(f"Error in audit logging: {e}")

    def log_security_event(self, user_id, event, details=None):
        try:
            message = f"SECURITY - User {user_id} - {event}"
            if details:
                message += f" - {details}"
            self.audit_logger.warning(message)
            logging.warning(f"Security event: {message}")
        except Exception as e:
            logging.error(f"Error in security event logging: {e}")

    def _cleanup_old_logs(self):
        try:
            self.last_cleanup = time.time()
            logging.info(f"Audit log cleanup completed. Total logs: {self.log_count}")
        except Exception as e:
            logging.error(f"Error in log cleanup: {e}")

# ==========================
# Global Audit Logger
# ==========================
audit_logger = AuditLogger()

# ==========================
# Persistence Utilities
# ==========================
_state_lock = asyncio.Lock()

def _write_json(path, data):
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def _read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

async def _save_state(path: str = PERSIST_PATH_DEFAULT):
    async with _state_lock:
        try:
            data = {
                "user_commands": {uid: list(q) for uid, q in rate_limiter.user_commands.items()},
                "user_tasks_created": {uid: list(q) for uid, q in rate_limiter.user_tasks_created.items()},
                "blocked_users": rate_limiter.blocked_users,
                "performance_stats": rate_limiter.performance_stats,
                "config": {
                    "commands_per_minute": rate_limiter.commands_per_minute,
                    "tasks_per_hour": rate_limiter.tasks_per_hour,
                    "block_duration": rate_limiter.block_duration
                }
            }
            await asyncio.to_thread(_write_json, path, data)
            logging.info("Rate limiter state saved.")
        except Exception as e:
            logging.error(f"Failed to save rate limiter state: {e}")

async def _load_state(path: str = PERSIST_PATH_DEFAULT):
    async with _state_lock:
        if not os.path.exists(path):
            logging.info("No rate limiter state file to load.")
            return
        try:
            data = await asyncio.to_thread(_read_json, path)
            rate_limiter.user_commands.clear()
            rate_limiter.user_tasks_created.clear()
            for uid, lst in data.get("user_commands", {}).items():
                rate_limiter.user_commands[uid] = deque(lst)
            for uid, lst in data.get("user_tasks_created", {}).items():
                rate_limiter.user_tasks_created[uid] = deque(lst)
            rate_limiter.blocked_users = {k: v for k, v in data.get("blocked_users", {}).items()}
            rate_limiter.performance_stats.update(data.get("performance_stats", {}))
            cfg = data.get("config", {})
            rate_limiter.commands_per_minute = cfg.get("commands_per_minute", rate_limiter.commands_per_minute)
            rate_limiter.tasks_per_hour = cfg.get("tasks_per_hour", rate_limiter.tasks_per_hour)
            rate_limiter.block_duration = cfg.get("block_duration", rate_limiter.block_duration)
            logging.info("Rate limiter state loaded.")
        except Exception as e:
            logging.error(f"Failed to load rate limiter state: {e}")

# ==========================
# Background Cleanup Task
# ==========================
async def cleanup_rate_limiter():
    while True:
        try:
            start_time = time.time()
            rate_limiter.cleanup_old_entries()
            if rate_limiter.performance_stats['cleanup_runs'] % 12 == 0:
                logging.info(f"Rate limiter stats: {rate_limiter.get_stats()}")
            cleanup_time = time.time() - start_time
            await asyncio.sleep(180 if cleanup_time > 0.1 else 300)
        except Exception as e:
            logging.error(f"Error in rate limiter cleanup: {e}")

# ==========================
# Admin / Discord Cog
# ==========================
class RateLimiterAdmin(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.is_owner()
    @commands.command(name="ratelimiter_stats")
    async def ratelimiter_stats(self, ctx):
        stats = rate_limiter.get_stats()
        embed = discord.Embed(title="Rate Limiter Stats", color=discord.Color.blue())
        embed.add_field(name="Total Requests", value=str(stats.get("total_requests", 0)))
        embed.add_field(name="Blocked Requests", value=str(stats.get("blocked_requests", 0)))
        embed.add_field(name="Active Users", value=str(stats.get("active_users", 0)))
        embed.add_field(name="Blocked Users", value=str(stats.get("blocked_users", 0)))
        embed.add_field(name="Command Queues", value=str(stats.get("memory_usage", {}).get("command_queues", 0)))
        embed.add_field(name="Task Queues", value=str(stats.get("memory_usage", {}).get("task_queues", 0)))
        await ctx.send(embed=embed)

    @commands.is_owner()
    @commands.command(name="ratelimiter_unblock")
    async def ratelimiter_unblock(self, ctx, user_id: str):
        if user_id in rate_limiter.blocked_users:
            del rate_limiter.blocked_users[user_id]
            await ctx.send(f"Unblocked user {user_id}.")
            audit_logger.log_action(ctx.author.id, "unblock_user", details=f"user={user_id}")
        else:
            await ctx.send("User is not blocked.")

    @commands.is_owner()
    @commands.command(name="ratelimiter_set")
    async def ratelimiter_set(self, ctx, limit_type: str, value: int):
        if limit_type not in ("commands_per_minute", "tasks_per_hour", "block_duration"):
            await ctx.send("Invalid limit type.")
            return
        setattr(rate_limiter, limit_type, max(0, value))
        await ctx.send(f"Set {limit_type} = {value}")
        audit_logger.log_action(ctx.author.id, "set_rate_limit", details=f"{limit_type}={value}")

# ==========================
# Setup / Initialization
# ==========================
def start_rate_limiter_background(bot, persist_path: Optional[str] = None):
    if persist_path is None:
        persist_path = PERSIST_PATH_DEFAULT
    if getattr(bot, "rate_limiter_task", None) is None or bot.rate_limiter_task.done():
        bot.loop.create_task(_load_state(persist_path))
        bot.rate_limiter_task = bot.loop.create_task(cleanup_rate_limiter())
        bot.rate_limiter_persist_path = persist_path
        logging.info("Rate limiter background task started and state load scheduled.")

def stop_rate_limiter_background(bot):
    async def _stop():
        try:
            task = getattr(bot, "rate_limiter_task", None)
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            path = getattr(bot, "rate_limiter_persist_path", PERSIST_PATH_DEFAULT)
            await _save_state(path)
            logging.info("Rate limiter background stopped and state saved.")
        except Exception as e:
            logging.error(f"Error stopping rate limiter background: {e}")
    return bot.loop.create_task(_stop())

def setup(bot: commands.Bot):
    bot.add_cog(RateLimiterAdmin(bot))
    start_rate_limiter_background(bot)
    logging.info("RateLimiterAdmin cog loaded and background started.")
