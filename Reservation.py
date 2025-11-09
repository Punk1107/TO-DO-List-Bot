# main.py - To-Do List Discord Bot Production Ready (Part 1)
import discord
from discord.ext import commands, tasks
from discord.ui import View, Button, Select, Modal, TextInput
from datetime import datetime, timedelta
import pytz, sqlite3, os, logging
from dotenv import load_dotenv

# ---------------- Load .env ----------------
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
DB_FILE = os.getenv("DATABASE_PATH", "tasks.db")
DEFAULT_TZ = os.getenv("DEFAULT_TIMEZONE", "UTC")
REMINDER_INTERVAL = int(os.getenv("REMINDER_INTERVAL", 60))
RECURRING_INTERVAL = int(os.getenv("RECURRING_CHECK_INTERVAL", 60))

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO)

# ---------------- Intents & Bot ----------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

# ---------------- Database ----------------
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
c = conn.cursor()
c.execute("""CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    timezone TEXT,
    channel_id INTEGER,
    role TEXT DEFAULT 'user'
)""")
c.execute("""CREATE TABLE IF NOT EXISTS tasks (
    task_id INTEGER PRIMARY KEY AUTOINCREMENT,
    task TEXT,
    deadline TEXT,
    priority INTEGER DEFAULT 0,
    status TEXT DEFAULT 'Pending',
    recurring TEXT DEFAULT NULL,
    owner_id TEXT,
    message_id INTEGER
)""")
c.execute("""CREATE TABLE IF NOT EXISTS task_assignments (
    task_id INTEGER,
    user_id TEXT,
    PRIMARY KEY (task_id, user_id)
)""")
conn.commit()

# ---------------- Helper Functions ----------------
def get_timezone(user_id):
    c.execute("SELECT timezone FROM users WHERE user_id=?", (user_id,))
    row = c.fetchone()
    return row[0] if row else DEFAULT_TZ

def get_channel(user_id):
    c.execute("SELECT channel_id FROM users WHERE user_id=?", (user_id,))
    row = c.fetchone()
    return row[0] if row else None

def get_role(user_id):
    c.execute("SELECT role FROM users WHERE user_id=?", (user_id,))
    row = c.fetchone()
    return row[0] if row else "user"

def save_user(user_id, tz=None, channel_id=None, role=None):
    c.execute("INSERT OR IGNORE INTO users (user_id, timezone, channel_id, role) VALUES (?, ?, ?, ?)",
              (user_id, tz if tz else DEFAULT_TZ, channel_id, role if role else "user"))
    if tz:
        c.execute("UPDATE users SET timezone=? WHERE user_id=?", (tz, user_id))
    if channel_id:
        c.execute("UPDATE users SET channel_id=? WHERE user_id=?", (channel_id, user_id))
    if role:
        c.execute("UPDATE users SET role=? WHERE user_id=?", (role, user_id))
    conn.commit()

def calculate_next_deadline(last_deadline, recurring):
    if recurring.lower() == "daily":
        return last_deadline + timedelta(days=1)
    elif recurring.lower() == "weekly":
        return last_deadline + timedelta(weeks=1)
    elif recurring.lower() == "monthly":
        month = last_deadline.month + 1
        year = last_deadline.year
        if month > 12:
            month = 1
            year += 1
        try:
            return last_deadline.replace(year=year, month=month)
        except:
            return last_deadline + timedelta(days=30)
    return None

async def assign_task(task_id, user_ids):
    for uid in user_ids:
        c.execute("INSERT OR IGNORE INTO task_assignments (task_id, user_id) VALUES (?, ?)", (task_id, uid))
    conn.commit()

# ---------------- Embed + Buttons ----------------
def create_task_buttons(task_id, user_id, role):
    class TaskButtons(View):
        def __init__(self):
            super().__init__(timeout=None)
            self.add_item(Button(label="✅ Mark as Done", style=discord.ButtonStyle.green, custom_id=f"done_{task_id}_{user_id}"))
            self.add_item(Button(label="🗑 Delete Task", style=discord.ButtonStyle.red, custom_id=f"delete_{task_id}_{user_id}"))
            self.add_item(Button(label="✏️ Edit Task", style=discord.ButtonStyle.blurple, custom_id=f"edit_{task_id}_{user_id}"))
    return TaskButtons()

async def update_task_embed(task_id):
    c.execute("SELECT task_id, task, deadline, priority, status, recurring, owner_id, message_id FROM tasks WHERE task_id=?", (task_id,))
    row = c.fetchone()
    if not row:
        return
    task_id, task_name, deadline, priority, status, recurring, owner_id, message_id = row
    tz = pytz.timezone(get_timezone(owner_id))
    try:
        deadline_dt = datetime.fromisoformat(deadline).astimezone(tz)
    except:
        deadline_dt = datetime.now(tz)
    channel_id = get_channel(owner_id)
    channel = bot.get_channel(channel_id) if channel_id else None
    if not channel or not message_id:
        return
    try:
        msg = await channel.fetch_message(message_id)
        color = discord.Color.green() if status=='Pending' else discord.Color.greyple()
        if status=='Pending' and deadline_dt < datetime.now(tz):
            color = discord.Color.red()
        embed = discord.Embed(
            title=f"{'✅' if status=='Completed' else '📝'} Task Update",
            description=task_name,
            color=color
        )
        embed.add_field(name="Deadline", value=deadline_dt.strftime("%Y-%m-%d %H:%M %Z"))
        embed.add_field(name="Priority", value=str(priority))
        embed.add_field(name="Status", value=status)
        embed.add_field(name="Recurring", value=recurring if recurring else "No")
        view = None
        if status=='Pending':
            view = create_task_buttons(task_id, owner_id, get_role(owner_id))
        await msg.edit(embed=embed, view=view)
    except Exception as e:
        logging.warning(f"Failed to update embed for task {task_id}: {e}")

async def complete_task(ctx_or_interaction, task_id, user_id):
    c.execute("SELECT task, recurring FROM tasks WHERE task_id=? AND owner_id=?", (task_id, user_id))
    row = c.fetchone()
    if not row:
        msg = "⚠️ ไม่เจองานนี้แล้ว"
        if isinstance(ctx_or_interaction, discord.Interaction):
            await ctx_or_interaction.response.send_message(msg, ephemeral=True)
        else:
            await ctx_or_interaction.send(msg)
        return
    task_name, recurring = row
    c.execute("UPDATE tasks SET status='Completed' WHERE task_id=?", (task_id,))
    conn.commit()
    await update_task_embed(task_id)
    msg = f"✅ <@{user_id}> ส่งงาน `{task_name}` แล้ว"
    if isinstance(ctx_or_interaction, discord.Interaction):
        await ctx_or_interaction.response.send_message(msg)
    else:
        await ctx_or_interaction.send(msg)

# ---------------- Bot Events ----------------
@bot.event
async def on_ready():
    logging.info(f"✅ Logged in as {bot.user}")
    try:
        synced = await tree.sync()
        logging.info(f"Slash commands synced: {len(synced)}")
    except Exception as e:
        logging.warning(f"Error syncing slash commands: {e}")
    reminder_loop.start()
    recurring_task_loop.start()

@bot.event
async def on_interaction(interaction: discord.Interaction):
    if interaction.data:
        custom_id = interaction.data.get("custom_id", "")
        parts = custom_id.split("_")
        if len(parts) >= 3:
            action, task_id, user_id = parts[0], int(parts[1]), parts[2]
            role = get_role(str(interaction.user.id))
            if action == "done":
                if str(interaction.user.id) != user_id and role != "admin":
                    await interaction.response.send_message("⚠️ คุณไม่สามารถทำเครื่องหมายงานคนอื่นได้", ephemeral=True)
                    return
                await complete_task(interaction, task_id, user_id)
            elif action == "delete":
                if str(interaction.user.id) != user_id and role != "admin":
                    await interaction.response.send_message("⚠️ คุณไม่สามารถลบงานของคนอื่นได้", ephemeral=True)
                    return
                c.execute("DELETE FROM tasks WHERE task_id=?", (task_id,))
                c.execute("DELETE FROM task_assignments WHERE task_id=?", (task_id,))
                conn.commit()
                await interaction.response.send_message("🗑 Task ถูกลบเรียบร้อยแล้ว", ephemeral=True)
            elif action == "edit":
                if str(interaction.user.id) != user_id and role != "admin":
                    await interaction.response.send_message("⚠️ คุณไม่สามารถแก้ไขงานคนอื่นได้", ephemeral=True)
                    return
                class EditModal(Modal, title=f"แก้ไข Task ID {task_id}"):
                    new_task = TextInput(label="Task Name", default="", required=False)
                    new_deadline = TextInput(label="Deadline YYYY-MM-DD HH:MM", default="", required=False)
                    new_priority = TextInput(label="Priority", default="", required=False)
                    new_recurring = TextInput(label="Recurring (daily/weekly/monthly)", default="", required=False)
                    async def on_submit(self2, modal_interaction):
                        updates = []
                        if self2.new_task.value:
                            c.execute("UPDATE tasks SET task=? WHERE task_id=?", (self2.new_task.value, task_id))
                            updates.append("Task name")
                        if self2.new_deadline.value:
                            try:
                                tz = pytz.timezone(get_timezone(user_id))
                                dt = datetime.strptime(self2.new_deadline.value, "%Y-%m-%d %H:%M")
                                dt = tz.localize(dt)
                                c.execute("UPDATE tasks SET deadline=? WHERE task_id=?", (dt.isoformat(), task_id))
                                updates.append("Deadline")
                            except:
                                await modal_interaction.response.send_message("⚠️ รูปแบบวัน/เวลาไม่ถูกต้อง", ephemeral=True)
                                return
                        if self2.new_priority.value.isdigit():
                            c.execute("UPDATE tasks SET priority=? WHERE task_id=?", (int(self2.new_priority.value), task_id))
                            updates.append("Priority")
                        if self2.new_recurring.value:
                            c.execute("UPDATE tasks SET recurring=? WHERE task_id=?", (self2.new_recurring.value, task_id))
                            updates.append("Recurring")
                        conn.commit()
                        await update_task_embed(task_id)
                        await modal_interaction.response.send_message(f"✏️ Updated: {', '.join(updates)}", ephemeral=True)
                await interaction.response.send_modal(EditModal())

# ---------------- Slash Commands Full ----------------
# /help, /settimezone, /setchannel, /addtask, /listtasks, /taskstats

@tree.command(name="help", description="ดูคำสั่งทั้งหมดของ To-Do Bot")
async def help_command(interaction: discord.Interaction):
    embed = discord.Embed(title="📖 To-Do Bot Commands", color=discord.Color.blurple())
    embed.add_field(name="/settimezone <tz>", value="ตั้ง Timezone ของคุณ", inline=False)
    embed.add_field(name="/setchannel", value="ตั้งช่องแจ้งเตือนสำหรับ reminder", inline=False)
    embed.add_field(name="/addtask <deadline> <task> [priority] [recurring] [users]", value="เพิ่มงานใหม่", inline=False)
    embed.add_field(name="/listtasks", value="ดูงานทั้งหมดของคุณแบบ filterable + interactive", inline=False)
    embed.add_field(name="/taskstats", value="ดูสถิติ Completed/Pending/Overdue ของคุณ", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=False)

@tree.command(name="settimezone", description="ตั้ง timezone ของคุณ")
async def settimezone(interaction: discord.Interaction, tz: str):
    try:
        pytz.timezone(tz)
    except pytz.UnknownTimeZoneError:
        await interaction.response.send_message("⚠️ Timezone ไม่ถูกต้อง", ephemeral=True)
        return
    save_user(str(interaction.user.id), tz=tz)
    await interaction.response.send_message(f"🌏 ตั้ง timezone เป็น `{tz}` แล้ว", ephemeral=True)

@tree.command(name="setchannel", description="ตั้งช่องแจ้งเตือนสำหรับ reminder")
async def setchannel(interaction: discord.Interaction):
    save_user(str(interaction.user.id), channel_id=interaction.channel.id)
    await interaction.response.send_message(f"📢 ตั้งช่องแจ้งเตือนเป็น {interaction.channel.mention}", ephemeral=True)

@tree.command(name="addtask", description="เพิ่มงานใหม่")
async def addtask(interaction: discord.Interaction, deadline: str, task: str, priority: int = 0, recurring: str = None, users: str = ""):
    user_id = str(interaction.user.id)
    tz = pytz.timezone(get_timezone(user_id))
    try:
        try:
            deadline_dt = datetime.strptime(deadline, "%Y-%m-%d %H:%M")
        except ValueError:
            deadline_dt = datetime.strptime(deadline, "%Y-%m-%d")
    except ValueError:
        await interaction.response.send_message("⚠️ รูปแบบวัน/เวลาไม่ถูกต้อง (YYYY-MM-DD หรือ YYYY-MM-DD HH:MM)", ephemeral=True)
        return
    deadline_dt = tz.localize(deadline_dt)
    c.execute("INSERT INTO tasks (task, deadline, priority, recurring, owner_id) VALUES (?, ?, ?, ?, ?)",
              (task, deadline_dt.isoformat(), priority, recurring, user_id))
    conn.commit()
    task_id = c.lastrowid
    user_ids = [u.strip() for u in users.split(",") if u.strip()]
    if user_ids:
        await assign_task(task_id, user_ids)
    embed = discord.Embed(title="📝 งานใหม่ถูกเพิ่มแล้ว", description=task, color=discord.Color.green())
    embed.add_field(name="Deadline", value=deadline_dt.strftime("%Y-%m-%d %H:%M %Z"))
    embed.add_field(name="Priority", value=str(priority))
    embed.add_field(name="Status", value="Pending")
    embed.add_field(name="Recurring", value=recurring if recurring else "No")
    embed.set_footer(text=f"เพิ่มโดย {interaction.user.display_name}")
    view = create_task_buttons(task_id, user_id, get_role(user_id))
    channel_id = get_channel(user_id)
    channel = bot.get_channel(channel_id) if channel_id else None
    if channel:
        msg = await channel.send(embed=embed, view=view)
        c.execute("UPDATE tasks SET message_id=? WHERE task_id=?", (msg.id, task_id))
        conn.commit()
    await interaction.response.send_message(f"✅ เพิ่มงาน `{task}` เรียบร้อยแล้ว", ephemeral=True)

# ---------------- Reminder & Recurring Loops ----------------
@tasks.loop(seconds=REMINDER_INTERVAL)
async def reminder_loop():
    now_utc = datetime.now(pytz.UTC)
    c.execute("SELECT user_id, channel_id, timezone FROM users WHERE channel_id IS NOT NULL")
    for user_id, channel_id, tz_name in c.fetchall():
        tz = pytz.timezone(tz_name)
        now_local = now_utc.astimezone(tz)
        c.execute("SELECT task_id, task, deadline, status FROM tasks WHERE owner_id=?", (user_id,))
        for task_id, task_name, deadline, status in c.fetchall():
            if status != "Pending":
                continue
            try:
                deadline_dt = datetime.fromisoformat(deadline).astimezone(tz)
            except:
                deadline_dt = now_local
            if 0 <= (deadline_dt - now_local).total_seconds() <= 3600:
                channel = bot.get_channel(channel_id)
                if channel:
                    embed = discord.Embed(title="⏰ Reminder!", description=f"<@{user_id}> อย่าลืมงาน: `{task_name}`", color=discord.Color.orange())
                    embed.add_field(name="Deadline", value=deadline_dt.strftime('%Y-%m-%d %H:%M %Z'))
                    await channel.send(embed=embed)
                try:
                    user = await bot.fetch_user(int(user_id))
                    await user.send(f"⏰ Reminder! อย่าลืมงาน: `{task_name}` Deadline: {deadline_dt.strftime('%Y-%m-%d %H:%M %Z')}")
                except: 
                    logging.warning(f"Cannot send DM to user {user_id}")

@tasks.loop(seconds=RECURRING_INTERVAL)
async def recurring_task_loop():
    now_utc = datetime.now(pytz.UTC)
    c.execute("SELECT task_id, task, deadline, recurring, owner_id FROM tasks WHERE recurring IS NOT NULL AND status='Completed'")
    for task_id, task_name, deadline, recurring, owner_id in c.fetchall():
        tz = pytz.timezone(get_timezone(owner_id))
        try:
            last_deadline = datetime.fromisoformat(deadline).astimezone(tz)
        except:
            last_deadline = datetime.now(tz)
        next_deadline = calculate_next_deadline(last_deadline, recurring)
        if next_deadline:
            next_deadline_utc = next_deadline.astimezone(pytz.UTC)
            c.execute("INSERT INTO tasks (task, deadline, priority, recurring, owner_id) VALUES (?, ?, ?, ?, ?)",
                      (task_name, next_deadline_utc.isoformat(), 0, recurring, owner_id))
            conn.commit()

# ---------------- Interactive /listtasks ----------------
@tree.command(name="listtasks", description="ดูงานทั้งหมดของคุณแบบ filterable")
async def listtasks(interaction: discord.Interaction):
    user_id = str(interaction.user.id)
    tz = pytz.timezone(get_timezone(user_id))
    c.execute("SELECT task_id, task, deadline, priority, status FROM tasks WHERE owner_id=?", (user_id,))
    tasks_list = c.fetchall()
    if not tasks_list:
        # ส่ง DM ให้ user
        try:
            user = await bot.fetch_user(int(user_id))
            await user.send("คุณยังไม่มีงานค้าง ณ ตอนนี้")
        except:
            pass
        await interaction.response.send_message("คุณยังไม่มีงาน", ephemeral=False)
        return

    def make_embed(filter_status=None):
        embed = discord.Embed(title="📝 งานของคุณ", color=discord.Color.blurple())
        filtered_tasks = []
        now_local = datetime.now(tz)
        for tid, tname, deadline, prio, status in tasks_list:
            try:
                deadline_dt = datetime.fromisoformat(deadline).astimezone(tz)
            except:
                deadline_dt = now_local
            if filter_status == "Overdue":
                if status == "Pending" and deadline_dt < now_local:
                    filtered_tasks.append((tid, tname, deadline_dt, prio, status))
            elif filter_status and status != filter_status:
                continue
            else:
                filtered_tasks.append((tid, tname, deadline_dt, prio, status))
        if not filtered_tasks:
            embed.description = "ไม่พบงานตาม filter"
        else:
            filtered_tasks.sort(key=lambda x: (x[3], x[2]))  # sort by priority then deadline
            for tid, tname, deadline_dt, prio, status in filtered_tasks:
                embed.add_field(name=f"{tname} ({status})", value=f"Deadline: {deadline_dt.strftime('%Y-%m-%d %H:%M %Z')} | Priority: {prio} | ID: {tid}", inline=False)
        return embed

    class TaskFilter(View):
        def __init__(self):
            super().__init__(timeout=None)
            options = [
                discord.SelectOption(label="All", value="all"),
                discord.SelectOption(label="Pending", value="Pending"),
                discord.SelectOption(label="Completed", value="Completed"),
                discord.SelectOption(label="Overdue", value="Overdue"),
            ]
            self.add_item(Select(placeholder="กรองสถานะงาน", options=options, custom_id="filter_select"))

        @discord.ui.select(custom_id="filter_select")
        async def select_callback(self, select, interaction_select):
            filter_val = select.values[0]
            if filter_val == "all":
                embed = make_embed()
            else:
                embed = make_embed(filter_val)
            await interaction_select.response.edit_message(embed=embed, view=self)

    # ส่ง DM summary ทุกงาน
    try:
        user = await bot.fetch_user(int(user_id))
        summary_lines = []
        for tid, tname, deadline, prio, status in tasks_list:
            summary_lines.append(f"- {tname} ({status}), Deadline: {deadline}, Priority: {prio}")
        summary_msg = "📋 รายการงานของคุณ:\n" + "\n".join(summary_lines)
        await user.send(summary_msg)
    except:
        logging.warning(f"Cannot send DM summary to user {user_id}")

    await interaction.response.send_message(embed=make_embed(), view=TaskFilter(), ephemeral=False)

# ---------------- Task Stats ----------------
@tree.command(name="taskstats", description="ดูสถิติ Completed/Pending/Overdue ของคุณ")
async def taskstats(interaction: discord.Interaction):
    user_id = str(interaction.user.id)
    tz = pytz.timezone(get_timezone(user_id))
    c.execute("SELECT status, deadline FROM tasks WHERE owner_id=?", (user_id,))
    rows = c.fetchall()
    now_local = datetime.now(tz)
    pending = sum(1 for s,d in rows if s=="Pending" and datetime.fromisoformat(d).astimezone(tz) >= now_local)
    completed = sum(1 for s,d in rows if s=="Completed")
    overdue = sum(1 for s,d in rows if s=="Pending" and datetime.fromisoformat(d).astimezone(tz) < now_local)
    total = pending + completed + overdue
    def bar(val):
        n = int((val/total)*20) if total else 0
        return "█"*n + "░"*(20-n)
    embed = discord.Embed(title="📊 Task Stats", color=discord.Color.purple())
    embed.add_field(name=f"Pending ({pending})", value=bar(pending), inline=False)
    embed.add_field(name=f"Completed ({completed})", value=bar(completed), inline=False)
    embed.add_field(name=f"Overdue ({overdue})", value=bar(overdue), inline=False)
    await interaction.response.send_message(embed=embed)

# ---------------- Run Bot ----------------
bot.run(TOKEN)