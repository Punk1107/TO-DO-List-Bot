# main.py - To-Do List Discord Bot Production Ready - IMPROVED VERSION
import discord
from discord.ext import commands, tasks
from discord.ui import View, Button, Select, Modal, TextInput
from datetime import datetime, timedelta
import pytz, sqlite3, os, logging, asyncio, csv, io, tempfile
from dotenv import load_dotenv
from rate_limiter import rate_limit, SecurityValidator, audit_logger, cleanup_rate_limiter
import threading
import webserver

# ---------------- Load .env ----------------
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
DB_FILE = os.getenv("DATABASE_PATH", "tasks.db")
DEFAULT_TZ = os.getenv("DEFAULT_TIMEZONE", "UTC")
REMINDER_INTERVAL = int(os.getenv("REMINDER_INTERVAL", 60))
RECURRING_INTERVAL = int(os.getenv("RECURRING_CHECK_INTERVAL", 60))

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)

# ---------------- Intents & Bot ----------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

class DatabaseManager:
    """Enhanced database manager with connection pooling and better error handling"""
    
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = None
        self._lock = asyncio.Lock()
        self.connection_pool = []
        self.max_connections = 10
        self.db_path = db_file
        
    async def connect(self):
        """Connect to database with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.conn = sqlite3.connect(self.db_file, check_same_thread=False, timeout=30)
                self.conn.row_factory = sqlite3.Row
                self.conn.execute("PRAGMA foreign_keys = ON")
                self.conn.execute("PRAGMA journal_mode = WAL")
                self.conn.execute("PRAGMA synchronous = NORMAL")
                self.conn.execute("PRAGMA cache_size = 10000")
                logging.info("Database connected successfully")
                # Ensure directory for DB exists (useful when DATABASE_PATH includes folders)
                db_dir = os.path.dirname(self.db_file)
                if db_dir and not os.path.exists(db_dir):
                    try:
                        os.makedirs(db_dir, exist_ok=True)
                    except Exception as e:
                        logging.warning(f"Could not create DB directory {db_dir}: {e}")

                # Add primary connection to pool
                self.connection_pool.append(self.conn)

                # Pre-warm a small number of pooled connections (don't exhaust resources)
                prewarm = min(3, max(0, self.max_connections - 1))
                for i in range(prewarm):
                    try:
                        pooled = sqlite3.connect(self.db_file, check_same_thread=False, timeout=30)
                        pooled.row_factory = sqlite3.Row
                        pooled.execute("PRAGMA foreign_keys = ON")
                        pooled.execute("PRAGMA journal_mode = WAL")
                        pooled.execute("PRAGMA synchronous = NORMAL")
                        pooled.execute("PRAGMA cache_size = 10000")
                        self.connection_pool.append(pooled)
                    except Exception as e:
                        logging.warning(f"Failed to create pooled connection #{i+1}: {e}")

                # Mark as connected for callers that may check this
                self.connected = True
                logging.info(f"Connection pool initialized with {len(self.connection_pool)} connection(s)")
                return
            except Exception as e:
                logging.error(f"Database connection attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(1)
    
    def init_db(self):
        try:
            self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
            self.conn.execute("PRAGMA foreign_keys = ON")
            c = self.conn.cursor()
            
            # Create users table first (no dependencies)
            c.execute("""CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                timezone TEXT DEFAULT 'UTC',
                channel_id INTEGER,
                role TEXT DEFAULT 'user',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
            
            try:
                # Check if role column exists in users table
                c.execute("PRAGMA table_info(users)")
                columns = [column[1] for column in c.fetchall()]
                
                if 'role' not in columns:
                    c.execute("ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'user'")
                    logging.info("Added role column to users table")
                    
                if 'created_at' not in columns:
                    c.execute("ALTER TABLE users ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                    logging.info("Added created_at column to users table")
                    
            except sqlite3.OperationalError as e:
                logging.warning(f"Column addition warning: {e}")
            
            # Insert default system user if not exists
            c.execute("INSERT OR IGNORE INTO users (user_id, timezone, role) VALUES (?, ?, ?)",
                     ('system', 'UTC', 'admin'))
            
            # Create categories table second (depends on users)
            c.execute("""CREATE TABLE IF NOT EXISTS categories (
                category_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                color TEXT DEFAULT '#3498db',
                emoji TEXT DEFAULT '📝',
                owner_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (owner_id) REFERENCES users (user_id)
            )""")
            
            try:
                c.execute("PRAGMA table_info(categories)")
                cat_columns = [column[1] for column in c.fetchall()]
                
                if 'color' not in cat_columns:
                    c.execute("ALTER TABLE categories ADD COLUMN color TEXT DEFAULT '#3498db'")
                    logging.info("Added color column to categories table")
                    
                if 'emoji' not in cat_columns:
                    c.execute("ALTER TABLE categories ADD COLUMN emoji TEXT DEFAULT '📝'")
                    logging.info("Added emoji column to categories table")
                    
                if 'created_at' not in cat_columns:
                    c.execute("ALTER TABLE categories ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                    logging.info("Added created_at column to categories table")
                    
            except sqlite3.OperationalError as e:
                logging.warning(f"Categories column addition warning: {e}")
            
            # Create tasks table last (depends on users and categories)
            c.execute("""CREATE TABLE IF NOT EXISTS tasks (
                task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                task TEXT NOT NULL,
                deadline TEXT NOT NULL,
                priority INTEGER DEFAULT 0,
                status TEXT DEFAULT 'Pending' CHECK(status IN ('Pending', 'Completed', 'Cancelled')),
                recurring TEXT CHECK(recurring IN ('daily', 'weekly', 'monthly') OR recurring IS NULL),
                category_id INTEGER,
                tags TEXT,
                description TEXT,
                parent_task_id INTEGER,
                owner_id TEXT NOT NULL,
                message_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_reminder TIMESTAMP,
                FOREIGN KEY (owner_id) REFERENCES users (user_id),
                FOREIGN KEY (category_id) REFERENCES categories (category_id),
                FOREIGN KEY (parent_task_id) REFERENCES tasks (task_id) ON DELETE CASCADE
            )""")
            
            try:
                c.execute("PRAGMA table_info(tasks)")
                task_columns = [column[1] for column in c.fetchall()]
                
                missing_task_columns = {
                    'priority': 'INTEGER DEFAULT 0',
                    'recurring': 'TEXT CHECK(recurring IN (\'daily\', \'weekly\', \'monthly\') OR recurring IS NULL)',
                    'category_id': 'INTEGER',
                    'tags': 'TEXT',
                    'description': 'TEXT',
                    'parent_task_id': 'INTEGER',
                    'message_id': 'INTEGER',
                    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'last_reminder': 'TIMESTAMP'
                }
                
                for col_name, col_def in missing_task_columns.items():
                    if col_name not in task_columns:
                        c.execute(f"ALTER TABLE tasks ADD COLUMN {col_name} {col_def}")
                        logging.info(f"Added {col_name} column to tasks table")
                        
            except sqlite3.OperationalError as e:
                logging.warning(f"Tasks column addition warning: {e}")
            
            # Insert default user
            c.execute("INSERT OR IGNORE INTO users (user_id, role) VALUES ('default', 'system')")
            
            # Check and add default categories
            c.execute("SELECT COUNT(*) FROM categories")
            if c.fetchone()[0] == 0:
                default_categories = [
                    ('งานทั่วไป', '#3498db', '📝', 'system'),
                    ('งานด่วน', '#e74c3c', '🚨', 'system'),
                    ('งานส่วนตัว', '#9b59b6', '👤', 'system'),
                    ('งานบ้าน', '#f39c12', '🏠', 'system'),
                    ('การเรียน', '#2ecc71', '📚', 'system')
                ]
                
                for name, color, emoji, owner_id in default_categories:
                    c.execute("INSERT OR IGNORE INTO categories (name, color, emoji, owner_id) VALUES (?, ?, ?, ?)",
                         (name, color, emoji, owner_id))
                
                logging.info("Added default categories")
            
            c.execute("""CREATE TABLE IF NOT EXISTS task_assignments (
                task_id INTEGER,
                user_id TEXT,
                assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (task_id, user_id),
                FOREIGN KEY (task_id) REFERENCES tasks (task_id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )""")
            
            # Create indexes for better performance
            c.execute("CREATE INDEX IF NOT EXISTS idx_tasks_owner ON tasks(owner_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_tasks_deadline ON tasks(deadline)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_tasks_category ON tasks(category_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_tasks_parent ON tasks(parent_task_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_categories_owner ON categories(owner_id)")
            
            self.conn.commit()
            logging.info("Database initialized successfully")
            
        except Exception as e:
            logging.error(f"Database initialization failed: {e}")
            raise

    def execute(self, query, params=None):
        """Execute query with better error handling"""
        try:
            cursor = self.conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.conn.commit()
            return cursor
        except sqlite3.Error as e:
            logging.error(f"Database execute error: {e}, Query: {query}")
            raise

    def fetchone(self, query, params=None):
        """Fetch one result with error handling"""
        try:
            cursor = self.conn.execute(query, params or ())
            return cursor.fetchone()
        except sqlite3.Error as e:
            logging.error(f"Database fetchone error: {e}, Query: {query}")
            return None

    def fetchall(self, query, params=None):
        """Fetch all results with error handling"""
        try:
            cursor = self.conn.execute(query, params or ())
            return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Database fetchall error: {e}, Query: {query}")
            return []
    
    async def backup_database(self):
        """Create database backup"""
        try:
            backup_path = f"{self.db_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            backup_conn = sqlite3.connect(backup_path)
            self.conn.backup(backup_conn)
            backup_conn.close()
            logging.info(f"Database backup created: {backup_path}")
            return backup_path
        except Exception as e:
            logging.error(f"Database backup failed: {e}")
            return None

# Initialize database
db = DatabaseManager(DB_FILE)

# ---------------- Helper Functions ----------------
def get_timezone(user_id):
    row = db.fetchone("SELECT timezone FROM users WHERE user_id=?", (user_id,))
    return row[0] if row else DEFAULT_TZ

def get_channel(user_id):
    row = db.fetchone("SELECT channel_id FROM users WHERE user_id=?", (user_id,))
    return row[0] if row else None

def get_role(user_id):
    row = db.fetchone("SELECT role FROM users WHERE user_id=?", (user_id,))
    return row[0] if row else "user"

def save_user(user_id, tz=None, channel_id=None, role=None):
    try:
        db.execute("INSERT OR IGNORE INTO users (user_id, timezone, channel_id, role) VALUES (?, ?, ?, ?)",
                  (user_id, tz or DEFAULT_TZ, channel_id, role, ))
        
        if tz:
            db.execute("UPDATE users SET timezone=? WHERE user_id=?", (tz, user_id))
        if channel_id:
            db.execute("UPDATE users SET channel_id=? WHERE user_id=?", (channel_id, user_id))
        if role:
            db.execute("UPDATE users SET role=? WHERE user_id=?", (role, user_id))
        
        logging.info(f"User {user_id} saved/updated successfully")
    except Exception as e:
        logging.error(f"Failed to save user {user_id}: {e}")

def calculate_next_deadline(last_deadline, recurring):
    try:
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
            except ValueError:
                return last_deadline + timedelta(days=30)
    except Exception as e:
        logging.error(f"Error calculating next deadline: {e}")
    return None

async def assign_task(task_id, user_ids):
    try:
        for uid in user_ids:
            db.execute("INSERT OR IGNORE INTO task_assignments (task_id, user_id) VALUES (?, ?)", (task_id, uid))
        logging.info(f"Task {task_id} assigned to users: {user_ids}")
    except Exception as e:
        logging.error(f"Failed to assign task {task_id}: {e}")

async def send_public_notification(channel, message, embed=None):
    """Send public notification to channel"""
    try:
        if channel:
            await channel.send(message, embed=embed)
    except Exception as e:
        logging.error(f"Failed to send public notification: {e}")

def get_subtasks(parent_task_id):
    """Get all subtasks for a parent task"""
    return db.fetchall("""SELECT task_id, task, status, priority FROM tasks 
                         WHERE parent_task_id=? AND status != 'Cancelled' 
                         ORDER BY priority DESC, created_at ASC""", (parent_task_id,))

def get_subtask_progress(parent_task_id):
    """Get subtask completion progress"""
    subtasks = get_subtasks(parent_task_id)
    if not subtasks:
        return 0, 0, 0  # total, completed, percentage
    
    total = len(subtasks)
    completed = len([s for s in subtasks if s[2] == 'Completed'])  # s[2] is status
    percentage = (completed / total * 100) if total > 0 else 0
    
    return total, completed, percentage

def is_subtask(task_id):
    """Check if task is a subtask"""
    row = db.fetchone("SELECT parent_task_id FROM tasks WHERE task_id=?", (task_id,))
    return row and row[0] is not None

def get_parent_task(task_id):
    """Get parent task info"""
    row = db.fetchone("SELECT parent_task_id FROM tasks WHERE task_id=?", (task_id,))
    if row and row[0]:
        return db.fetchone("SELECT task_id, task FROM tasks WHERE task_id=?", (row[0],))
    return None

def get_user_categories(user_id):
    """Get all categories for a user"""
    return db.fetchall("SELECT category_id, name, color, emoji FROM categories WHERE owner_id=? ORDER BY name", (user_id,))

def create_category(user_id, name, color="#3498db", emoji="📝"):
    """Create a new category for user"""
    try:
        db.execute("INSERT OR IGNORE INTO users (user_id, timezone, role) VALUES (?, ?, ?)",
                  (user_id, 'UTC', 'user'))
        
        cursor = db.execute("INSERT INTO categories (name, color, emoji, owner_id) VALUES (?, ?, ?, ?)",
                  (name, color, emoji, user_id))
        return cursor.lastrowid
    except sqlite3.IntegrityError:
        return None  # Category already exists

def get_category_info(category_id):
    """Get category information"""
    return db.fetchone("SELECT name, color, emoji FROM categories WHERE category_id=?", (category_id,))

def parse_tags(tags_string):
    """Parse tags string into list"""
    if not tags_string:
        return []
    return [tag.strip() for tag in tags_string.split(',') if tag.strip()]

def format_tags(tags_list):
    """Format tags list into string"""
    return ', '.join(tags_list) if tags_list else ''

# ---------------- Embed + Buttons ----------------
def create_task_buttons(task_id, user_ids, role):
    class TaskButtons(View):
        def __init__(self):
            super().__init__(timeout=300)  # 5 minute timeout
            
        @discord.ui.button(label="✅ Mark Done", style=discord.ButtonStyle.green, custom_id=f"done_{task_id}")
        async def mark_done(self, interaction: discord.Interaction, button: Button):
            await handle_task_action(interaction, "done", task_id, str(interaction.user.id))
            
        @discord.ui.button(label="🗑 Delete", style=discord.ButtonStyle.red, custom_id=f"delete_{task_id}")
        async def delete_task(self, interaction: discord.Interaction, button: Button):
            await handle_task_action(interaction, "delete", task_id, str(interaction.user.id))
            
        @discord.ui.button(label="✏️ Edit", style=discord.ButtonStyle.blurple, custom_id=f"edit_{task_id}")
        async def edit_task(self, interaction: discord.Interaction, button: Button):
            await handle_task_action(interaction, "edit", task_id, str(interaction.user.id))
        
        @discord.ui.button(label="➕ Add Subtask", style=discord.ButtonStyle.secondary, custom_id=f"subtask_{task_id}")
        async def add_subtask(self, interaction: discord.Interaction, button: Button):
            # Only show for main tasks (not subtasks)
            if not is_subtask(task_id):
                await handle_task_action(interaction, "subtask", task_id, str(interaction.user.id))
            else:
                await interaction.response.send_message("⚠️ ไม่สามารถเพิ่ม subtask ใน subtask ได้", ephemeral=True)
    
    return TaskButtons()

async def handle_task_action(interaction, action, task_id, user_id):
    try:
        # Check permissions
        task_row = db.fetchone("SELECT owner_id, task FROM tasks WHERE task_id=?", (task_id,))
        if not task_row:
            await interaction.response.send_message("⚠️ ไม่พบงานนี้", ephemeral=True)
            return
            
        owner_id, task_name = task_row
        user_role = get_role(user_id)
        
        if user_id != owner_id and user_role != "admin":
            await interaction.response.send_message("⚠️ คุณไม่มีสิทธิ์ในการดำเนินการนี้", ephemeral=True)
            return
        
        if action == "done":
            await complete_task(interaction, task_id, user_id)
        elif action == "delete":
            db.execute("UPDATE tasks SET status='Cancelled' WHERE task_id=?", (task_id,))
            await interaction.response.send_message("🗑 งานถูกยกเลิกเรียบร้อยแล้ว", ephemeral=True)
            
            channel = interaction.channel
            await send_public_notification(
                channel, 
                f"🗑 <@{user_id}> ได้ยกเลิกงาน: `{task_name}`"
            )
            
        elif action == "edit":
            await show_edit_modal(interaction, task_id, user_id)
        elif action == "subtask":
            await show_subtask_modal(interaction, task_id, user_id)
            
    except Exception as e:
        logging.error(f"Error handling task action {action} for task {task_id}: {e}")
        await interaction.response.send_message("❌ เกิดข้อผิดพลาด กรุณาลองใหม่", ephemeral=True)

async def show_subtask_modal(interaction, parent_task_id, user_id):
    class SubtaskModal(Modal, title=f"เพิ่ม Subtask สำหรับ Task ID {parent_task_id}"):
        subtask_name = TextInput(label="ชื่อ Subtask", placeholder="ใส่ชื่อ subtask...", required=True, max_length=200)
        subtask_priority = TextInput(label="ความสำคัญ", placeholder="0-10", required=False, max_length=2, default="0")
        
        async def on_submit(self, modal_interaction):
            try:
                # Get parent task info
                parent_row = db.fetchone("SELECT deadline, category_id, tags FROM tasks WHERE task_id=?", (parent_task_id,))
                if not parent_row:
                    await modal_interaction.response.send_message("⚠️ ไม่พบงานหลัก", ephemeral=True)
                    return
                
                parent_deadline, parent_category, parent_tags = parent_row
                
                # Validate priority
                priority = 0
                if self.subtask_priority.value.strip().isdigit():
                    priority = max(0, min(10, int(self.subtask_priority.value.strip())))
                
                cursor = db.execute("""INSERT INTO tasks (task, deadline, priority, category_id, tags, parent_task_id, owner_id) 
                             VALUES (?, ?, ?, ?, ?, ?, ?)""",
                          (self.subtask_name.value.strip(), parent_deadline, priority, parent_category, parent_tags, parent_task_id, user_id))
                
                subtask_id = cursor.lastrowid
                
                # Assign to same user
                await assign_task(subtask_id, [user_id])
                
                # Update parent task embed to show subtask progress
                await update_task_embed(parent_task_id)
                
                await modal_interaction.response.send_message(f"✅ เพิ่ม subtask **{self.subtask_name.value.strip()}** เรียบร้อยแล้ว!", ephemeral=True)
                
                # Send notification
                channel = modal_interaction.channel
                await send_public_notification(
                    channel, 
                    f"➕ <@{user_id}> ได้เพิ่ม subtask: `{self.subtask_name.value.strip()}` ใน Task ID {parent_task_id}"
                )
                
            except Exception as e:
                logging.error(f"Error in subtask modal: {e}")
                await modal_interaction.response.send_message("❌ เกิดข้อผิดพลาดในการเพิ่ม subtask", ephemeral=True)
    
    await interaction.response.send_modal(SubtaskModal())

async def show_edit_modal(interaction, task_id, user_id):
    """Show modal for editing task details"""
    try:
        # Get current task data
        task_row = db.fetchone("""
            SELECT task, deadline, priority, category_id, tags, description 
            FROM tasks WHERE task_id=? AND owner_id=?
        """, (task_id, user_id))
        
        if not task_row:
            await interaction.response.send_message("⚠️ ไม่พบงานที่ต้องการแก้ไข หรือคุณไม่มีสิทธิ์แก้ไข", ephemeral=True)
            return
        
        task_name, deadline, priority, category_id, tags, description = task_row
        
        class EditTaskModal(Modal, title=f"แก้ไขงาน ID {task_id}"):
            task_name_input = TextInput(
                label="ชื่องาน", 
                placeholder="ใส่ชื่องานใหม่...", 
                required=True, 
                max_length=200,
                default=task_name
            )
            
            deadline_input = TextInput(
                label="กำหนดส่ง (YYYY-MM-DD HH:MM)", 
                placeholder="2024-12-31 23:59", 
                required=False, 
                max_length=16,
                default=deadline or ""
            )
            
            priority_input = TextInput(
                label="ความสำคัญ (0-10)", 
                placeholder="0-10", 
                required=False, 
                max_length=2,
                default=str(priority) if priority else "0"
            )
            
            tags_input = TextInput(
                label="แท็ก (คั่นด้วยจุลภาค)", 
                placeholder="งาน, ด่วน, สำคัญ", 
                required=False, 
                max_length=200,
                default=tags or ""
            )
            
            description_input = TextInput(
                label="รายละเอียด", 
                placeholder="รายละเอียดเพิ่มเติม...", 
                required=False, 
                max_length=500,
                style=discord.TextStyle.paragraph,
                default=description or ""
            )
            
            async def on_submit(self, modal_interaction):
                try:
                    # Validate inputs
                    new_task_name = self.task_name_input.value.strip()
                    if not new_task_name:
                        await modal_interaction.response.send_message("❌ ชื่องานไม่สามารถเป็นค่าว่างได้", ephemeral=True)
                        return
                    
                    # Validate and parse deadline
                    new_deadline = None
                    if self.deadline_input.value.strip():
                        try:
                            new_deadline = datetime.strptime(self.deadline_input.value.strip(), "%Y-%m-%d %H:%M").strftime("%Y-%m-%d %H:%M")
                        except ValueError:
                            await modal_interaction.response.send_message("❌ รูปแบบวันที่ไม่ถูกต้อง ใช้รูปแบบ YYYY-MM-DD HH:MM", ephemeral=True)
                            return
                    
                    # Validate priority
                    new_priority = 0
                    if self.priority_input.value.strip().isdigit():
                        new_priority = max(0, min(10, int(self.priority_input.value.strip())))
                    
                    # Validate tags
                    new_tags = self.tags_input.value.strip() if self.tags_input.value.strip() else None
                    new_description = self.description_input.value.strip() if self.description_input.value.strip() else None
                    
                    # Update task in database
                    db.execute("""
                        UPDATE tasks 
                        SET task=?, deadline=?, priority=?, tags=?, description=?, updated_at=CURRENT_TIMESTAMP
                        WHERE task_id=? AND owner_id=?
                    """, (new_task_name, new_deadline, new_priority, new_tags, new_description, task_id, user_id))
                    
                    # Update task embed
                    await update_task_embed(task_id)
                    
                    await modal_interaction.response.send_message(f"✅ แก้ไขงาน **{new_task_name}** เรียบร้อยแล้ว!", ephemeral=True)
                    
                    # Send notification
                    channel = modal_interaction.channel
                    await send_public_notification(
                        channel, 
                        f"✏️ <@{user_id}> ได้แก้ไขงาน: `{new_task_name}` (ID: {task_id})"
                    )
                    
                    # Log action
                    audit_logger.log_action(user_id, f"EDIT_TASK", f"Task ID: {task_id}, Name: {new_task_name}")
                    
                except Exception as e:
                    logging.error(f"Error in edit task modal: {e}")
                    await modal_interaction.response.send_message("❌ เกิดข้อผิดพลาดในการแก้ไขงาน", ephemeral=True)
        
        await interaction.response.send_modal(EditTaskModal())
        
    except Exception as e:
        logging.error(f"Error showing edit modal: {e}")
        await interaction.response.send_message("❌ เกิดข้อผิดพลาดในการแสดงฟอร์มแก้ไข", ephemeral=True)

async def update_task_embed(task_id):
    """Update task embed with improved performance and error handling"""
    try:
        row = db.fetchone("""SELECT task_id, task, deadline, priority, status, recurring, category_id, tags, parent_task_id, owner_id, message_id, description 
                            FROM tasks WHERE task_id=?""", (task_id,))
        if not row:
            return
            
        task_id, task_name, deadline, priority, status, recurring, category_id, tags, parent_task_id, owner_id, message_id, description = row
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
            
            # Determine color based on status and deadline
            embed_color = discord.Color.blue()
            if status == 'Completed':
                embed_color = discord.Color.green()
                emoji = "✅"
            elif status == 'Cancelled':
                embed_color = discord.Color.greyple()
                emoji = "🚫"
            elif deadline_dt < datetime.now(tz):
                embed_color = discord.Color.red()
                emoji = "⏰"
            else:
                emoji = "📝"
            
            # Override color with category color if available
            if category_id:
                cat_info = get_category_info(category_id)
                if cat_info:
                    cat_name, cat_color, cat_emoji = cat_info
                    embed_color = discord.Color(int(cat_color.replace('#', ''), 16))
                    emoji = cat_emoji
            
            title_prefix = ""
            if parent_task_id:
                parent_info = get_parent_task(task_id)
                if parent_info:
                    title_prefix = f"└─ "  # Subtask indicator
            
            embed = discord.Embed(
                title=f"{title_prefix}{emoji} {task_name}",
                color=embed_color,
                description=description if description else None
            )
            embed.add_field(name="📅 กำหนดส่ง", value=deadline_dt.strftime("%Y-%m-%d %H:%M %Z"), inline=True)
            embed.add_field(name="⭐ ความสำคัญ", value=str(priority), inline=True)
            embed.add_field(name="📊 สถานะ", value=status, inline=True)
            embed.add_field(name="🔄 ทำซ้ำ", value=recurring if recurring else "ไม่", inline=True)
            
            # Add category info
            if category_id:
                cat_info = get_category_info(category_id)
                if cat_info:
                    cat_name, cat_color, cat_emoji = cat_info
                    embed.add_field(name="🏷️ หมวดหมู่", value=f"{cat_emoji} {cat_name}", inline=True)
            
            # Add tags
            if tags:
                tags_list = parse_tags(tags)
                if tags_list:
                    embed.add_field(name="🏷️ แท็ก", value=', '.join([f"`{tag}`" for tag in tags_list]), inline=True)
            
            if parent_task_id:
                parent_info = get_parent_task(task_id)
                if parent_info:
                    embed.add_field(name="📋 งานหลัก", value=f"Task ID: {parent_info[0]} - {parent_info[1]}", inline=False)
            
            if not parent_task_id:  # Only for main tasks
                total_subtasks, completed_subtasks, progress_percent = get_subtask_progress(task_id)
                if total_subtasks > 0:
                    progress_bar = create_progress_bar(completed_subtasks, total_subtasks, 10)
                    embed.add_field(
                        name="📋 Subtasks Progress", 
                        value=f"`{progress_bar}` {completed_subtasks}/{total_subtasks} ({progress_percent:.0f}%)", 
                        inline=False
                    )
                    
                    # Show subtask list
                    subtasks = get_subtasks(task_id)
                    if subtasks:
                        subtask_list = []
                        for st_id, st_name, st_status, st_priority in subtasks[:5]:  # Show max 5
                            status_emoji = "✅" if st_status == "Completed" else "📝"
                            subtask_list.append(f"{status_emoji} {st_name} (ID: {st_id})")
                        
                        embed.add_field(
                            name="📝 Subtasks", 
                            value="\n".join(subtask_list) + (f"\n... และอีก {len(subtasks)-5} subtasks" if len(subtasks) > 5 else ""), 
                            inline=False
                        )
            
            embed.set_footer(text=f"Task ID: {task_id}")
            
            # Only show buttons for pending tasks
            view = None
            if status == 'Pending':
                assigned_users = db.fetchall("SELECT user_id FROM task_assignments WHERE task_id=?", (task_id,))
                user_ids = [uid for (uid,) in assigned_users]
                if user_ids:
                    view = create_task_buttons(task_id, user_ids, get_role(owner_id))
            
            await msg.edit(embed=embed, view=view)
            
        except discord.NotFound:
            logging.warning(f"Message {message_id} not found for task {task_id}")
        except Exception as e:
            logging.error(f"Failed to update embed for task {task_id}: {e}")
            
    except Exception as e:
        logging.error(f"Error in update_task_embed: {e}")

async def complete_task(interaction, task_id, user_id):
    try:
        row = db.fetchone("SELECT task, recurring, parent_task_id FROM tasks WHERE task_id=? AND owner_id=?", (task_id, user_id))
        if not row:
            await interaction.response.send_message("⚠️ ไม่พบงานนี้", ephemeral=True)
            return
            
        task_name, recurring, parent_task_id = row
        db.execute("UPDATE tasks SET status='Completed', updated_at=CURRENT_TIMESTAMP WHERE task_id=?", (task_id,))
        
        await update_task_embed(task_id)
        
        # If this is a subtask, check if all subtasks of parent are completed
        if parent_task_id:
            total_subtasks, completed_subtasks, progress_percent = get_subtask_progress(parent_task_id)
            if progress_percent == 100:
                # Auto-complete parent task
                db.execute("UPDATE tasks SET status='Completed', updated_at=CURRENT_TIMESTAMP WHERE task_id=?", (parent_task_id,))
                await update_task_embed(parent_task_id)
                
                # Get parent task name
                parent_row = db.fetchone("SELECT task FROM tasks WHERE task_id=?", (parent_task_id,))
                parent_name = parent_row[0] if parent_row else "Unknown"
                
                embed = discord.Embed(
                    title="🎉 งานและ Subtasks เสร็จสิ้น!",
                    description=f"<@{user_id}> ได้ทำ subtask **{task_name}** เสร็จ\n\n🎊 งานหลัก **{parent_name}** เสร็จสมบูรณ์แล้ว!",
                    color=discord.Color.gold()
                )
            else:
                await update_task_embed(parent_task_id)  # Update parent progress
                embed = discord.Embed(
                    title="🎉 Subtask เสร็จสิ้น!",
                    description=f"<@{user_id}> ได้ทำ subtask **{task_name}** เสร็จเรียบร้อยแล้ว\n\n📊 ความคืบหน้างานหลัก: {progress_percent:.0f}%",
                    color=discord.Color.green()
                )
        else:
            embed = discord.Embed(
                title="🎉 งานเสร็จสิ้น!",
                description=f"<@{user_id}> ได้ทำงาน **{task_name}** เสร็จเรียบร้อยแล้ว",
                color=discord.Color.green()
            )
        
        embed.set_footer(text=f"Task ID: {task_id}")
        
        await interaction.response.send_message(embed=embed)
        
        # Send DM to user
        try:
            user = await bot.fetch_user(int(user_id))
            await user.send(f"🎉 ยินดีด้วย! คุณทำงาน `{task_name}` เสร็จแล้ว")
        except:
            logging.warning(f"Cannot send DM to user {user_id}")
            
    except Exception as e:
        logging.error(f"Error completing task {task_id}: {e}")
        await interaction.response.send_message("❌ เกิดข้อผิดพลาด", ephemeral=True)

def create_progress_bar(value, total_val, length=15):
    if total_val == 0:
        return "░" * length
    filled = int((value / total_val) * length)
    return "█" * filled + "░" * (length - filled)

@bot.tree.command(name="addcategory", description="🏷️ เพิ่มหมวดหมู่ใหม่สำหรับจัดกลุ่มงาน")
@rate_limit("command")
async def addcategory(interaction: discord.Interaction, name: str, emoji: str = "📝", color: str = "#3498db"):
    try:
        user_id = str(interaction.user.id)
        
        # Validate inputs
        is_valid, error_msg = SecurityValidator.validate_category_name(name)
        if not is_valid:
            await interaction.response.send_message(f"⚠️ {error_msg}", ephemeral=True)
            return
        
        is_valid, error_msg = SecurityValidator.validate_color(color)
        if not is_valid:
            await interaction.response.send_message(f"⚠️ {error_msg}", ephemeral=True)
            return
        
        # Sanitize inputs
        name = SecurityValidator.sanitize_input(name)
        emoji = SecurityValidator.sanitize_input(emoji)
        
        # Validate emoji (basic check)
        if len(emoji) > 4:
            await interaction.response.send_message("⚠️ emoji ไม่ถูกต้อง", ephemeral=True)
            return
        
        category_id = create_category(user_id, name, color, emoji)
        
        if category_id:
            audit_logger.log_action(user_id, "CREATE_CATEGORY", f"name={name}, color={color}")
            
            embed = discord.Embed(
                title="🏷️ เพิ่มหมวดหมู่เรียบร้อย",
                description=f"{emoji} **{name}**",
                color=int(color.replace('#', ''), 16)
            )
            embed.add_field(name="🎨 สี", value=color, inline=True)
            embed.add_field(name="📊 ID", value=str(category_id), inline=True)
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message("⚠️ หมวดหมู่นี้มีอยู่แล้ว", ephemeral=True)
            
    except Exception as e:
        logging.error(f"Error in addcategory: {e}")
        await interaction.response.send_message("❌ เกิดข้อผิดพลาด", ephemeral=True)

@bot.tree.command(name="listcategories", description="🏷️ ดูหมวดหมู่ทั้งหมดของคุณ")
@rate_limit("command")
async def listcategories(interaction: discord.Interaction):
    try:
        user_id = str(interaction.user.id)
        categories = get_user_categories(user_id)
        
        if not categories:
            embed = discord.Embed(
                title="🏷️ หมวดหมู่ของคุณ",
                description="คุณยังไม่มีหมวดหมู่ ใช้ `/addcategory` เพื่อเพิ่ม",
                color=discord.Color.blue()
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        
        embed = discord.Embed(
            title="🏷️ หมวดหมู่ของคุณ",
            color=discord.Color.blue()
        )
        
        for cat_id, name, color, emoji in categories:
            # Count tasks in this category
            task_count = len(db.fetchall("SELECT task_id FROM tasks WHERE category_id=? AND owner_id=? AND status != 'Cancelled'", (cat_id, user_id)))
            
            embed.add_field(
                name=f"{emoji} {name}",
                value=f"🎨 {color}\n📊 ID: {cat_id}\n📝 งาน: {task_count}",
                inline=True
            )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        logging.error(f"Error in listcategories: {e}")
        await interaction.response.send_message("❌ เกิดข้อผิดพลาด", ephemeral=True)

@bot.tree.command(name="addtask", description="➕ เพิ่มงานใหม่")
@rate_limit("task")
async def addtask(
    interaction: discord.Interaction, 
    deadline: str, 
    task: str, 
    priority: int = 0, 
    recurring: str = None, 
    users: str = "",
    category: int = None,
    tags: str = "",
    description: str = ""
):
    try:
        user_id = str(interaction.user.id)
        tz = pytz.timezone(get_timezone(user_id))
        
        # Validate task name
        is_valid, error_msg = SecurityValidator.validate_task_name(task)
        if not is_valid:
            audit_logger.log_security_event(user_id, "INVALID_TASK_NAME", task)
            await interaction.response.send_message(f"⚠️ {error_msg}", ephemeral=True)
            return
        
        # Validate tags
        is_valid, error_msg = SecurityValidator.validate_tags(tags)
        if not is_valid:
            await interaction.response.send_message(f"⚠️ {error_msg}", ephemeral=True)
            return
        
        # Sanitize inputs
        task = SecurityValidator.sanitize_input(task)
        tags = SecurityValidator.sanitize_input(tags)
        description = SecurityValidator.sanitize_input(description)
        
        # Validate and parse deadline
        try:
            if len(deadline.split()) == 1:  # Only date provided
                deadline_dt = datetime.strptime(deadline, "%Y-%m-%d")
                deadline_dt = deadline_dt.replace(hour=23, minute=59)  # Set to end of day
            else:  # Date and time provided
                deadline_dt = datetime.strptime(deadline, "%Y-%m-%d %H:%M")
        except ValueError:
            embed = discord.Embed(
                title="⚠️ Date Format Error",
                description="รูปแบบวัน/เวลาไม่ถูกต้อง\n\n**ตัวอย่าง:**\n• `2024-12-25` (วันที่อย่างเดียว)\n• `2024-12-25 14:30` (วันที่และเวลา)",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        
        # Localize datetime
        deadline_dt = tz.localize(deadline_dt)
        
        # Validate priority
        priority = max(0, min(10, priority))
        
        # Validate recurring
        if recurring and recurring.lower() not in ['daily', 'weekly', 'monthly']:
            embed = discord.Embed(
                title="⚠️ Recurring Error",
                description="ค่า recurring ต้องเป็น: `daily`, `weekly`, `monthly` หรือเว้นว่าง",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        
        # Validate category
        if category:
            cat_info = db.fetchone("SELECT category_id FROM categories WHERE category_id=? AND owner_id=?", (category, user_id))
            if not cat_info:
                await interaction.response.send_message("⚠️ ไม่พบหมวดหมู่นี้", ephemeral=True)
                return
        
        # Parse tags
        tags_list = parse_tags(tags)
        tags_string = format_tags(tags_list)
        
        cursor = db.execute("""INSERT INTO tasks (task, deadline, priority, recurring, category_id, tags, owner_id, description) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                  (task, deadline_dt.isoformat(), priority, recurring, category, tags_string, user_id, description))
        
        task_id = cursor.lastrowid
        
        audit_logger.log_action(user_id, "CREATE_TASK", f"task_id={task_id}, name={task}")
        
        # Handle user assignments
        user_ids = [u.strip() for u in users.split(",") if u.strip()]
        if not user_ids:
            user_ids = [user_id]
        
        await assign_task(task_id, user_ids)
        
        # Create and send embed
        embed_color = discord.Color.green()
        category_display = ""
        
        if category:
            cat_info = get_category_info(category)
            if cat_info:
                cat_name, cat_color, cat_emoji = cat_info
                embed_color = discord.Color(int(cat_color.replace('#', ''), 16))
                category_display = f"{cat_emoji} {cat_name}"
        
        embed = discord.Embed(
            title="✅ งานใหม่ถูกเพิ่มแล้ว",
            description=f"**{task}**",
            color=embed_color
        )
        embed.add_field(name="📅 กำหนดส่ง", value=deadline_dt.strftime("%Y-%m-%d %H:%M %Z"), inline=True)
        embed.add_field(name="⭐ ความสำคัญ", value=str(priority), inline=True)
        embed.add_field(name="🔄 ทำซ้ำ", value=recurring if recurring else "ไม่", inline=True)
        
        if category_display:
            embed.add_field(name="🏷️ หมวดหมู่", value=category_display, inline=True)
        
        if tags_list:
            embed.add_field(name="🏷️ แท็ก", value=', '.join([f"`{tag}`" for tag in tags_list]), inline=True)

        if description:
            embed.add_field(name="📝 รายละเอียด", value=description, inline=False)
        
        embed.set_footer(text=f"Task ID: {task_id} | เพิ่มโดย {interaction.user.display_name}")
        
        view = create_task_buttons(task_id, user_ids, get_role(user_id))
        
        # Send to notification channel
        channel_id = get_channel(user_id)
        channel = bot.get_channel(channel_id) if channel_id else interaction.channel
        
        msg = await channel.send(embed=embed, view=view)
        
        # Update task with message ID
        db.execute("UPDATE tasks SET message_id=? WHERE task_id=?", (msg.id, task_id))
        
        await interaction.response.send_message(f"✅ เพิ่มงาน **{task}** เรียบร้อยแล้ว!", ephemeral=True)
        
    except Exception as e:
        logging.error(f"Error in addtask: {e}")
        await interaction.response.send_message("❌ เกิดข้อผิดพลาดในการเพิ่มงาน", ephemeral=True)

@bot.tree.command(name="addsubtask", description="➕ เพิ่ม subtask ให้กับงานที่มีอยู่")
@rate_limit("task")
async def addsubtask(interaction: discord.Interaction, parent_task_id: int, subtask_name: str, priority: int = 0):
    try:
        user_id = str(interaction.user.id)
        
        # Validate subtask name
        is_valid, error_msg = SecurityValidator.validate_task_name(subtask_name)
        if not is_valid:
            audit_logger.log_security_event(user_id, "INVALID_SUBTASK_NAME", subtask_name)
            await interaction.response.send_message(f"⚠️ {error_msg}", ephemeral=True)
            return
        
        # Sanitize input
        subtask_name = SecurityValidator.sanitize_input(subtask_name)
        
        # Check if parent task exists and belongs to user
        parent_row = db.fetchone("SELECT task, deadline, category_id, tags, parent_task_id FROM tasks WHERE task_id=? AND owner_id=?", (parent_task_id, user_id))
        if not parent_row:
            await interaction.response.send_message("⚠️ ไม่พบงานหลักหรือคุณไม่มีสิทธิ์", ephemeral=True)
            return
        
        parent_name, parent_deadline, parent_category, parent_tags, parent_parent_id = parent_row
        
        # Check if parent is already a subtask (no nested subtasks)
        if parent_parent_id:
            await interaction.response.send_message("⚠️ ไม่สามารถเพิ่ม subtask ใน subtask ได้", ephemeral=True)
            return
        
        # Validate priority
        priority = max(0, min(10, priority))
        
        # Insert subtask
        db.execute("""INSERT INTO tasks (task, deadline, priority, category_id, tags, parent_task_id, owner_id) 
                     VALUES (?, ?, ?, ?, ?, ?, ?)""",
                  (subtask_name, parent_deadline, priority, parent_category, parent_tags, parent_task_id, user_id))
        
        subtask_id = db.conn.lastrowid
        
        audit_logger.log_action(user_id, "CREATE_SUBTASK", f"subtask_id={subtask_id}, parent_id={parent_task_id}, name={subtask_name}")
        
        # Assign to same user
        await assign_task(subtask_id, [user_id])
        
        # Update parent task embed
        await update_task_embed(parent_task_id)
        
        embed = discord.Embed(
            title="✅ เพิ่ม Subtask เรียบร้อย",
            description=f"**{subtask_name}**\n\nสำหรับงานหลัก: {parent_name}",
            color=discord.Color.green()
        )
        embed.add_field(name="⭐ ความสำคัญ", value=str(priority), inline=True)
        embed.add_field(name="📊 Subtask ID", value=str(subtask_id), inline=True)
        embed.set_footer(text=f"Parent Task ID: {parent_task_id}")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        logging.error(f"Error in addsubtask: {e}")
        await interaction.response.send_message("❌ เกิดข้อผิดพลาดในการเพิ่ม subtask", ephemeral=True)

@bot.tree.command(name="searchtasks", description="🔍 ค้นหางานด้วยคำสำคัญ")
@rate_limit("command")
async def searchtasks(interaction: discord.Interaction, keyword: str, category: int = None, status: str = None):
    try:
        user_id = str(interaction.user.id)
        tz = pytz.timezone(get_timezone(user_id))
        
        keyword = SecurityValidator.sanitize_input(keyword)
        
        if not keyword or len(keyword.strip()) == 0:
            await interaction.response.send_message("⚠️ กรุณาใส่คำสำคัญในการค้นหา", ephemeral=True)
            return
        
        audit_logger.log_action(user_id, "SEARCH_TASKS", f"keyword={keyword}")
        
        # Build search query
        base_query = """SELECT task_id, task, deadline, priority, status, recurring, category_id, tags, parent_task_id 
                       FROM tasks WHERE owner_id=? AND status != 'Cancelled'"""
        params = [user_id]
        
        # Add keyword search
        if keyword:
            base_query += " AND (task LIKE ? OR tags LIKE ?)"
            keyword_param = f"%{keyword}%"
            params.extend([keyword_param, keyword_param])
        
        # Add category filter
        if category:
            base_query += " AND category_id=?"
            params.append(category)
        
        # Add status filter
        if status and status.lower() in ['pending', 'completed']:
            base_query += " AND status=?"
            params.append(status.capitalize())
        
        base_query += " ORDER BY priority DESC, deadline ASC"
        
        tasks_list = db.fetchall(base_query, params)
        
        if not tasks_list:
            embed = discord.Embed(
                title="🔍 ผลการค้นหา",
                description=f"ไม่พบงานที่ตรงกับ: `{keyword}`",
                color=discord.Color.orange()
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        
        embed = discord.Embed(
            title=f"🔍 ผลการค้นหา: {keyword}",
            description=f"พบ {len(tasks_list)} งาน",
            color=discord.Color.blue()
        )
        
        now_local = datetime.now(tz)
        
        for tid, tname, deadline, prio, task_status, recurring, cat_id, tags, parent_id in tasks_list[:10]:  # Show max 10
            try:
                deadline_dt = datetime.fromisoformat(deadline).astimezone(tz)
            except:
                deadline_dt = now_local
            
            # Status emoji
            if task_status == "Completed":
                status_emoji = "✅"
            elif deadline_dt < now_local and task_status == "Pending":
                status_emoji = "⏰"
            else:
                status_emoji = "📝"
            
            # Category info
            cat_display = ""
            if cat_id:
                cat_info = get_category_info(cat_id)
                if cat_info:
                    cat_name, cat_color, cat_emoji = cat_info
                    cat_display = f" | {cat_emoji} {cat_name}"
            
            # Parent task indicator
            parent_indicator = "└─ " if parent_id else ""
            
            embed.add_field(
                name=f"{parent_indicator}{status_emoji} {tname}",
                value=f"📅 {deadline_dt.strftime('%m/%d %H:%M')} | ⭐ {prio} | ID: {tid}{cat_display}",
                inline=False
            )
        
        if len(tasks_list) > 10:
            embed.set_footer(text=f"แสดง 10 จาก {len(tasks_list)} งาน")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        logging.error(f"Error in searchtasks: {e}")
        await interaction.response.send_message("❌ เกิดข้อผิดพลาดในการค้นหา", ephemeral=True)

@bot.tree.command(name="help", description="📖 ดูคำสั่งทั้งหมดของ To-Do Bot")
async def help_command(interaction: discord.Interaction):
    try:
        embed = discord.Embed(
            title="📖 To-Do Bot Commands", 
            description="บอทจัดการงานที่ทรงพลัง ใช้งานง่าย!",
            color=discord.Color.blurple()
        )
        
        commands_info = [
            ("🌏 /settimezone", "ตั้ง timezone ของคุณ (เช่น Asia/Bangkok)"),
            ("📢 /setchannel", "ตั้งช่องแจ้งเตือนสำหรับ reminder"),
            ("➕ /addtask", "เพิ่มงานใหม่พร้อมกำหนดส่งและความสำคัญ"),
            ("➕ /addsubtask", "เพิ่ม subtask ให้กับงานที่มีอยู่"),
            ("🏷️ /addcategory", "เพิ่มหมวดหมู่ใหม่สำหรับจัดกลุ่มงาน"),
            ("🏷️ /listcategories", "ดูหมวดหมู่ทั้งหมดของคุณ"),
            ("📋 /listtasks", "ดูงานทั้งหมดแบบ interactive filter"),
            ("🔍 /searchtasks", "ค้นหางานด้วยคำสำคัญ"),
            ("📊 /taskstats", "ดูสถิติงาน Completed/Pending/Overdue"),
            ("🔧 /admin", "คำสั่งสำหรับ admin (ถ้ามีสิทธิ์)")
        ]
        
        for name, desc in commands_info:
            embed.add_field(name=name, value=desc, inline=False)
        
        embed.set_footer(text="💡 ใช้ปุ่มใต้งานเพื่อจัดการงานได้ง่ายขึ้น!")
        await interaction.response.send_message(embed=embed, ephemeral=False)
        
    except Exception as e:
        logging.error(f"Error in help command: {e}")
        await interaction.response.send_message("❌ เกิดข้อผิดพลาด", ephemeral=True)

@bot.tree.command(name="listtasks", description="📋 ดูงานทั้งหมดของคุณแบบ filterable")
async def listtasks(interaction: discord.Interaction):
    try:
        user_id = str(interaction.user.id)
        tz = pytz.timezone(get_timezone(user_id))
        
        tasks_list = db.fetchall("""SELECT task_id, task, deadline, priority, status, recurring, category_id, tags, parent_task_id 
                                   FROM tasks WHERE owner_id=? AND status != 'Cancelled'
                                   ORDER BY priority DESC, deadline ASC""", (user_id,))
        
        if not tasks_list:
            embed = discord.Embed(
                title="📋 งานของคุณ",
                description="🎉 คุณยังไม่มีงานค้าง ณ ตอนนี้",
                color=discord.Color.green()
            )
            await interaction.response.send_message(embed=embed, ephemeral=False)
            
            # Send DM summary
            try:
                user = await bot.fetch_user(int(user_id))
                await user.send("📋 คุณยังไม่มีงานค้าง ณ ตอนนี้")
            except:
                pass
            return

        def make_embed(filter_status=None, filter_category=None, show_subtasks=True):
            embed = discord.Embed(title="📋 งานของคุณ", color=discord.Color.blurple())
            filtered_tasks = []
            now_local = datetime.now(tz)
            
            for tid, tname, deadline, prio, status, recurring, cat_id, tags, parent_id in tasks_list:
                try:
                    deadline_dt = datetime.fromisoformat(deadline).astimezone(tz)
                except:
                    deadline_dt = now_local
                
                # Apply filters
                if filter_status == "Overdue":
                    if status == "Pending" and deadline_dt < now_local:
                        filtered_tasks.append((tid, tname, deadline_dt, prio, status, recurring, cat_id, tags, parent_id))
                elif filter_status and status != filter_status:
                    continue
                elif filter_category and cat_id != filter_category:
                    continue
                elif not show_subtasks and parent_id:
                    continue  # Skip subtasks if not showing them
                else:
                    filtered_tasks.append((tid, tname, deadline_dt, prio, status, recurring, cat_id, tags, parent_id))
            
            if not filtered_tasks:
                embed.description = "ไม่พบงานตาม filter ที่เลือก"
            else:
                # Group by status for better organization
                pending_tasks = [t for t in filtered_tasks if t[4] == "Pending"]
                completed_tasks = [t for t in filtered_tasks if t[4] == "Completed"]
                
                if pending_tasks:
                    embed.add_field(name="📝 งานที่ต้องทำ", value="\u200b", inline=False)
                    for tid, tname, deadline_dt, prio, status, recurring, cat_id, tags, parent_id in pending_tasks[:10]:  # Limit to 10
                        status_emoji = "⏰" if deadline_dt < now_local else "📝"
                        
                        # Category info
                        cat_display = ""
                        if cat_id:
                            cat_info = get_category_info(cat_id)
                            if cat_info:
                                cat_name, cat_color, cat_emoji = cat_info
                                cat_display = f" | {cat_emoji} {cat_name}"
                        
                        # Parent task indicator
                        parent_indicator = "└─ " if parent_id else ""
                        
                        embed.add_field(
                            name=f"{parent_indicator}{status_emoji} {tname}",
                            value=f"📅 {deadline_dt.strftime('%m/%d %H:%M')} | ⭐ {prio} | ID: {tid}{cat_display}",
                            inline=True
                        )
                
                if completed_tasks and not filter_status:
                    embed.add_field(name="✅ งานที่เสร็จแล้ว", value="\u200b", inline=False)
                    for tid, tname, deadline_dt, prio, status, recurring, cat_id, tags, parent_id in completed_tasks[:5]:  # Limit to 5
                        parent_indicator = "└─ " if parent_id else ""
                        embed.add_field(
                            name=f"{parent_indicator}✅ {tname}",
                            value=f"📅 {deadline_dt.strftime('%m/%d %H:%M')} | ID: {tid}",
                            inline=True
                        )
            
            embed.set_footer(text=f"รวม {len(filtered_tasks)} งาน | ใช้ dropdown เพื่อกรอง")
            return embed

        class TaskFilter(View):
            def __init__(self):
                super().__init__(timeout=300)
                
                # Status filter
                status_options = [
                    discord.SelectOption(label="📋 ทั้งหมด", value="all", emoji="📋"),
                    discord.SelectOption(label="📝 รอดำเนินการ", value="Pending", emoji="📝"),
                    discord.SelectOption(label="✅ เสร็จแล้ว", value="Completed", emoji="✅"),
                    discord.SelectOption(label="⏰ เกินกำหนด", value="Overdue", emoji="⏰"),
                ]
                self.add_item(Select(placeholder="🔍 เลือกสถานะที่ต้องการดู", options=status_options, custom_id="status_filter"))
                
                # Category filter
                categories = get_user_categories(user_id)
                if categories:
                    cat_options = [discord.SelectOption(label="🏷️ ทุกหมวดหมู่", value="all_cats", emoji="🏷️")]
                    for cat_id, cat_name, cat_color, cat_emoji in categories[:20]:  # Max 20 categories
                        cat_options.append(discord.SelectOption(label=cat_name, value=str(cat_id), emoji=cat_emoji))
                    
                    self.add_item(Select(placeholder="🏷️ เลือกหมวดหมู่", options=cat_options, custom_id="category_filter"))
                
                # Subtask toggle
                subtask_options = [
                    discord.SelectOption(label="📋 แสดงทั้งหมด", value="show_all", emoji="📋"),
                    discord.SelectOption(label="📝 เฉพาะงานหลัก", value="main_only", emoji="📝"),
                ]
                self.add_item(Select(placeholder="📋 แสดง subtasks", options=subtask_options, custom_id="subtask_filter"))

            @discord.ui.select(custom_id="status_filter")
            async def status_filter_callback(self, interaction_select: discord.Interaction, select):
                filter_val = select.values[0]
                status_filter = None if filter_val == "all" else filter_val
                embed = make_embed(filter_status=status_filter)
                await interaction_select.response.edit_message(embed=embed, view=self)
            
            @discord.ui.select(custom_id="category_filter")
            async def category_filter_callback(self, interaction_select: discord.Interaction, select):
                filter_val = select.values[0]
                category_filter = None if filter_val == "all_cats" else int(filter_val)
                embed = make_embed(filter_category=category_filter)
                await interaction_select.response.edit_message(embed=embed, view=self)
            
            @discord.ui.select(custom_id="subtask_filter")
            async def subtask_filter_callback(self, interaction_select: discord.Interaction, select):
                filter_val = select.values[0]
                show_subtasks = filter_val == "show_all"
                embed = make_embed(show_subtasks=show_subtasks)
                await interaction_select.response.edit_message(embed=embed, view=self)

        # Send DM summary
        try:
            user = await bot.fetch_user(int(user_id))
            summary_lines = []
            for tid, tname, deadline, prio, status, recurring, cat_id, tags, parent_id in tasks_list:
                parent_indicator = "└─ " if parent_id else ""
                summary_lines.append(f"• {parent_indicator}{tname} ({status}) - Priority: {prio}")
            
            summary_msg = f"📋 **รายการงานของคุณ** ({len(tasks_list)} งาน):\n" + "\n".join(summary_lines[:20])
            if len(tasks_list) > 20:
                summary_msg += f"\n... และอีก {len(tasks_list) - 20} งาน"
            
            await user.send(summary_msg)
        except Exception as e:
            logging.warning(f"Cannot send DM summary to user {user_id}: {e}")

        try:
            await interaction.response.send_message(embed=make_embed(), view=TaskFilter(), ephemeral=False)
        except discord.HTTPException as e:
            # This usually indicates the embed or view was too large/invalid for Discord.
            logging.error(f"Failed to send tasks embed publicly: {e}")
            # Fallback: send a concise summary via DM and notify the user ephemerally.
            try:
                user = await bot.fetch_user(int(user_id))
                summary_lines = []
                # limit amount of items in DM to avoid hitting message length limits
                for tid, tname, deadline, prio, status, recurring, cat_id, tags, parent_id in tasks_list[:20]:
                    parent_indicator = "└─ " if parent_id else ""
                    summary_lines.append(f"• {parent_indicator}{tname} ({status}) - Priority: {prio}")
                summary_msg = f"📋 รายการงานของคุณ ({len(tasks_list)} งาน):\n" + "\n".join(summary_lines)
                # ensure the DM is not empty and not too long
                if len(summary_msg) > 1900:
                    summary_msg = summary_msg[:1900] + "\n... (ตัดทอนเนื้อหาเพื่อความปลอดภัย)"
                await user.send(summary_msg)
                # notify in the interaction that we sent a DM as fallback
                try:
                    # If the original response failed, use followup to notify ephemeral
                    await interaction.followup.send("✅ ส่งสรุปงานทาง DM เรียบร้อยแล้ว (เนื่องจากเนื้อหามากเกินไปที่จะโพสต์ที่ช่องนี้)", ephemeral=True)
                except Exception:
                    # Last resort: try to respond ephemerally (may fail if already responded)
                    try:
                        await interaction.response.send_message("✅ ส่งสรุปงานทาง DM เรียบร้อยแล้ว", ephemeral=True)
                    except Exception as e2:
                        logging.error(f"Could not notify user in interaction after DM fallback: {e2}")
            except Exception as dm_err:
                logging.error(f"Fallback DM also failed while handling listtasks: {dm_err}")
                # Final fallback: attempt a minimal followup message
                try:
                    await interaction.followup.send("❌ ไม่สามารถแสดงรายการงานที่ช่องนี้ และส่ง DM ไม่สำเร็จ โปรดลองใหม่ภายหลัง", ephemeral=True)
                except Exception as final_err:
                    logging.error(f"Final fallback failed in listtasks: {final_err}")
        
    except Exception as e:
        logging.error(f"Error in listtasks: {e}")
        await interaction.response.send_message("❌ เกิดข้อผิดพลาด", ephemeral=True)

@bot.tree.command(name="taskstats", description="📊 ดูสถิติ Completed/Pending/Overdue ของคุณ")
async def taskstats(interaction: discord.Interaction):
    try:
        user_id = str(interaction.user.id)
        tz = pytz.timezone(get_timezone(user_id))
        
        rows = db.fetchall("SELECT status, deadline, parent_task_id FROM tasks WHERE owner_id=? AND status != 'Cancelled'", (user_id,))
        
        if not rows:
            embed = discord.Embed(
                title="📊 สถิติงานของคุณ",
                description="🎉 คุณยังไม่มีงานในระบบ",
                color=discord.Color.green()
            )
            await interaction.response.send_message(embed=embed)
            return
        
        now_local = datetime.now(tz)
        pending = 0
        completed = 0
        overdue = 0
        main_tasks = 0
        subtasks = 0
        
        for status, deadline, parent_id in rows:
            try:
                deadline_dt = datetime.fromisoformat(deadline).astimezone(tz)
            except:
                deadline_dt = now_local
            
            # Count task types
            if parent_id:
                subtasks += 1
            else:
                main_tasks += 1
                
            if status == "Completed":
                completed += 1
            elif status == "Pending":
                if deadline_dt < now_local:
                    overdue += 1
                else:
                    pending += 1
        
        total = pending + completed + overdue
        
        def create_progress_bar(value, total_val, length=15):
            if total_val == 0:
                return "░" * length
            filled = int((value / total_val) * length)
            return "█" * filled + "░" * (length - filled)
        
        embed = discord.Embed(title="📊 สถิติงานของคุณ", color=discord.Color.purple())
        
        # Add statistics with progress bars
        embed.add_field(
            name=f"📝 รอดำเนินการ ({pending})",
            value=f"`{create_progress_bar(pending, total)}` {pending}/{total}",
            inline=False
        )
        embed.add_field(
            name=f"✅ เสร็จแล้ว ({completed})",
            value=f"`{create_progress_bar(completed, total)}` {completed}/{total}",
            inline=False
        )
        embed.add_field(
            name=f"⏰ เกินกำหนด ({overdue})",
            value=f"`{create_progress_bar(overdue, total)}` {overdue}/{total}",
            inline=False
        )
        
        # Task type breakdown
        embed.add_field(name="📋 งานหลัก", value=str(main_tasks), inline=True)
        embed.add_field(name="📝 Subtasks", value=str(subtasks), inline=True)
        
        # Calculate completion rate
        completion_rate = (completed / total * 100) if total > 0 else 0
        embed.add_field(
            name="🎯 อัตราความสำเร็จ",
            value=f"{completion_rate:.1f}%",
            inline=True
        )
        
        # Category breakdown
        cat_stats = db.fetchall("""SELECT c.name, c.emoji, COUNT(t.task_id) as task_count
                                  FROM categories c 
                                  LEFT JOIN tasks t ON c.category_id = t.category_id AND t.owner_id = ? AND t.status != 'Cancelled'
                                  WHERE c.owner_id = ?
                                  GROUP BY c.category_id, c.name, c.emoji
                                  HAVING task_count > 0
                                  ORDER BY task_count DESC""", (user_id, user_id))
        
        if cat_stats:
            cat_display = []
            for cat_name, cat_emoji, count in cat_stats[:5]:  # Top 5 categories
                cat_display.append(f"{cat_emoji} {cat_name}: {count}")
            
            embed.add_field(
                name="🏷️ หมวดหมู่ที่ใช้มากที่สุด",
                value="\n".join(cat_display),
                inline=False
            )
        
        embed.set_footer(text=f"รวมทั้งหมด {total} งาน")
        await interaction.response.send_message(embed=embed)
        
    except Exception as e:
        logging.error(f"Error in taskstats: {e}")
        await interaction.response.send_message("❌ เกิดข้อผิดพลาด", ephemeral=True)

@bot.tree.command(name="settimezone", description="🌏 ตั้ง timezone ของคุณ")
async def settimezone(interaction: discord.Interaction, timezone: str):
    try:
        # Validate timezone
        pytz.timezone(timezone)
        save_user(str(interaction.user.id), tz=timezone)
        
        embed = discord.Embed(
            title="🌏 Timezone Updated",
            description=f"ตั้ง timezone เป็น **{timezone}** เรียบร้อยแล้ว",
            color=discord.Color.green()
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except pytz.UnknownTimeZoneError:
        embed = discord.Embed(
            title="⚠️ Timezone Error",
            description=f"Timezone `{timezone}` ไม่ถูกต้อง\n\nตัวอย่าง: `Asia/Bangkok`, `UTC`, `America/New_York`",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
    except Exception as e:
        logging.error(f"Error in settimezone: {e}")
        await interaction.response.send_message("❌ เกิดข้อผิดพลาด", ephemeral=True)

@bot.tree.command(name="setchannel", description="📢 ตั้งช่องแจ้งเตือนสำหรับ reminder")
async def setchannel(interaction: discord.Interaction):
    try:
        save_user(str(interaction.user.id), channel_id=interaction.channel.id)
        
        embed = discord.Embed(
            title="📢 Channel Set",
            description=f"ตั้งช่องแจ้งเตือนเป็น {interaction.channel.mention} เรียบร้อยแล้ว",
            color=discord.Color.green()
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        logging.error(f"Error in setchannel: {e}")
        await interaction.response.send_message("❌ เกิดข้อผิดพลาด", ephemeral=True)

# ---------------- Improved Background Tasks ----------------
@tasks.loop(seconds=REMINDER_INTERVAL)
async def reminder_loop():
    try:
        now_utc = datetime.now(pytz.UTC)
        users = db.fetchall("SELECT user_id, channel_id, timezone FROM users WHERE channel_id IS NOT NULL")
        
        for user_id, channel_id, tz_name in users:
            try:
                tz = pytz.timezone(tz_name)
                now_local = now_utc.astimezone(tz)
                
                # Get pending tasks approaching deadline (within 1 hour)
                tasks_data = db.fetchall("""SELECT task_id, task, deadline FROM tasks 
                                           WHERE owner_id=? AND status='Pending'""", (user_id,))
                
                for task_id, task_name, deadline in tasks_data:
                    try:
                        deadline_dt = datetime.fromisoformat(deadline).astimezone(tz)
                        time_diff = (deadline_dt - now_local).total_seconds()
                        
                        # Remind 1 hour before deadline
                        if 0 <= time_diff <= 3600:
                            channel = bot.get_channel(channel_id)
                            if channel:
                                embed = discord.Embed(
                                    title="⏰ แจ้งเตือน!",
                                    description=f"<@{user_id}> งาน **{task_name}** ใกล้ถึงกำหนดส่งแล้ว!",
                                    color=discord.Color.orange()
                                )
                                embed.add_field(name="📅 กำหนดส่ง", value=deadline_dt.strftime('%Y-%m-%d %H:%M %Z'))
                                embed.add_field(name="⏱ เหลือเวลา", value=f"{int(time_diff/60)} นาที")
                                
                                await channel.send(embed=embed)
                            
                            # Send DM
                            try:
                                user = await bot.fetch_user(int(user_id))
                                await user.send(f"⏰ แจ้งเตือน! งาน `{task_name}` ใกล้ถึงกำหนดส่งแล้ว ({deadline_dt.strftime('%Y-%m-%d %H:%M %Z')})")
                            except:
                                pass
                                
                    except Exception as e:
                        logging.error(f"Error processing reminder for task {task_id}: {e}")
                        
            except Exception as e:
                logging.error(f"Error processing reminders for user {user_id}: {e}")
                
    except Exception as e:
        logging.error(f"Error in reminder loop: {e}")

@tasks.loop(seconds=RECURRING_INTERVAL)
async def recurring_task_loop():
    try:
        now_utc = datetime.now(pytz.UTC)
        
        # Get completed recurring tasks
        recurring_tasks = db.fetchall("""SELECT task_id, task, deadline, priority, recurring, owner_id 
                                        FROM tasks WHERE recurring IS NOT NULL AND status='Completed'""")
        
        for task_id, task_name, deadline, priority, recurring, owner_id in recurring_tasks:
            try:
                tz = pytz.timezone(get_timezone(owner_id))
                last_deadline = datetime.fromisoformat(deadline).astimezone(tz)
                next_deadline = calculate_next_deadline(last_deadline, recurring)
                
                if next_deadline and next_deadline <= datetime.now(tz):
                    # Create new recurring task
                    next_deadline_utc = next_deadline.astimezone(pytz.UTC)
                    
                    db.execute("""INSERT INTO tasks (task, deadline, priority, recurring, owner_id) 
                                 VALUES (?, ?, ?, ?, ?)""",
                              (task_name, next_deadline_utc.isoformat(), priority, recurring, owner_id))
                    
                    new_task_id = db.conn.lastrowid
                    
                    # Assign to same users
                    assigned_users = db.fetchall("SELECT user_id FROM task_assignments WHERE task_id=?", (task_id,))
                    for (user_id,) in assigned_users:
                        db.execute("INSERT INTO task_assignments (task_id, user_id) VALUES (?, ?)", 
                                  (new_task_id, user_id))
                    
                    logging.info(f"Created recurring task {new_task_id} from {task_id}")
                    
            except Exception as e:
                logging.error(f"Error processing recurring task {task_id}: {e}")
                
    except Exception as e:
        logging.error(f"Error in recurring task loop: {e}")

@bot.event
async def on_disconnect():
    logging.info("Bot disconnected")

@bot.event
async def on_resumed():
    logging.info("Bot resumed connection")

@bot.event
async def on_ready():
    """Enhanced bot ready event with comprehensive initialization"""
    try:
        logging.info(f'{bot.user} has connected to Discord!')
        
        # Initialize database with retry logic
        await db.connect()
        db.init_db()
        
        # Start background tasks
        #asyncio.create_task(cleanup_rate_limiter())
        #asyncio.create_task(daily_backup_task())
        #asyncio.create_task(reminder_task())
        reminder_loop.start()
        recurring_task_loop.start()
        
        # Sync commands
        try:
            synced = await bot.tree.sync()
            logging.info(f"Synced {len(synced)} command(s)")
        except Exception as e:
            logging.error(f"Failed to sync commands: {e}")
        
        # Set bot status
        await bot.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching, 
                name="📝 TODO Lists | /help"
            )
        )
        
        logging.info("Bot initialization completed successfully")
        
    except Exception as e:
        logging.error(f"Error in on_ready: {e}")

async def daily_backup_task():
    """Daily database backup task"""
    while True:
        try:
            # Wait until 2 AM
            now = datetime.now()
            next_backup = now.replace(hour=2, minute=0, second=0, microsecond=0)
            if next_backup <= now:
                next_backup += timedelta(days=1)
            
            wait_seconds = (next_backup - now).total_seconds()
            await asyncio.sleep(wait_seconds)
            
            # Perform backup
            backup_path = await db.backup_database()
            if backup_path:
                logging.info(f"Daily backup completed: {backup_path}")
            
        except Exception as e:
            logging.error(f"Error in daily backup task: {e}")
            await asyncio.sleep(3600)  # Retry in 1 hour

# Added enhanced reminder task with better performance
async def reminder_task():
    """Enhanced reminder task with optimized queries"""
    while True:
        try:
            now = datetime.now()
            
            # Check for tasks due in the next hour (optimized query)
            upcoming_tasks = db.fetchall("""
                SELECT t.task_id, t.task, t.deadline, t.tags, t.description, t.owner_id,
                       c.emoji as category_emoji
                FROM tasks t
                LEFT JOIN categories c ON t.category_id = c.category_id
                WHERE t.status = 'Pending' 
                AND t.deadline IS NOT NULL
                AND datetime(t.deadline) BETWEEN datetime('now') AND datetime('now', '+1 hour')
                AND (t.last_reminder IS NULL OR datetime(t.last_reminder) < datetime('now', '-1 hour'))
            """)
            
            for task in upcoming_tasks:
                try:
                    # Send reminder to assigned users
                    user_id = task['owner_id']
                    user = bot.get_user(int(user_id))
                    if user:
                        embed = discord.Embed(
                            title=f"⏰ เตือนงานใกล้ครบกำหนด",
                            description=f"{task['category_emoji'] or '📝'} **{task['task']}**",
                            color=discord.Color.orange(),
                            timestamp=datetime.now()
                        )
                        embed.add_field(name="🆔 Task ID", value=f"`{task['task_id']}`", inline=True)
                        embed.add_field(name="⏰ กำหนดส่ง", value=task['deadline'], inline=True)
                        if task['tags']:
                            embed.add_field(name="🏷️ Tags", value=task['tags'], inline=False)
                        if task['description']:
                            embed.add_field(name="📝 Description", value=task['description'], inline=False)
                        
                        try:
                            await user.send(embed=embed)
                        except discord.Forbidden:
                            logging.warning(f"Cannot send DM to user {user_id}")
                    
                    # Update last reminder timestamp
                    db.execute("UPDATE tasks SET last_reminder = CURRENT_TIMESTAMP WHERE task_id = ?", (task['task_id'],))
                    
                except Exception as e:
                    logging.error(f"Error sending reminder for task {task['task_id']}: {e}")
            
            # Sleep for 30 minutes before next check
            await asyncio.sleep(1800)
            
        except Exception as e:
            logging.error(f"Error in reminder task: {e}")
            await asyncio.sleep(300)  # Retry in 5 minutes
            # ---------------- Additional Features & Migrations (placeholder replacement) ----------------

            # Synchronous lightweight schema migrations to add new columns if missing
            def migrate_schema_sync(db_path=DB_FILE):
                try:
                    conn = sqlite3.connect(db_path)
                    c = conn.cursor()

                    # Ensure tasks table has reminder_offset (minutes), notify_role_id
                    c.execute("PRAGMA table_info(tasks)")
                    task_cols = [r[1] for r in c.fetchall()]
                    if 'reminder_offset' not in task_cols:
                        try:
                            c.execute("ALTER TABLE tasks ADD COLUMN reminder_offset INTEGER")
                            logging.info("Added reminder_offset column to tasks table")
                        except Exception as e:
                            logging.warning(f"Could not add reminder_offset: {e}")
                    if 'notify_role_id' not in task_cols:
                        try:
                            c.execute("ALTER TABLE tasks ADD COLUMN notify_role_id INTEGER")
                            logging.info("Added notify_role_id column to tasks table")
                        except Exception as e:
                            logging.warning(f"Could not add notify_role_id: {e}")

                    # Ensure users table has completed_count for leaderboard/achievements
                    c.execute("PRAGMA table_info(users)")
                    user_cols = [r[1] for r in c.fetchall()]
                    if 'completed_count' not in user_cols:
                        try:
                            c.execute("ALTER TABLE users ADD COLUMN completed_count INTEGER DEFAULT 0")
                            logging.info("Added completed_count column to users table")
                        except Exception as e:
                            logging.warning(f"Could not add completed_count: {e}")

                    conn.commit()
                    conn.close()
                except Exception as e:
                    logging.error(f"Migration failed: {e}")

            # Run migrations immediately (safe, idempotent)
            try:
                migrate_schema_sync()
            except Exception as e:
                logging.error(f"Failed running migrations on import: {e}")

            # Helper: check if user can edit/manage a task (owner | assigned | admin)
            def has_edit_permission(user_id: str, task_id: int) -> bool:
                try:
                    # Owner
                    row = db.fetchone("SELECT owner_id FROM tasks WHERE task_id=?", (task_id,))
                    if not row:
                        return False
                    owner_id = row[0]
                    if user_id == owner_id:
                        return True
                    # Admin role
                    if get_role(user_id) == "admin":
                        return True
                    # Assigned user
                    assigned = db.fetchone("SELECT 1 FROM task_assignments WHERE task_id=? AND user_id=?", (task_id, user_id))
                    if assigned:
                        return True
                    return False
                except Exception as e:
                    logging.error(f"Error checking permissions for user {user_id} on task {task_id}: {e}")
                    return False

            # Override handle_task_action to allow assigned users to operate (owner/admin/assigned)
            async def handle_task_action(interaction, action, task_id, user_id):
                try:
                    # Check task exists
                    task_row = db.fetchone("SELECT owner_id, task FROM tasks WHERE task_id=?", (task_id,))
                    if not task_row:
                        await interaction.response.send_message("⚠️ ไม่พบงานนี้", ephemeral=True)
                        return

                    owner_id, task_name = task_row
                    # Permission check using new helper
                    if not has_edit_permission(user_id, task_id):
                        await interaction.response.send_message("⚠️ คุณไม่มีสิทธิ์ในการดำเนินการนี้", ephemeral=True)
                        return

                    if action == "done":
                        await complete_task(interaction, task_id, user_id)
                    elif action == "delete":
                        db.execute("UPDATE tasks SET status='Cancelled', updated_at=CURRENT_TIMESTAMP WHERE task_id=?", (task_id,))
                        await interaction.response.send_message("🗑 งานถูกยกเลิกเรียบร้อยแล้ว", ephemeral=True)

                        channel = interaction.channel
                        await send_public_notification(
                            channel,
                            f"🗑 <@{user_id}> ได้ยกเลิกงาน: `{task_name}`"
                        )

                    elif action == "edit":
                        await show_edit_modal(interaction, task_id, user_id)
                    elif action == "subtask":
                        await show_subtask_modal(interaction, task_id, user_id)

                except Exception as e:
                    logging.error(f"Error handling task action {action} for task {task_id}: {e}")
                    await interaction.response.send_message("❌ เกิดข้อผิดพลาด กรุณาลองใหม่", ephemeral=True)

            # Override complete_task to increment completed_count, award achievements, and notify roles
            async def complete_task(interaction, task_id, user_id):
                try:
                    row = db.fetchone("SELECT task, recurring, parent_task_id, owner_id FROM tasks WHERE task_id=?", (task_id,))
                    if not row:
                        await interaction.response.send_message("⚠️ ไม่พบงานนี้", ephemeral=True)
                        return

                    task_name, recurring, parent_task_id, owner_id = row
                    # Only allow allowed users
                    if not has_edit_permission(user_id, task_id):
                        await interaction.response.send_message("⚠️ คุณไม่มีสิทธิ์ในการทำเครื่องหมายว่าเสร็จ", ephemeral=True)
                        return

                    db.execute("UPDATE tasks SET status='Completed', updated_at=CURRENT_TIMESTAMP WHERE task_id=?", (task_id,))

                    # Update user's completed_count (if exists)
                    try:
                        db.execute("UPDATE users SET completed_count = COALESCE(completed_count, 0) + 1 WHERE user_id=?", (user_id,))
                        # Fetch new count
                        cnt_row = db.fetchone("SELECT completed_count FROM users WHERE user_id=?", (user_id,))
                        completed_count = cnt_row[0] if cnt_row else None
                    except Exception:
                        completed_count = None

                    await update_task_embed(task_id)

                    # If this is a subtask, possibly auto-complete parent (existing behavior)
                    embed = None
                    if parent_task_id:
                        total_subtasks, completed_subtasks, progress_percent = get_subtask_progress(parent_task_id)
                        if progress_percent == 100:
                            db.execute("UPDATE tasks SET status='Completed', updated_at=CURRENT_TIMESTAMP WHERE task_id=?", (parent_task_id,))
                            await update_task_embed(parent_task_id)
                            parent_row = db.fetchone("SELECT task FROM tasks WHERE task_id=?", (parent_task_id,))
                            parent_name = parent_row[0] if parent_row else "Unknown"

                            embed = discord.Embed(
                                title="🎉 งานและ Subtasks เสร็จสิ้น!",
                                description=f"<@{user_id}> ได้ทำ subtask **{task_name}** เสร็จ\n\n🎊 งานหลัก **{parent_name}** เสร็จสมบูรณ์แล้ว!",
                                color=discord.Color.gold()
                            )
                        else:
                            await update_task_embed(parent_task_id)
                            embed = discord.Embed(
                                title="🎉 Subtask เสร็จสิ้น!",
                                description=f"<@{user_id}> ได้ทำ subtask **{task_name}** เสร็จเรียบร้อยแล้ว\n\n📊 ความคืบหน้างานหลัก: {progress_percent:.0f}%",
                                color=discord.Color.green()
                            )
                    else:
                        embed = discord.Embed(
                            title="🎉 งานเสร็จสิ้น!",
                            description=f"<@{user_id}> ได้ทำงาน **{task_name}** เสร็จเรียบร้อยแล้ว",
                            color=discord.Color.green()
                        )

                    embed.set_footer(text=f"Task ID: {task_id}")
                    await interaction.response.send_message(embed=embed)

                    # DM user
                    try:
                        user = await bot.fetch_user(int(user_id))
                        await user.send(f"🎉 ยินดีด้วย! คุณทำงาน `{task_name}` เสร็จแล้ว")
                        # Notify about achievements if thresholds met
                        if completed_count:
                            # simple achievement example
                            if completed_count in (10, 25, 50):
                                ach_embed = discord.Embed(
                                    title="🏆 Achievement Unlocked!",
                                    description=f"คุณทำงานเสร็จครบ {completed_count} งานแล้ว! รางวัล: {'🏅' if completed_count==10 else '🥇' if completed_count==25 else '🏆'}",
                                    color=discord.Color.gold()
                                )
                                try:
                                    await user.send(embed=ach_embed)
                                except:
                                    logging.warning(f"Could not DM achievement to {user_id}")
                    except:
                        logging.warning(f"Cannot send DM to user {user_id}")

                    # Mention notify_role if set for this task
                    try:
                        trow = db.fetchone("SELECT notify_role_id FROM tasks WHERE task_id=?", (task_id,))
                        if trow and trow[0]:
                            role_id = trow[0]
                            # attempt to send mention in owner's channel if exists
                            channel_id = get_channel(owner_id)
                            channel = bot.get_channel(channel_id) if channel_id else interaction.channel
                            if channel:
                                await channel.send(f"<@&{role_id}> งาน `{task_name}` (ID:{task_id}) ถูกทำเสร็จโดย <@{user_id}>")
                    except Exception:
                        logging.warning(f"Could not notify role for task {task_id}")

                except Exception as e:
                    logging.error(f"Error completing task {task_id}: {e}")
                    try:
                        await interaction.response.send_message("❌ เกิดข้อผิดพลาด", ephemeral=True)
                    except:
                        pass

            # Command: set custom reminder offset (minutes before deadline)
            @bot.tree.command(name="setreminder", description="⏱ ตั้งการเตือนแบบกำหนดเอง (นาทีก่อนกำหนดส่ง)")
            async def setreminder(interaction: discord.Interaction, task_id: int, minutes: int):
                try:
                    user_id = str(interaction.user.id)
                    # permission: owner/admin/assigned
                    if not has_edit_permission(user_id, task_id):
                        await interaction.response.send_message("⚠️ คุณไม่มีสิทธิ์ในการตั้งเตือนสำหรับงานนี้", ephemeral=True)
                        return

                    minutes = max(0, min(60*24*365, int(minutes)))  # clamp to reasonable range
                    db.execute("UPDATE tasks SET reminder_offset=? WHERE task_id=?", (minutes, task_id))
                    await interaction.response.send_message(f"✅ ตั้งเตือน {minutes} นาทีก่อนกำหนดส่งสำหรับ Task ID {task_id}", ephemeral=True)
                except Exception as e:
                    logging.error(f"Error in setreminder: {e}")
                    await interaction.response.send_message("❌ เกิดข้อผิดพลาดในการตั้งเตือน", ephemeral=True)

            # Command: assign a role to be mentioned on completion/reminders for a task
            @bot.tree.command(name="assignrole", description="🔔 ตั้ง role ให้ถูกแจ้งเตือนสำหรับงาน (mention by ID)")
            async def assignrole(interaction: discord.Interaction, task_id: int, role: discord.Role):
                try:
                    user_id = str(interaction.user.id)
                    if not has_edit_permission(user_id, task_id):
                        await interaction.response.send_message("⚠️ คุณไม่มีสิทธิ์ในการตั้งค่า role สำหรับงานนี้", ephemeral=True)
                        return

                    db.execute("UPDATE tasks SET notify_role_id=? WHERE task_id=?", (role.id, task_id))
                    await interaction.response.send_message(f"✅ ตั้ง role {role.mention} ให้ถูกแจ้งเตือนสำหรับ Task ID {task_id}", ephemeral=True)
                except Exception as e:
                    logging.error(f"Error in assignrole: {e}")
                    await interaction.response.send_message("❌ เกิดข้อผิดพลาดในการตั้ง role", ephemeral=True)

            # Leaderboard: top users by completed_count (server-wide)
            @bot.tree.command(name="leaderboard", description="🏆 แสดงผู้ที่ทำงานเสร็จมากที่สุด")
            async def leaderboard(interaction: discord.Interaction, limit: int = 10):
                try:
                    limit = max(3, min(25, int(limit)))
                    rows = db.fetchall("SELECT user_id, COALESCE(completed_count,0) as cnt FROM users ORDER BY cnt DESC LIMIT ?", (limit,))
                    if not rows:
                        await interaction.response.send_message("ยังไม่มีข้อมูลคะแนน", ephemeral=True)
                        return

                    embed = discord.Embed(title="🏆 Leaderboard - Task Completions", color=discord.Color.gold())
                    rank = 1
                    for uid, cnt in rows:
                        # attempt to resolve member/display name
                        display = uid
                        try:
                            member = await bot.fetch_user(int(uid))
                            display = member.display_name if hasattr(member, "display_name") else str(member)
                        except:
                            pass
                        embed.add_field(name=f"#{rank} - {display}", value=f"✅ {cnt} งานเสร็จ", inline=False)
                        rank += 1

                    await interaction.response.send_message(embed=embed, ephemeral=False)
                except Exception as e:
                    logging.error(f"Error in leaderboard: {e}")
                    await interaction.response.send_message("❌ เกิดข้อผิดพลาด", ephemeral=True)

            # Export tasks to CSV and DM to user (simple export)
            @bot.tree.command(name="exporttasks", description="📤 ส่งรายการงานของคุณเป็น CSV ทาง DM")
            async def exporttasks(interaction: discord.Interaction, include_completed: bool = True):
                try:
                    user_id = str(interaction.user.id)
                    status_filter = "" if include_completed else " AND status!='Completed'"
                    rows = db.fetchall(f"SELECT task_id, task, deadline, priority, status, recurring, category_id, tags, description FROM tasks WHERE owner_id=? {status_filter} ORDER BY deadline ASC", (user_id,))
                    if not rows:
                        await interaction.response.send_message("ไม่พบงานที่จะส่งออก", ephemeral=True)
                        return

                    output = io.StringIO()
                    writer = csv.writer(output)
                    writer.writerow(["task_id","task","deadline","priority","status","recurring","category_id","tags","description"])
                    for r in rows:
                        writer.writerow([r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7] or "", r[8] or ""])
                    csv_data = output.getvalue().encode('utf-8')
                    output.close()

                    bio = io.BytesIO(csv_data)
                    bio.seek(0)
                    filename = f"tasks_export_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

                    try:
                        await interaction.user.send(content="📤 นี่คือไฟล์ CSV รายการงานของคุณ", file=discord.File(fp=bio, filename=filename))
                        await interaction.response.send_message("✅ ส่งไฟล์ CSV ทาง DM เรียบร้อยแล้ว", ephemeral=True)
                    except discord.Forbidden:
                        await interaction.response.send_message("❌ ไม่สามารถส่ง DM ให้คุณได้ โปรดเปิดการรับข้อความจากเซิร์ฟเวอร์นี้หรือให้บอทส่งที่ช่องนี้", ephemeral=True)
                    finally:
                        bio.close()

                except Exception as e:
                    logging.error(f"Error in exporttasks: {e}")
                    await interaction.response.send_message("❌ เกิดข้อผิดพลาดในการส่งออก", ephemeral=True)
                    
# ---------------- Run Bot ----------------
if __name__ == "__main__":
    
    # ✅ Start Flask WebServer
    threading.Thread(target=webserver, daemon=True).start()
    async def main():
        try:
            # Connect and initialize DB (async connect)
            await db.connect()
            db.init_db()

            # Start housekeeping/background tasks (safe to schedule)
            try:
                asyncio.create_task(cleanup_rate_limiter())
            except Exception as e:
                logging.warning(f"Could not start cleanup_rate_limiter: {e}")

            try:
                asyncio.create_task(daily_backup_task())
            except Exception as e:
                logging.warning(f"Could not start daily_backup_task: {e}")

            # Start reminder_task as background worker if not already started elsewhere
            try:
                asyncio.create_task(reminder_task())
            except Exception as e:
                logging.warning(f"Could not start reminder_task: {e}")

            # Start the Discord bot (awaitable)
            await bot.start(TOKEN)

        finally:
            # Ensure DB clean up on shutdown
            try:
                if db.conn:
                    db.conn.close()
                    logging.info("Database connection closed")
            except Exception as e:
                logging.error(f"Error closing DB connection: {e}")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.error(f"Failed to start bot: {e}")
    finally:
        logging.info("Shutdown complete")