# Database Migration Script - Enhanced version with better error handling and performance
import sqlite3
import os
import logging
import time
import shutil
import json
from datetime import datetime
from pathlib import Path

# Configuration management
CONFIG_FILE = "config.json"
DEFAULT_CONFIG = {
    "database": {
        "path": "todo_bot.db",
        "backup_dir": "backups",
        "timeout": 30,
        "journal_mode": "WAL"
    },
    "logging": {
        "level": "INFO",
        "format": "%(asctime)s - %(levelname)s - %(message)s",
        "file": "migration.log",
        "max_size": 5242880,  # 5MB
        "backup_count": 3
    }
}

def load_config():
    """Load configuration from file or create default"""
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                return {**DEFAULT_CONFIG, **json.load(f)}
        return DEFAULT_CONFIG
    except Exception as e:
        logging.error(f"Error loading config: {e}")
        return DEFAULT_CONFIG

# Load configuration
config = load_config()

# Database configuration
DB_FILE = os.getenv("DATABASE_PATH", config['database']['path'])

# Enhanced logging setup with rotation
from logging.handlers import RotatingFileHandler

logging.basicConfig(
    level=getattr(logging, config['logging']['level']),
    format=config['logging']['format'],
    handlers=[
        RotatingFileHandler(
            config['logging']['file'],
            maxBytes=config['logging']['max_size'],
            backupCount=config['logging']['backup_count']
        ),
        logging.StreamHandler()
    ]
)

class DatabaseMigrator:
    """Enhanced database migrator with rollback support, performance optimization, and improved error handling"""
    
    def __init__(self, db_file):
        self.db_file = db_file
        self.backup_file = None
        self.migration_start_time = None
        
    def create_backup(self):
        """Create backup before migration"""
        try:
            if os.path.exists(self.db_file):
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                self.backup_file = f"{self.db_file}.backup_pre_migration_{timestamp}"
                shutil.copy2(self.db_file, self.backup_file)
                logging.info(f"✅ Backup created: {self.backup_file}")
                return True
            else:
                logging.info("📝 No existing database found, creating new one")
                return True
        except Exception as e:
            logging.error(f"❌ Failed to create backup: {e}")
            return False
    
    def rollback(self):
        """Rollback to backup if migration fails"""
        try:
            if self.backup_file and os.path.exists(self.backup_file):
                if os.path.exists(self.db_file):
                    os.remove(self.db_file)
                shutil.copy2(self.backup_file, self.db_file)
                logging.info(f"🔄 Rolled back to backup: {self.backup_file}")
                return True
        except Exception as e:
            logging.error(f"❌ Rollback failed: {e}")
        return False
    
    def migrate_database(self):
        """Migrate existing database to support new features with enhanced error handling"""
        self.migration_start_time = time.time()
        
        try:
            # Create backup first
            if not self.create_backup():
                return False
            
            conn = sqlite3.connect(self.db_file, timeout=30)
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            c = conn.cursor()
            
            logging.info("🔄 Starting enhanced database migration...")
            
            c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='categories'")
            if not c.fetchone():
                logging.info("Creating enhanced categories table...")
                c.execute("""CREATE TABLE categories (
                    category_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    color TEXT DEFAULT '#3498db',
                    emoji TEXT DEFAULT '📝',
                    user_id TEXT NOT NULL,
                    description TEXT,
                    is_default BOOLEAN DEFAULT FALSE,
                    sort_order INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""")
                
                default_categories = [
                    ('งานทั่วไป', '#3498db', '📝', 'งานทั่วไปและงานประจำ', True, 1),
                    ('งานด่วน', '#e74c3c', '🚨', 'งานที่ต้องทำด่วนและสำคัญ', True, 2),
                    ('งานส่วนตัว', '#9b59b6', '👤', 'งานส่วนตัวและกิจกรรมส่วนตัว', True, 3),
                    ('งานบ้าน', '#f39c12', '🏠', 'งานบ้านและงานครัวเรือน', True, 4),
                    ('การเรียน', '#2ecc71', '📚', 'งานเรียนและการศึกษา', True, 5),
                    ('โปรเจค', '#1abc9c', '💼', 'โปรเจคและงานพิเศษ', True, 6),
                    ('สุขภาพ', '#e67e22', '💪', 'การออกกำลังกายและสุขภาพ', True, 7)
                ]
                
                for name, color, emoji, description, is_default, sort_order in default_categories:
                    c.execute("""INSERT INTO categories 
                                (name, color, emoji, user_id, description, is_default, sort_order) 
                                VALUES (?, ?, ?, 'default', ?, ?, ?)""", 
                             (name, color, emoji, description, is_default, sort_order))
                
                logging.info("✅ Enhanced categories table created with 7 default categories")
            
            c.execute("PRAGMA table_info(tasks)")
            columns = [column[1] for column in c.fetchall()]
            
            new_columns = [
                ('category_id', 'INTEGER REFERENCES categories(category_id)'),
                ('tags', 'TEXT'),
                ('description', 'TEXT'),
                ('estimated_hours', 'REAL DEFAULT 0'),
                ('actual_hours', 'REAL DEFAULT 0'),
                ('start_date', 'TIMESTAMP'),
                ('completion_date', 'TIMESTAMP'),
                ('last_reminder', 'TIMESTAMP'),
                ('reminder_count', 'INTEGER DEFAULT 0'),
                ('parent_task_id', 'INTEGER REFERENCES tasks(task_id)'),
                ('sort_order', 'INTEGER DEFAULT 0'),
                ('is_template', 'BOOLEAN DEFAULT FALSE'),
                ('template_name', 'TEXT'),
                ('updated_at', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
            ]
            
            for column_name, column_def in new_columns:
                if column_name not in columns:
                    logging.info(f"Adding {column_name} column to tasks table...")
                    c.execute(f"ALTER TABLE tasks ADD COLUMN {column_name} {column_def}")
                    logging.info(f"✅ Added {column_name} column")
            
            additional_tables = [
                ("subtasks", """CREATE TABLE subtasks (
                    subtask_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    parent_task_id INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    completed BOOLEAN DEFAULT FALSE,
                    sort_order INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (parent_task_id) REFERENCES tasks (task_id) ON DELETE CASCADE
                )"""),
                ("task_assignments", """CREATE TABLE task_assignments (
                    assignment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    user_id TEXT NOT NULL,
                    role TEXT DEFAULT 'assignee',
                    assigned_by TEXT,
                    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (task_id) REFERENCES tasks (task_id) ON DELETE CASCADE,
                    UNIQUE(task_id, user_id)
                )"""),
                ("task_comments", """CREATE TABLE task_comments (
                    comment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    user_id TEXT NOT NULL,
                    comment TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (task_id) REFERENCES tasks (task_id) ON DELETE CASCADE
                )"""),
                ("task_attachments", """CREATE TABLE task_attachments (
                    attachment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    filename TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    file_size INTEGER,
                    mime_type TEXT,
                    uploaded_by TEXT NOT NULL,
                    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (task_id) REFERENCES tasks (task_id) ON DELETE CASCADE
                )"""),
                ("user_preferences", """CREATE TABLE user_preferences (
                    user_id TEXT PRIMARY KEY,
                    timezone TEXT DEFAULT 'UTC',
                    date_format TEXT DEFAULT '%Y-%m-%d',
                    time_format TEXT DEFAULT '%H:%M',
                    reminder_enabled BOOLEAN DEFAULT TRUE,
                    reminder_minutes INTEGER DEFAULT 60,
                    daily_summary BOOLEAN DEFAULT TRUE,
                    weekly_report BOOLEAN DEFAULT TRUE,
                    theme TEXT DEFAULT 'default',
                    language TEXT DEFAULT 'th',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""")
            ]
            
            for table_name, table_sql in additional_tables:
                c.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                if not c.fetchone():
                    logging.info(f"Creating {table_name} table...")
                    c.execute(table_sql)
                    logging.info(f"✅ {table_name} table created")
            
            logging.info("Creating/updating performance indexes...")
            indexes = [
                ("idx_tasks_category", "CREATE INDEX IF NOT EXISTS idx_tasks_category ON tasks(category_id)"),
                ("idx_tasks_owner", "CREATE INDEX IF NOT EXISTS idx_tasks_owner ON tasks(owner_id)"),
                ("idx_tasks_deadline", "CREATE INDEX IF NOT EXISTS idx_tasks_deadline ON tasks(deadline)"),
                ("idx_tasks_completed", "CREATE INDEX IF NOT EXISTS idx_tasks_completed ON tasks(completed)"),
                ("idx_tasks_parent", "CREATE INDEX IF NOT EXISTS idx_tasks_parent ON tasks(parent_task_id)"),
                ("idx_subtasks_parent", "CREATE INDEX IF NOT EXISTS idx_subtasks_parent ON subtasks(parent_task_id)"),
                ("idx_assignments_task", "CREATE INDEX IF NOT EXISTS idx_assignments_task ON task_assignments(task_id)"),
                ("idx_assignments_user", "CREATE INDEX IF NOT EXISTS idx_assignments_user ON task_assignments(user_id)"),
                ("idx_comments_task", "CREATE INDEX IF NOT EXISTS idx_comments_task ON task_comments(task_id)"),
                ("idx_attachments_task", "CREATE INDEX IF NOT EXISTS idx_attachments_task ON task_attachments(task_id)"),
                ("idx_tasks_updated", "CREATE INDEX IF NOT EXISTS idx_tasks_updated ON tasks(updated_at)"),
                ("idx_categories_user", "CREATE INDEX IF NOT EXISTS idx_categories_user ON categories(user_id)"),
            ]
            
            for index_name, index_sql in indexes:
                try:
                    c.execute(index_sql)
                    logging.info(f"✅ Index {index_name} created/updated")
                except Exception as e:
                    logging.warning(f"⚠️ Index {index_name} creation warning: {e}")
            
            logging.info("Creating update triggers...")
            triggers = [
                """CREATE TRIGGER IF NOT EXISTS update_tasks_timestamp 
                   AFTER UPDATE ON tasks 
                   BEGIN 
                       UPDATE tasks SET updated_at = CURRENT_TIMESTAMP WHERE task_id = NEW.task_id;
                   END""",
                """CREATE TRIGGER IF NOT EXISTS update_categories_timestamp 
                   AFTER UPDATE ON categories 
                   BEGIN 
                       UPDATE categories SET updated_at = CURRENT_TIMESTAMP WHERE category_id = NEW.category_id;
                   END""",
                """CREATE TRIGGER IF NOT EXISTS update_subtasks_timestamp 
                   AFTER UPDATE ON subtasks 
                   BEGIN 
                       UPDATE subtasks SET updated_at = CURRENT_TIMESTAMP WHERE subtask_id = NEW.subtask_id;
                   END""",
                """CREATE TRIGGER IF NOT EXISTS update_comments_timestamp 
                   AFTER UPDATE ON task_comments 
                   BEGIN 
                       UPDATE task_comments SET updated_at = CURRENT_TIMESTAMP WHERE comment_id = NEW.comment_id;
                   END""",
                """CREATE TRIGGER IF NOT EXISTS update_preferences_timestamp 
                   AFTER UPDATE ON user_preferences 
                   BEGIN 
                       UPDATE user_preferences SET updated_at = CURRENT_TIMESTAMP WHERE user_id = NEW.user_id;
                   END"""
            ]
            
            for trigger_sql in triggers:
                try:
                    c.execute(trigger_sql)
                except Exception as e:
                    logging.warning(f"⚠️ Trigger creation warning: {e}")
            
            logging.info("✅ Update triggers created")
            
            logging.info("Optimizing database settings...")
            optimization_queries = [
                "PRAGMA optimize",
                "ANALYZE",
                "VACUUM"
            ]
            
            for query in optimization_queries:
                try:
                    c.execute(query)
                    logging.info(f"✅ Executed: {query}")
                except Exception as e:
                    logging.warning(f"⚠️ Optimization warning for {query}: {e}")
            
            conn.commit()
            
            migration_time = time.time() - self.migration_start_time
            
            table_counts = {}
            tables = ['tasks', 'categories', 'subtasks', 'task_assignments', 'task_comments', 'task_attachments', 'user_preferences']
            for table in tables:
                try:
                    count = c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    table_counts[table] = count
                except:
                    table_counts[table] = 'N/A'
            
            logging.info("✅ Enhanced database migration completed successfully!")
            logging.info(f"⏱️ Migration completed in {migration_time:.2f} seconds")
            logging.info("📊 Table statistics:")
            for table, count in table_counts.items():
                logging.info(f"   {table}: {count} records")
            logging.info("🚀 You can now run the main bot with enhanced features!")
            
            return True
            
        except Exception as e:
            logging.error(f"❌ Migration failed: {e}")
            logging.info("🔄 Attempting rollback...")
            if self.rollback():
                logging.info("✅ Rollback completed successfully")
            else:
                logging.error("❌ Rollback failed - manual intervention required")
            return False
            
        finally:
            if 'conn' in locals():
                conn.close()
    
    def verify_migration(self):
        """Verify migration was successful"""
        try:
            conn = sqlite3.connect(self.db_file, timeout=10)
            c = conn.cursor()
            
            # Check if all expected tables exist
            expected_tables = ['tasks', 'categories', 'subtasks', 'task_assignments', 'task_comments', 'task_attachments', 'user_preferences']
            
            c.execute("SELECT name FROM sqlite_master WHERE type='table'")
            existing_tables = [row[0] for row in c.fetchall()]
            
            missing_tables = [table for table in expected_tables if table not in existing_tables]
            
            if missing_tables:
                logging.error(f"❌ Migration verification failed - missing tables: {missing_tables}")
                return False
            
            # Check if indexes exist
            c.execute("SELECT name FROM sqlite_master WHERE type='index'")
            indexes = [row[0] for row in c.fetchall()]
            
            expected_indexes = ['idx_tasks_category', 'idx_tasks_owner', 'idx_tasks_deadline']
            missing_indexes = [idx for idx in expected_indexes if idx not in indexes]
            
            if missing_indexes:
                logging.warning(f"⚠️ Some indexes are missing: {missing_indexes}")
            
            logging.info("✅ Migration verification completed successfully")
            return True
            
        except Exception as e:
            logging.error(f"❌ Migration verification failed: {e}")
            return False
        finally:
            if 'conn' in locals():
                conn.close()

def migrate_database():
    """Main migration function with enhanced error handling"""
    migrator = DatabaseMigrator(DB_FILE)
    
    logging.info("🚀 Starting enhanced database migration process...")
    
    if migrator.migrate_database():
        if migrator.verify_migration():
            logging.info("🎉 Migration process completed successfully!")
            return True
        else:
            logging.error("❌ Migration verification failed")
            return False
    else:
        logging.error("❌ Migration process failed")
        return False

if __name__ == "__main__":
    success = migrate_database()
    exit(0 if success else 1)
