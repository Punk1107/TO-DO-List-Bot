#!/usr/bin/env python3
"""
Database Migration Runner
Run this script to update your database schema before starting the bot
"""

import sys
import os

# Add the parent directory to the path so we can import database_migration
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database_migration import migrate_database

if __name__ == "__main__":
    print("🚀 Starting database migration...")
    success = migrate_database()
    
    if success:
        print("✅ Database migration completed successfully!")
        print("🎉 You can now run your Discord bot!")
    else:
        print("❌ Database migration failed!")
        print("Please check the migration.log file for details")
    
    exit(0 if success else 1)
