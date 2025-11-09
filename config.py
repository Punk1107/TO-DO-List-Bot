import os
from typing import Dict, Any
import logging

def validate_environment():
    """Validate required environment variables"""
    required_vars = ['DISCORD_BOT_TOKEN']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Bot Configuration
BOT_TOKEN = os.getenv('DISCORD_BOT_TOKEN')
DATABASE_PATH = os.getenv('DATABASE_PATH', 'todo_bot.db')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

FEATURES = {
    'categories': True,          # Task categorization system
    'subtasks': True,           # Hierarchical task support
    'file_attachments': True,   # File attachment support
    'team_workspaces': True,    # Multi-user workspace support
    'time_tracking': True,      # Time tracking for tasks
    'templates': True,          # Task templates
    'analytics': True,          # Usage analytics and reporting
    'backup': True,            # Automated backup system
    'notifications': True,      # Push notifications
    'reminders': True,         # Automated reminders
    'api_integration': False,   # External API integrations (disabled by default)
    'advanced_search': True,   # Advanced search capabilities
    'export_import': True,     # Data export/import functionality
}

DB_CONFIG = {
    'pool_size': int(os.getenv('DB_POOL_SIZE', '10')),
    'timeout': int(os.getenv('DB_TIMEOUT', '30')),
    'check_same_thread': False,
    'journal_mode': 'WAL',      # Write-Ahead Logging for better performance
    'synchronous': 'NORMAL',    # Balance between safety and performance
    'cache_size': 10000,        # 10MB cache
    'temp_store': 'MEMORY',     # Store temporary tables in memory
    'mmap_size': 268435456,     # 256MB memory-mapped I/O
}

RATE_LIMITS = {
    'commands_per_minute': int(os.getenv('RATE_LIMIT_COMMANDS', '30')),
    'tasks_per_hour': int(os.getenv('RATE_LIMIT_TASKS', '100')),
    'searches_per_minute': int(os.getenv('RATE_LIMIT_SEARCHES', '10')),
    'exports_per_day': int(os.getenv('RATE_LIMIT_EXPORTS', '5')),
    'block_duration': int(os.getenv('RATE_LIMIT_BLOCK_DURATION', '300')),  # 5 minutes
}

NOTIFICATIONS = {
    'public_updates': os.getenv('NOTIFICATIONS_PUBLIC', 'true').lower() == 'true',
    'daily_summary': os.getenv('NOTIFICATIONS_DAILY', 'true').lower() == 'true',
    'weekly_reports': os.getenv('NOTIFICATIONS_WEEKLY', 'true').lower() == 'true',
    'deadline_reminders': os.getenv('NOTIFICATIONS_REMINDERS', 'true').lower() == 'true',
    'task_assignments': os.getenv('NOTIFICATIONS_ASSIGNMENTS', 'true').lower() == 'true',
    'system_alerts': os.getenv('NOTIFICATIONS_SYSTEM', 'true').lower() == 'true',
}

PERFORMANCE = {
    'enable_metrics': os.getenv('ENABLE_METRICS', 'true').lower() == 'true',
    'metrics_interval': int(os.getenv('METRICS_INTERVAL', '300')),  # 5 minutes
    'slow_query_threshold': float(os.getenv('SLOW_QUERY_THRESHOLD', '1.0')),  # 1 second
    'memory_warning_threshold': int(os.getenv('MEMORY_WARNING_MB', '500')),  # 500MB
    'enable_profiling': os.getenv('ENABLE_PROFILING', 'false').lower() == 'true',
}

SECURITY = {
    'max_task_length': int(os.getenv('MAX_TASK_LENGTH', '200')),
    'max_description_length': int(os.getenv('MAX_DESCRIPTION_LENGTH', '500')),
    'max_category_length': int(os.getenv('MAX_CATEGORY_LENGTH', '50')),
    'max_tags_length': int(os.getenv('MAX_TAGS_LENGTH', '200')),
    'max_tags_count': int(os.getenv('MAX_TAGS_COUNT', '10')),
    'enable_input_sanitization': os.getenv('ENABLE_INPUT_SANITIZATION', 'true').lower() == 'true',
    'enable_audit_logging': os.getenv('ENABLE_AUDIT_LOGGING', 'true').lower() == 'true',
    'max_file_size_mb': int(os.getenv('MAX_FILE_SIZE_MB', '10')),
}

BACKUP = {
    'enabled': os.getenv('BACKUP_ENABLED', 'true').lower() == 'true',
    'interval_hours': int(os.getenv('BACKUP_INTERVAL_HOURS', '24')),
    'retention_days': int(os.getenv('BACKUP_RETENTION_DAYS', '30')),
    'backup_path': os.getenv('BACKUP_PATH', './backups/'),
    'compress_backups': os.getenv('COMPRESS_BACKUPS', 'true').lower() == 'true',
}

LOGGING = {
    'level': LOG_LEVEL,
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file_path': os.getenv('LOG_FILE_PATH', 'bot.log'),
    'max_file_size_mb': int(os.getenv('LOG_MAX_FILE_SIZE_MB', '10')),
    'backup_count': int(os.getenv('LOG_BACKUP_COUNT', '5')),
    'enable_console': os.getenv('LOG_ENABLE_CONSOLE', 'true').lower() == 'true',
}

DISCORD = {
    'command_prefix': os.getenv('DISCORD_PREFIX', '/'),
    'embed_color': os.getenv('DISCORD_EMBED_COLOR', '#3498db'),
    'max_embed_fields': int(os.getenv('DISCORD_MAX_EMBED_FIELDS', '25')),
    'activity_type': os.getenv('DISCORD_ACTIVITY_TYPE', 'watching'),
    'activity_name': os.getenv('DISCORD_ACTIVITY_NAME', '📝 TODO Lists | /help'),
    'enable_slash_commands': os.getenv('DISCORD_ENABLE_SLASH', 'true').lower() == 'true',
}

DEBUG = {
    'enabled': os.getenv('DEBUG_ENABLED', 'false').lower() == 'true',
    'verbose_logging': os.getenv('DEBUG_VERBOSE', 'false').lower() == 'true',
    'enable_test_commands': os.getenv('DEBUG_TEST_COMMANDS', 'false').lower() == 'true',
    'mock_external_apis': os.getenv('DEBUG_MOCK_APIS', 'false').lower() == 'true',
}

def validate_config():
    """Validate configuration values"""
    errors = []
    
    # Validate rate limits
    if RATE_LIMITS['commands_per_minute'] <= 0:
        errors.append("commands_per_minute must be positive")
    
    if RATE_LIMITS['tasks_per_hour'] <= 0:
        errors.append("tasks_per_hour must be positive")
    
    # Validate database config
    if DB_CONFIG['pool_size'] <= 0:
        errors.append("db_pool_size must be positive")
    
    if DB_CONFIG['timeout'] <= 0:
        errors.append("db_timeout must be positive")
    
    # Validate security limits
    if SECURITY['max_task_length'] <= 0:
        errors.append("max_task_length must be positive")
    
    # Validate backup config
    if BACKUP['enabled'] and BACKUP['interval_hours'] <= 0:
        errors.append("backup_interval_hours must be positive when backup is enabled")
    
    if errors:
        raise ValueError(f"Configuration validation failed: {', '.join(errors)}")

def init_config():
    """Initialize and validate configuration"""
    try:
        validate_environment()
        validate_config()
        
        # Setup logging
        logging.basicConfig(
            level=getattr(logging, LOGGING['level'].upper()),
            format=LOGGING['format'],
            handlers=[
                logging.FileHandler(LOGGING['file_path']),
                logging.StreamHandler() if LOGGING['enable_console'] else logging.NullHandler()
            ]
        )
        
        logging.info("Configuration initialized successfully")
        logging.info(f"Features enabled: {[k for k, v in FEATURES.items() if v]}")
        
        return True
        
    except Exception as e:
        print(f"Configuration initialization failed: {e}")
        return False

def get_config_summary():
    """Get configuration summary for debugging"""
    return {
        'features': {k: v for k, v in FEATURES.items() if v},
        'rate_limits': RATE_LIMITS,
        'database': {k: v for k, v in DB_CONFIG.items() if k != 'password'},
        'security': SECURITY,
        'performance': PERFORMANCE,
        'backup': BACKUP if BACKUP['enabled'] else {'enabled': False},
    }

# Auto-initialize configuration when module is imported
if __name__ != "__main__":
    init_config()
