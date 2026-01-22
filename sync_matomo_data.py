#!/usr/bin/env python3
"""
Matomo Data Synchronization Script

Synchronizes Matomo analytics tables from MariaDB to PostgreSQL.
Supports incremental updates based on timestamps.

Usage:
    python sync_matomo_data.py

Environment variables required:
    - MATOMO_SOURCE: 'local' or 'remote' (default: 'local')
    - MATOMO_HOST, MATOMO_PORT, MATOMO_DATABASE, MATOMO_USER, MATOMO_PASSWORD (for local)
    - REMOTE_SERVER_URL, REMOTE_SERVER_USER, REMOTE_SERVER_PASSWORD (for remote)
    - PostgreSQL variables (POSTGRES_* or TRANSFER_DESTINATION=remote)
"""

from src.migration.matomo_sync import run_matomo_sync

if __name__ == '__main__':
    print("""
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║           Matomo → PostgreSQL Data Synchronization            ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
    """)

    try:
        run_matomo_sync()
    except KeyboardInterrupt:
        print("\n\n⚠️  Synchronization interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Synchronization failed: {e}")
        raise
