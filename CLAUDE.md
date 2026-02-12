# DBWappizyToSmartDiet - Database Migration Tool

Python-based migration tool that transfers data from MongoDB to PostgreSQL for a diet/wellness application with complex relationships between users, quizzes, appointments, and coaching data.

## Quick Reference

**Main Scripts:**
- `python transfert_data.py` - Run full MongoDB → PostgreSQL migration
- `python sync_matomo_data.py` - Sync Matomo analytics (MariaDB → PostgreSQL)
- `python refresh_postgres_db.py` / `refresh_mongo_db.py` - Database refresh utilities
- `python check_db_differences.py` - Compare database structures

**Key Directories:**
- `config/schemas.yaml` - Schema definitions (source of truth)
- `src/migration/strategies/` - Custom import strategies by domain
- `src/connections/` - Database connection managers

## Core Architecture

### Data Transformation
- MongoDB collections → PostgreSQL tables with defined schemas
- ObjectId references → Foreign key relationships (VARCHAR)
- Nested documents → Normalized separate tables
- Arrays → Junction/relationship tables
- MongoDB dates → PostgreSQL TIMESTAMP/DATE

### Migration Strategies

**Four main patterns:**

1. **DirectTranslationStrategy** - Simple 1:1 document to row mapping
   - For: users, companies, ingredients, events, etc.
   - Uses: `ON CONFLICT (id) DO UPDATE` for upserts
   - Pattern: One document → One table row

2. **ArrayExtractionStrategy** - Array normalization for separate collections
   - For: menu.recipes[] → menu_recipes table (when stored as arrays)
   - Uses: `ON CONFLICT (unique_constraint) DO UPDATE`
   - Pattern: One document with array → Multiple table rows
   - **Limitation**: Cannot detect array item removals (orphaned data)

3. **DeleteAndInsertStrategy** - Complete array synchronization (base class)
   - For: Relationship tables where array items can be removed
   - Why: Upsert can't detect removals; DELETE+INSERT ensures exact mirror
   - Pattern: DELETE all relationships for changed parent, INSERT fresh data
   - **Limitation**: Inefficient for small changes (deletes/inserts ALL items)
   - Legacy: Older implementations still use this

4. **SmartDiffStrategy** - Intelligent diff-based optimization (RECOMMENDED)
   - For: Relationship tables with typical small incremental changes
   - Why: 50-100x faster than delete-and-insert for small changes
   - Pattern: Compute diff, apply only delta (INSERT new, DELETE removed)
   - **Auto-selects strategy**: Diff-based for ≤30% changes, delete-and-insert for >30%
   - Subclasses: UserEventsSmartStrategy, UsersTargetsSmartStrategy (recommended)

**Strategy Class Hierarchy:**
```
ImportStrategy (ABC)
├── DirectTranslationStrategy
│   └── UsersLogbookStrategy (custom filtering)
├── ArrayExtractionStrategy (⚠️ Can't detect removals)
├── DeleteAndInsertStrategy (Legacy - inefficient)
│   ├── UserEventsStrategy (legacy)
│   ├── UsersTargetsStrategy (legacy)
│   ├── CoachingReasonsStrategy (legacy)
│   ├── QuizzsLinksQuestionsStrategy
│   ├── UsersQuizzsLinksQuestionsStrategy
│   ├── UsersContentsReadsStrategy
│   ├── DaysContentsLinksStrategy
│   └── DaysLogbooksLinksStrategy
└── SmartDiffStrategy (✅ RECOMMENDED for relationships)
    ├── UserEventsSmartStrategy
    ├── UsersTargetsSmartStrategy
    └── CoachingReasonsSmartStrategy
```

**Performance Comparison:**

| Strategy | Small Change (2%) | Medium Change (30%) | Large Change (100%) |
|----------|------------------|---------------------|---------------------|
| ArrayExtractionStrategy | ⚠️ Orphaned data | ⚠️ Orphaned data | ⚠️ Orphaned data |
| DeleteAndInsertStrategy | 100 ops | 100 ops | 100 ops |
| SmartDiffStrategy | 2 ops (50x faster) | 30 ops (3x faster) | 100 ops (same) |

*Example: User has 50 items, modifies 1 item*

### Incremental Migration

**Date Filtering:**
- Uses `$gte` comparison: `{$or: [{'creation_date': {$gte: after_date}}, {'update_date': {$gte: after_date}}]}`
- Last migration timestamp: `MAX(GREATEST(created_at, updated_at))` from PostgreSQL
- Inclusive comparison ensures no records missed during migration execution

**Upsert Behavior:**
```sql
-- Tables with primary key
INSERT INTO users (...) VALUES (...)
ON CONFLICT (id) DO UPDATE SET ...

-- Tables with unique constraints
INSERT INTO user_events (...) VALUES (...)
ON CONFLICT (user_id, event_id) DO UPDATE SET ...
```

**Scenarios:**
- New record → Inserted
- Updated record → Updated via ON CONFLICT
- Unchanged record → Filtered out by date query
- Relationship changes → Full refresh via DELETE+INSERT

### Export Order & Dependencies

Migration follows strict dependency order (1-6):

1. **Base entities** (no dependencies): ingredients, appointment_types, companies, offers, categories, targets, recipes, quizzs, quizzs_questions, contents
2. **Core entities**: events, users, menus, quizzs_links_questions
3. **Relationships**: user_events, users_targets, messages, coachings, users_logbook, menu_recipes, users_quizzs, users_quizzs_questions, users_contents_reads
4. **Complex data**: appointments, periods, items, users_quizzs_links_questions
5. **Coaching days**: days
6. **Day relationships**: days_contents_links, days_logbooks_links

## Advanced Features

### Force Reimport

Configure per table in `config/schemas.yaml`:

**`force_reimport: true`** - Bypasses incremental logic, forces full reimport
- Use when: Re-syncing all data, testing, or after manual PostgreSQL changes

**`truncate_before_import: true`** - Clears table before import (requires force_reimport)
- Use when: Table structure changed, need clean slate
- Warning: Uses `TRUNCATE TABLE CASCADE` (irreversible)

```yaml
# Example: Clean slate migration
days_contents_links:
  columns: [...]
  force_reimport: true           # Temporary flag
  truncate_before_import: true   # Temporary flag
```

**Workflow:** Add flags → Run migration → Verify → Remove flags → Commit schema

### Global Date Threshold

Extends migration window backward across all tables (`.env` file):

```bash
GLOBAL_DATE_THRESHOLD=2024-01-01  # ISO 8601 format (YYYY-MM-DD)
```

**Date Logic Priority:**
1. `force_reimport=true` → Full reimport (bypasses dates)
2. Global threshold + table date → Uses earlier date (extends window backward)
3. Table date only → Normal incremental
4. Neither → Full migration (first run)

**Use Cases:**
- Recover lost data from specific date
- Re-sync after MongoDB corrections
- Limit historical data on first run

### Special Tables Reference

| Table | MongoDB Source | Key Feature | Strategy | Notes |
|-------|---------------|-------------|----------|-------|
| **users_targets** | `users.targets[]`<br>`users.specificity_targets[]`<br>`users.health_targets[]` | Multi-array consolidation with type discrimination | UsersTargetsSmartStrategy ✅ (SmartDiff)<br>UsersTargetsStrategy (Legacy) | Three arrays → one table with `type` column ('basic', 'specificity', 'health')<br>UNIQUE(user_id, target_id, type)<br>**Use smart version for 50x performance** |
| **coaching_reasons** | `coachings.reasons[]`<br>`coachings.health_reasons[]` | Multi-array consolidation with type discrimination | CoachingReasonsSmartStrategy ✅ (SmartDiff)<br>CoachingReasonsStrategy (Legacy) | Two arrays → one table with `type` column ('reason', 'health_reason')<br>Links coachings to targets<br>UNIQUE(coaching_id, target_id, type)<br>**Use smart version for optimal performance** |
| **user_events** | `users.registered_events[]` | Event registration tracking | UserEventsSmartStrategy ✅ (SmartDiff)<br>UserEventsStrategy (Legacy) | Handles registration AND unregistration<br>UNIQUE(user_id, event_id)<br>**Use smart version for 50x performance** |
| **users_logbook** | `coachinglogbooks` collection | Deduplication per user per day | UsersLogbookStrategy (DirectTranslation) | Auto-incremented id (not MongoDB _id)<br>Filters documents with `user` field<br>UNIQUE(user_id, day)<br>555,182 of 563,086 docs have user field |
| **periods** | `periods` collection | Coaching program phases | DirectTranslationStrategy | `number_of_days` → `days_numbers`<br>`coaching` → `coaching_id` |
| **days** | `days` collection | Individual coaching days | DirectTranslationStrategy | `is_success` → `completed`<br>`date` → `day`<br>`userQuizz` → `user_quizz_id` |
| **days_contents_links** | `days.contents[]` | Educational content per day | DaysContentsLinksStrategy (Delete+Insert) | UNIQUE(day_id, content_id) |
| **days_logbooks_links** | `days.main_logbooks[]` | Logbook entries per day | DaysLogbooksLinksStrategy (Delete+Insert) | UNIQUE(day_id, logbook_id) |

## Matomo Analytics Sync

Standalone sync process for Matomo analytics (MariaDB → PostgreSQL):

**Tables:**
- `matomo_log_visit` - Visitor sessions (PK: idvisit, sync: visit_last_action_time)
- `matomo_log_link_visit_action` - Page views (PK: idlink_va, sync: server_time)

**Sync Strategy:**
- Incremental based on timestamp columns
- `ON CONFLICT DO UPDATE` on primary keys
- Batch size: 5,000 rows
- Binary data conversion: BLOB → BYTEA

**Configuration:**
```bash
MATOMO_SOURCE=local|remote  # Default: local
MATOMO_HOST=localhost
MATOMO_PORT=3306
MATOMO_DATABASE=matomo
MATOMO_USER=root
MATOMO_PASSWORD=***
```

## Configuration

### Environment Variables (.env)

**Transfer Direction:**
```bash
TRANSFER_SOURCE=local|remote        # MongoDB source
TRANSFER_DESTINATION=local|remote   # PostgreSQL destination
```

**Local MongoDB:**
```bash
MONGODB_URL=mongodb://localhost:27017
MONGODB_DATABASE=your_db_name
```

**Local PostgreSQL:**
```bash
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=your_db_name
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
```

**Remote Access (SSH Tunnel):**
```bash
REMOTE_SERVER_URL=your_server_ip
REMOTE_SERVER_USER=ssh_user
REMOTE_SERVER_PASSWORD=ssh_password
REMOTE_MONGODB_URL=mongodb://localhost:27017
REMOTE_MONGODB_DATABASE=remote_db_name
REMOTE_POSTGRES_PORT=5432
REMOTE_POSTGRES_DATABASE=remote_db_name
REMOTE_POSTGRES_USER=remote_user
REMOTE_POSTGRES_PASSWORD=remote_password
```

**Matomo:**
```bash
MATOMO_SOURCE=local|remote
MATOMO_HOST=localhost
MATOMO_PORT=3306
MATOMO_DATABASE=matomo
MATOMO_USER=root
MATOMO_PASSWORD=matomo_password
```

**Optional:**
```bash
GLOBAL_DATE_THRESHOLD=2024-01-01  # Extend sync window backward
```

### Transfer Scenarios

| Source | Destination | Use Case |
|--------|-------------|----------|
| local | local | Development (default) |
| local | remote | Push to production |
| remote | local | Pull production data |
| remote | remote | Production migration |

## Common Development Tasks

### Adding New Entity Migration

1. **Define schema** in `config/schemas.yaml`:
```yaml
notifications:
  include_base: true  # Adds id, created_at, updated_at
  additional_columns:
    - name: user_id
      sql_type: VARCHAR
      nullable: false
      foreign_key: users(id)
      mongo_field: user  # MongoDB field name
    - name: title
      sql_type: VARCHAR(255)
    - name: read
      sql_type: BOOLEAN
      default: false
  export_order: 3  # After users (order 2)
```

2. **Run migration**: `python transfert_data.py`

3. **For complex cases**, create custom strategy in `src/migration/strategies/`:
```python
from src.migration.import_strategies import DirectTranslationStrategy

class NotificationsStrategy(DirectTranslationStrategy):
    def transform_document(self, doc):
        doc = super().transform_document(doc)
        # Custom transformations
        return doc
```

4. **Reference strategy** in schemas.yaml:
```yaml
notifications:
  # ... columns ...
  import_strategy: notifications  # Matches class name pattern
```

### Modifying Import Logic

- **Core export logic**: Edit `src/migration/data_export.py`
- **Custom strategies**: Add to `src/migration/strategies/` by domain:
  - `user_strategies.py` - User events, targets
  - `quiz_strategies.py` - Quiz relationships
  - `content_strategies.py` - Content reads
  - `coaching_strategies.py` - Coaching days, links

### Using SmartDiffStrategy for Relationship Tables

**When to use SmartDiffStrategy:**
- Relationship tables with MongoDB arrays (e.g., user_events, users_targets)
- Typical changes are small (user adds/removes 1-2 items)
- Need to handle both additions AND removals correctly
- Want optimal performance for incremental migrations

**Migrating from DeleteAndInsertStrategy to SmartDiffStrategy:**

1. **Update strategy factory** in `src/migration/strategies/user_strategies.py`:
```python
# Change from:
# from data_export import get_strategy
# strategy = create_user_events_strategy()  # Uses DeleteAndInsertStrategy

# To:
strategy = create_user_events_smart_strategy()  # Uses SmartDiffStrategy
```

2. **No schema changes needed** - SmartDiffStrategy works with existing table structures

3. **Configure threshold** (optional):
```python
class MySmartStrategy(SmartDiffStrategy):
    DIFF_THRESHOLD = 0.5  # Use diff-based for ≤50% changes (default: 0.3)
```

**Implementation Example:**

See strategy files for complete examples:
- `create_user_events_smart_strategy()` - Simple relationship (user_id, event_id) in `user_strategies.py`
- `create_users_targets_smart_strategy()` - Composite key (user_id, target_id, type) in `user_strategies.py`
- `create_coaching_reasons_smart_strategy()` - Composite key (coaching_id, target_id, type) in `coaching_strategies.py`

**Key methods to implement:**
```python
def extract_current_items(self, document) -> set:
    """Return set of tuples: {('child_id1',), ('child_id2',)}"""

def _item_to_sql_values(self, parent_id: str, item: tuple):
    """Convert tuple to (values, columns) for SQL INSERT"""

def get_child_column_name(self) -> str:
    """Return column name for child ID (e.g., 'event_id')"""

def get_parent_column_name(self) -> str:
    """Return column name for parent ID (e.g., 'user_id')"""

def get_additional_columns(self) -> list:
    """Optional: Return ['type'] for composite keys"""
```

### Troubleshooting

**Connection Issues:**
- Check `.env` file configuration
- Verify services running: `mongod --version`, `psql --version`
- For remote: Check SSH tunnel and firewall rules

**Foreign Key Violations:**
- Verify `export_order` in schemas.yaml
- Parent tables must migrate before children

**Memory Errors:**
- Reduce batch size in data_export.py
- Avoid force_reimport on very large tables

**Missing Records:**
- Check GLOBAL_DATE_THRESHOLD setting
- Review date filtering logic
- Check logs for filtered/skipped records

## Dependencies

```
dnspython==2.7.0
psycopg2==2.9.10
pymongo==4.11.1
PyMySQL==1.1.0
PyYAML==6.0.2
python-dotenv==1.1.1
sshtunnel==0.4.0
paramiko==3.4.0
```

## Testing

```bash
python -m pytest tests/
```
