# DBWappizyToSmartDiet - Database Migration Tool

This project is a Python-based database migration tool that transfers data from MongoDB to PostgreSQL. It's designed to migrate a diet/wellness application database with complex relationships between users, quizzes, appointments, and coaching data.

## Project Structure

```
├── config/                    # Configuration files
│   └── schemas.yaml           # YAML schema source of truth
├── logs/                      # Application logs
├── sql_exports/               # SQL export files
├── src/
│   ├── connections/
│   │   ├── mongo_connection.py    # MongoDB connection singleton
│   │   └── postgres_connection.py # PostgreSQL connection setup
│   ├── migration/
│   │   ├── data_export.py         # Core data export logic
│   │   ├── import_strategies.py   # Base strategy classes and utilities
│   │   ├── runner.py              # Migration orchestration entrypoint
│   │   ├── strategies/            # Strategy implementations by domain
│   │   │   ├── user_strategies.py     # User events and targets
│   │   │   ├── quiz_strategies.py     # Quiz relationships
│   │   │   ├── content_strategies.py  # Content read tracking
│   │   │   └── coaching_strategies.py # Coaching days and links
│   │   └── import_summary.py      # Migration reporting
│   ├── schemas/
│   │   ├── schemas.py            # YAML-driven schema loader
│   │   └── table_schemas.py      # Base schema classes
│   └── utils/                    # Utility modules
└── tests/                     # Test files
```

## Key Components

### Main Scripts
- `transfert_data.py` - CLI entrypoint that invokes the migration runner
- `src/migration/runner.py` - Main migration orchestration (iterates schemas, runs strategies)
- `refresh_mongo_db.py` - MongoDB database refresh utility
- `refresh_postgres_db.py` - PostgreSQL database refresh utility
- `check_db_differences.py` - Database comparison tool

### Database Schema
The project migrates the following entities (in order):
1. **Base entities**: ingredients, appointment_types, companies, offers, categories, targets, recipes, quizzs, quizzs_questions
2. **Users & Events**: events, users, menus, quizzs_links_questions
3. **Relationships**: user_events, users_targets, messages, coachings, users_logbook, menu_recipes, users_quizzs, users_quizzs_questions, items, contents, users_contents_reads
4. **Complex data**: appointments, periods, users_quizzs_links_questions
5. **Coaching days**: days
6. **Day relationships**: days_contents_links, days_logbooks_links

### Special Features
- **Array Extraction**: Handles MongoDB arrays as separate PostgreSQL tables
- **Incremental Migration**: Only imports records created or updated after the last migration
- **Upsert Strategy**: Automatically updates existing records instead of skipping them
- **Foreign Key Management**: Maintains referential integrity during migration
- **Custom Strategies**: Specialized import logic for complex data structures
- **Multi-Array Consolidation**: Combines multiple MongoDB arrays into single PostgreSQL tables with type discrimination (e.g., users_targets)

## Database Transformation Process

### Core Transformation
The system performs a complete structural transformation from MongoDB's document-based format to PostgreSQL's relational format:

- **MongoDB collections** → **PostgreSQL tables** with defined schemas
- **Document fields** → **Typed columns** (VARCHAR, INTEGER, TIMESTAMP, etc.)
- **ObjectId references** → **Foreign key relationships**
- **Nested documents** → **Normalized separate tables**

### Data Type Conversions
- `ObjectId` → `VARCHAR` (string representation)
- MongoDB dates → PostgreSQL `TIMESTAMP`/`DATE`
- Embedded documents → Foreign key references
- Arrays → Separate junction/relationship tables

### Migration Strategies

The system uses three main strategy types to handle different data migration patterns:

#### DirectTranslationStrategy
Simple 1:1 mapping for basic entities:
- Collections like `users`, `companies`, `ingredients`, `events`
- Direct field mapping with type conversion based on schema definitions
- Handles missing fields as NULL values
- Uses `ON CONFLICT DO UPDATE` for automatic upserts
- Pattern: One document → One table row

#### ArrayExtractionStrategy
Complex array normalization for direct collections:
- `menu.recipes[]` → `menu_recipes` table (if stored as arrays)
- Uses `ON CONFLICT DO UPDATE` on unique constraints
- Handles both embedded documents and ObjectId references in arrays
- Updates timestamps when relationships already exist
- Pattern: One document with array → Multiple table rows

#### DeleteAndInsertStrategy (Base Class)
Template method pattern for relationship tables requiring complete array synchronization:

**Architecture:**
- Base class implementing the 4-step migration pattern
- Template method `export_data()` handles common control flow
- Subclasses implement data extraction and configuration methods

**Why delete-and-insert pattern?**
   - When items are removed from MongoDB arrays, they disappear from the document
- Upsert strategies (ON CONFLICT DO UPDATE) can't detect removals
- Delete-and-insert ensures PostgreSQL perfectly mirrors MongoDB's current state
- Example: User unregisters from an event → removed from `registered_events[]` array
  - Without DELETE: orphaned relationship remains in PostgreSQL
  - With DELETE + INSERT: PostgreSQL reflects current state accurately

**The 4-step process:**
1. **Get last migration date** (handled by `src/migration/runner.py`)
   - Query PostgreSQL for latest timestamp
   - Returns None for full import, datetime for incremental

2. **Query changed documents** (implemented by subclasses)
   - Find parent documents with changes to array fields
   - Filter by creation/update dates using MongoDB $gte operator
   - Uses methods: `count_total_documents()`, `get_documents()`

3. **Extract relationship data** (implemented by subclasses)
   - Transform document arrays into SQL row data
   - Handle type discrimination (e.g., basic/specificity/health targets)
   - Uses method: `extract_data_for_sql()`

4. **Delete old + Insert fresh relationships** (handled by base class)
   - DELETE all relationships for changed parent documents
   - INSERT fresh relationships from current array state
   - No ON CONFLICT clause needed (fresh insert)
   - Uses method: `export_data()` (template method)

**Subclass implementations:**

**UserEventsStrategy** (extends DeleteAndInsertStrategy)
- Migrates: `users.registered_events[]` → `user_events` table
- Handles event registration and unregistration
- Supports both ObjectId and embedded document formats
- Unique constraint: (user_id, event_id)
- Example change: User adds/removes event from registered list
  - DELETE all user_events for that user_id
  - INSERT fresh relationships from current registered_events array

**UsersTargetsStrategy** (extends DeleteAndInsertStrategy)
- Migrates three arrays to one table with type discrimination:
  - `users.targets[]` → `users_targets` (type='basic')
  - `users.specificity_targets[]` → `users_targets` (type='specificity')
  - `users.health_targets[]` → `users_targets` (type='health')
- Unique constraint: (user_id, target_id, type)
- Example change: User modifies any of the three target arrays
  - DELETE all users_targets for that user_id
  - INSERT fresh relationships from all three arrays with appropriate types

**Strategy Class Hierarchy:**
```
ImportStrategy (ABC)
├── DirectTranslationStrategy
│   └── UsersLogbookStrategy (custom filtering)
├── ArrayExtractionStrategy
└── DeleteAndInsertStrategy (Base class for relationships)
    ├── UserEventsStrategy
    ├── UsersTargetsStrategy
    ├── QuizzsLinksQuestionsStrategy
    ├── UsersQuizzsLinksQuestionsStrategy
    ├── UsersContentsReadsStrategy
    ├── DaysContentsLinksStrategy
    └── DaysLogbooksLinksStrategy
```

**Benefits of this architecture:**
- Eliminates ~220 lines of code duplication
- Single source of truth for delete-and-insert logic
- Easy to add new relationship tables (just extend DeleteAndInsertStrategy)
- Template method pattern ensures consistent behavior
- Maintains full backward compatibility

### Incremental Migration & Update Strategy

The migration system supports incremental synchronization to efficiently handle large datasets and ongoing updates.

#### Date Filtering

The system uses `$gte` (greater than or equal to) comparison for incremental imports:

```python
# MongoDB query filter
{
    '$or': [
        {'creation_date': {'$gte': after_date}},
        {'update_date': {'$gte': after_date}}
    ]
}
```

- **Last migration timestamp**: Retrieved from PostgreSQL using `MAX(GREATEST(created_at, updated_at))`
- **Inclusive comparison**: Records created or updated exactly at the last migration timestamp are included
- **Rationale**: Ensures no records are missed even if created during migration execution

#### Upsert Behavior (ON CONFLICT DO UPDATE)

All import strategies use PostgreSQL's `ON CONFLICT` clause to handle duplicate records:

##### Tables with Primary Key (`id`)
```sql
INSERT INTO users (id, created_at, updated_at, firstname, lastname, email, ...)
VALUES (...)
ON CONFLICT (id) DO UPDATE SET
  created_at = EXCLUDED.created_at,
  updated_at = EXCLUDED.updated_at,
  firstname = EXCLUDED.firstname,
  lastname = EXCLUDED.lastname,
  email = EXCLUDED.email,
  ...
```

##### Tables with Unique Constraints
```sql
INSERT INTO user_events (user_id, event_id, created_at, updated_at)
VALUES (...)
ON CONFLICT (user_id, event_id) DO UPDATE SET
  created_at = EXCLUDED.created_at,
  updated_at = EXCLUDED.updated_at
```

#### Update Handling Examples

**Scenario 1: New Record**
- MongoDB: Record created after last migration
- PostgreSQL: Inserted normally
- Result: New record in PostgreSQL ✓

**Scenario 2: Updated Record**
- MongoDB: Record exists but was updated after last migration
- PostgreSQL: Record exists with older data
- Result: PostgreSQL record updated with latest data ✓

**Scenario 3: Unchanged Record**
- MongoDB: Record hasn't changed since last migration
- PostgreSQL: Record exists
- Result: Not fetched from MongoDB (filtered out by date query) ✓

**Scenario 4: Relationship Changes (users_targets)**
- MongoDB: User's target arrays modified
- PostgreSQL: Uses delete-and-insert pattern
- Result: All relationships for that user refreshed ✓

#### Strategy-Specific Behavior

| Strategy | Conflict Resolution | Update Handling |
|----------|-------------------|-----------------|
| DirectTranslationStrategy | `ON CONFLICT (id) DO UPDATE` | All columns updated except primary key |
| UsersLogbookStrategy | `ON CONFLICT (user_id, day) DO UPDATE` | Timestamps updated; filters documents with user field |
| ArrayExtractionStrategy | `ON CONFLICT (unique_constraint) DO UPDATE` | Timestamps updated (for separate collections like menu_recipes) |
| UserEventsStrategy | Delete + Insert (no conflict clause) | Full relationship refresh per user |
| UsersTargetsStrategy | Delete + Insert (no conflict clause) | Full relationship refresh per user |
| QuizzsLinksQuestionsStrategy | Delete + Insert (no conflict clause) | Full relationship refresh per quiz |
| UsersQuizzsLinksQuestionsStrategy | Delete + Insert (no conflict clause) | Full relationship refresh per user quiz |
| UsersContentsReadsStrategy | Delete + Insert (no conflict clause) | Full relationship refresh per content |
| DaysContentsLinksStrategy | Delete + Insert (no conflict clause) | Full relationship refresh per day |
| DaysLogbooksLinksStrategy | Delete + Insert (no conflict clause) | Full relationship refresh per day |

### Force Reimport Feature

The migration system supports forcing a complete reimport of specific tables when needed, particularly useful when table structures change or data needs to be completely refreshed.

#### Configuration Flags

Two boolean flags can be set per table in `config/schemas.yaml`:

**1. `force_reimport`** (boolean, default: `false`)
- When `true`, bypasses incremental migration logic
- Forces a full reimport of all data from MongoDB regardless of timestamps
- Ignores the last migration date from PostgreSQL
- Useful when you need to re-sync all data without changing the table structure

**2. `truncate_before_import`** (boolean, default: `false`)
- When `true`, truncates (clears) the PostgreSQL table before import
- Uses `TRUNCATE TABLE {table_name} CASCADE` to remove all existing data
- **Must be used with `force_reimport: true`** (only effective when force reimport is enabled)
- Useful when the table structure has changed and you need a clean slate

#### When to Use

**Use `force_reimport: true` when:**
- You need to re-sync all data from MongoDB to PostgreSQL
- Data in PostgreSQL may have been manually modified and needs to be restored
- You want to ensure PostgreSQL exactly matches MongoDB state
- Testing or debugging migration logic changes

**Use `truncate_before_import: true` when:**
- The table structure (columns, types, constraints) has changed
- You need to remove orphaned or incorrect data that can't be updated
- Starting fresh is cleaner than trying to merge old and new data
- The table has structural issues that prevent normal upsert operations

#### YAML Configuration Examples

**Example 1: Force reimport with upsert (preserve existing structure)**
```yaml
users:
  include_base: true
  additional_columns:
    - name: firstname
      sql_type: VARCHAR(255)
      nullable: false
    - name: lastname
      sql_type: VARCHAR(255)
      nullable: false
  export_order: 2
  force_reimport: true  # Force full reimport, but use upsert logic
```

**Example 2: Force reimport with truncate (clean slate)**
```yaml
days_contents_links:
  mongo_collection: days
  columns:
    - name: day_id
      sql_type: VARCHAR
      nullable: false
      foreign_key: days(id)
    - name: content_id
      sql_type: VARCHAR
      nullable: false
      foreign_key: contents(id)
  export_order: 6
  import_strategy: days_contents_links
  force_reimport: true           # Force full reimport
  truncate_before_import: true   # Clear table first
```

#### Migration Behavior

**Normal incremental migration:**
```
📅 Step 1: Last migration date: 2024-03-15 10:30:00
   → Will import records created or updated after this date
```

**With `force_reimport: true` only:**
```
🔄 FORCE REIMPORT enabled for this table
   → Will perform full reimport from MongoDB
```

**With `force_reimport: true` and `truncate_before_import: true`:**
```
🔄 FORCE REIMPORT enabled for this table
⚠️  TRUNCATE enabled - clearing all existing data
   → Table days_contents_links truncated successfully
   → Will perform full reimport from MongoDB
```

#### Important Notes

- **Temporary setting**: These flags should typically be set temporarily and removed after the forced reimport completes
- **CASCADE deletion**: `TRUNCATE TABLE ... CASCADE` will also truncate dependent tables if foreign keys exist
- **Performance**: Force reimport processes all MongoDB documents, which can be slow for large collections
- **No rollback**: Truncate operations cannot be rolled back in most PostgreSQL configurations
- **Testing recommended**: Test force reimport on development environment before running on production

#### Workflow Example

When you change a table structure:

1. **Update schema definition** in `config/schemas.yaml`:
   ```yaml
   days:
     include_base: true
     additional_columns:
       - name: new_column  # Added new column
         sql_type: VARCHAR(100)
     export_order: 5
     force_reimport: true           # Add this temporarily
     truncate_before_import: true   # Add this temporarily
   ```

2. **Run migration**:
   ```bash
   python transfert_data.py
   ```

3. **Verify results** and **remove flags**:
   ```yaml
   days:
     include_base: true
     additional_columns:
       - name: new_column
         sql_type: VARCHAR(100)
     export_order: 5
     # force_reimport: true           ← Remove after successful import
     # truncate_before_import: true   ← Remove after successful import
   ```

4. **Commit the schema changes** (without the force flags) to version control

### Users Targets Feature

The `users_targets` table represents a many-to-many relationship between users and their health/wellness targets. Unlike simple array extraction, this feature consolidates three different MongoDB arrays into a single normalized table.

#### MongoDB Structure
In MongoDB, users have three separate arrays for different target categories:
```javascript
{
  _id: ObjectId("..."),
  targets: [ObjectId("target1"), ObjectId("target2")],           // Basic targets
  specificity_targets: [ObjectId("target3")],                    // Specificity targets
  health_targets: [ObjectId("target4"), ObjectId("target5")]     // Health targets
}
```

#### PostgreSQL Structure
```sql
CREATE TABLE users_targets (
  user_id VARCHAR NOT NULL REFERENCES users(id),
  target_id VARCHAR NOT NULL REFERENCES targets(id),
  type VARCHAR(50) NOT NULL,  -- 'basic', 'specificity', or 'health'
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  UNIQUE(user_id, target_id, type)
);
```

#### Incremental Sync Behavior
For efficiency, the users_targets migration uses a delete-and-insert pattern:
1. **Query**: Find users with any target changes after `after_date`
2. **Delete**: Remove ALL existing relationships for changed users
3. **Insert**: Insert fresh relationships from all three arrays
4. **Benefit**: Handles target additions, removals, and type changes correctly

This approach ensures data consistency without complex diff logic while maintaining good performance through batch processing.

### User Events Feature

The `user_events` table represents a many-to-many relationship between users and events they are registered for.

#### MongoDB Structure
In MongoDB, users have an array of registered events:
```javascript
{
  _id: ObjectId("..."),
  registered_events: [
    ObjectId("event1"),
    ObjectId("event2"),
    ObjectId("event3")
  ]
}
```

#### PostgreSQL Structure
```sql
CREATE TABLE user_events (
  user_id VARCHAR NOT NULL REFERENCES users(id),
  event_id VARCHAR NOT NULL REFERENCES events(id),
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  UNIQUE(user_id, event_id)
);
```

#### Incremental Sync Behavior
The user_events migration uses the same delete-and-insert pattern as users_targets:
1. **Query**: Find users with changes to `registered_events` array after `after_date`
2. **Delete**: Remove ALL existing event relationships for changed users
3. **Insert**: Insert fresh relationships from the `registered_events` array
4. **Benefit**: Handles event registration AND unregistration correctly

**Why delete-and-insert?**
- When a user unregisters from an event, it's removed from the MongoDB array
- Without deletion, the old relationship would remain orphaned in PostgreSQL
- This pattern ensures PostgreSQL perfectly mirrors MongoDB's current state

### Users Logbook Feature

The `users_logbook` table represents a deduplicated tracking of coaching logbook entries per user per day. This table consolidates potentially multiple MongoDB documents for the same user-day combination into a single PostgreSQL row.

#### MongoDB Structure
In MongoDB, the `coachinglogbooks` collection has entries with:
```javascript
{
  _id: ObjectId("..."),
  day: ISODate("2024-03-06T23:00:00Z"),
  user: ObjectId("6418d6af3015567c5af862ee"),  // Some documents may not have this field
  logbook: ObjectId("65eb4414a1a7f677042c3a62"),
  coaching: ObjectId("64ef0616b99d86061670228a"),
  creation_date: ISODate("2024-03-08T17:00:04.143Z"),
  update_date: ISODate("2024-03-08T17:00:04.143Z")
}
```

**Important characteristics:**
- Not all documents have a `user` field (555,182 out of 563,086 documents have it)
- Multiple documents can exist for the same user+day combination in MongoDB
- The original MongoDB `_id` is NOT mapped to PostgreSQL (PostgreSQL generates its own sequential `id`)

#### PostgreSQL Structure
```sql
CREATE TABLE users_logbook (
  id SERIAL PRIMARY KEY,
  user_id VARCHAR NOT NULL REFERENCES users(id),
  day DATE NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  UNIQUE(user_id, day)
);
```

**Key design decisions:**
- **Auto-incremented `id` column**: PostgreSQL-generated sequential primary key (unlike other tables that use MongoDB's `_id`)
- **Unique constraint on (user_id, day)**: Ensures only one entry per user per day
- **Automatic deduplication**: Multiple MongoDB documents for same user+day → single PostgreSQL row

#### Custom Import Strategy

Uses a custom `UsersLogbookStrategy` (extends `DirectTranslationStrategy`):
- **Filtering**: Only processes documents where `user` field exists and is not null
- **Conflict resolution**: `ON CONFLICT (user_id, day) DO UPDATE SET created_at, updated_at`
- **Deduplication**: If multiple MongoDB documents have same (user, day), the last one processed updates the PostgreSQL row

#### Migration Behavior

**Full import:**
```
555,182 MongoDB documents (with user field) → PostgreSQL rows (deduplicated by user_id+day)
```

**Incremental import:**
- Filters documents by: `user IS NOT NULL AND (creation_date >= after_date OR update_date >= after_date)`
- Updates existing rows with latest timestamps
- Inserts new user-day combinations

**Example scenario:**
- MongoDB has 3 documents: user=X, day=2024-03-06 (created at different times)
- PostgreSQL will have 1 row: user=X, day=2024-03-06 (with latest timestamps)
- On subsequent migrations, if any of these 3 documents are updated, the PostgreSQL row updates

#### Use Case
This table provides a normalized view of which days each user has logbook entries, regardless of how many individual logbook records exist for that day. Useful for:
- Tracking user activity/engagement by day
- Identifying active coaching days
- Calculating user streaks or patterns
- Efficient queries for "days with entries" without counting duplicate logbook records

### Coaching Periods and Days Feature

The coaching system tracks periods within a coaching program and individual days within those periods. This feature includes three tables that work together to manage coaching progress.

#### Periods Table

Represents distinct phases or periods within a coaching program.

**MongoDB Structure (periods collection):**
```javascript
{
  _id: ObjectId("..."),
  number_of_days: 14,
  coaching: ObjectId("64ef0616b99d86061670228a"),
  order: 1,
  status: "active",
  creation_date: ISODate("2024-03-01T10:00:00Z"),
  update_date: ISODate("2024-03-01T10:00:00Z")
}
```

**PostgreSQL Structure:**
```sql
CREATE TABLE periods (
  id VARCHAR PRIMARY KEY,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  days_numbers SMALLINT NOT NULL,
  coaching_id VARCHAR REFERENCES coachings(id),
  order SMALLINT NOT NULL,
  status VARCHAR(100) NOT NULL
);
```

**Migration behavior:**
- Uses DirectTranslationStrategy (standard 1:1 mapping)
- `number_of_days` → `days_numbers` field mapping
- `coaching` → `coaching_id` foreign key reference
- Export order: 4 (after coachings at order 3)

#### Days Table

Represents individual days within coaching periods, tracking completion status and associated quizzes.

**MongoDB Structure (days collection):**
```javascript
{
  _id: ObjectId("..."),
  period: ObjectId("65e8f2a1b99d86061670234a"),
  is_success: true,
  date: ISODate("2024-03-06T00:00:00Z"),
  userQuizz: ObjectId("65eb4414a1a7f677042c3a62"),
  contents: [ObjectId("content1"), ObjectId("content2")],
  main_logbooks: [ObjectId("logbook1"), ObjectId("logbook2")],
  creation_date: ISODate("2024-03-06T08:00:00Z"),
  update_date: ISODate("2024-03-06T20:00:00Z")
}
```

**PostgreSQL Structure:**
```sql
CREATE TABLE days (
  id VARCHAR PRIMARY KEY,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  period_id VARCHAR NOT NULL REFERENCES periods(id),
  completed BOOLEAN NOT NULL,
  day DATE NOT NULL,
  user_quizz_id VARCHAR REFERENCES users_quizzs(id)
);
```

**Key design decisions:**
- `is_success` → `completed` (boolean field for day completion status)
- `date` → `day` (DATE type for the specific day)
- `period` → `period_id` foreign key to periods table
- `userQuizz` → `user_quizz_id` optional foreign key to users_quizzs
- Array fields (`contents`, `main_logbooks`) extracted to separate link tables

**Migration behavior:**
- Uses DirectTranslationStrategy with field mappings
- Export order: 5 (after periods at order 4)

#### Days Link Tables

Two relationship tables extract array fields from the days collection:

**1. days_contents_links**

Tracks which educational contents are associated with each coaching day.

**MongoDB Source:** `days.contents[]` array

**PostgreSQL Structure:**
```sql
CREATE TABLE days_contents_links (
  day_id VARCHAR NOT NULL REFERENCES days(id),
  content_id VARCHAR NOT NULL REFERENCES contents(id),
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  UNIQUE(day_id, content_id)
);
```

**2. days_logbooks_links**

Tracks which main logbooks (user quizzes) are linked to each coaching day.

**MongoDB Source:** `days.main_logbooks[]` array

**PostgreSQL Structure:**
```sql
CREATE TABLE days_logbooks_links (
  day_id VARCHAR NOT NULL REFERENCES days(id),
  logbook_id VARCHAR NOT NULL REFERENCES users_quizzs(id),
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  UNIQUE(day_id, logbook_id)
);
```

#### Custom Import Strategies

Both link tables use custom `DeleteAndInsertStrategy` implementations defined in `src/migration/strategies/coaching_strategies.py`:

**DaysContentsLinksStrategy:**
- Queries days documents with non-empty `contents` array
- Extracts day-content relationships
- Uses delete-and-insert pattern keyed on `day_id`
- Handles both ObjectId and embedded document formats

**DaysLogbooksLinksStrategy:**
- Queries days documents with non-empty `main_logbooks` array
- Extracts day-logbook relationships
- Uses delete-and-insert pattern keyed on `day_id`
- Handles both ObjectId and embedded document formats

#### Incremental Sync Behavior

**Days table:**
- Standard incremental sync based on creation_date/update_date
- Updates all fields when document changes

**Link tables:**
1. **Query**: Find days with array changes after `after_date`
2. **Delete**: Remove ALL existing links for changed days
3. **Insert**: Insert fresh relationships from current array state
4. **Benefit**: Correctly handles both additions and removals from arrays

**Why delete-and-insert for link tables?**
- When content/logbook is removed from a day's array in MongoDB, it disappears from the document
- Upsert strategies can't detect these removals
- Delete-and-insert ensures PostgreSQL perfectly mirrors MongoDB's current state

#### Use Cases

**Periods:**
- Track distinct phases in coaching programs (e.g., "Week 1", "Month 1")
- Organize coaching timeline into manageable segments
- Monitor period completion and status

**Days:**
- Track daily progress within coaching periods
- Record completion status for each day
- Link specific quizzes/assessments to individual days
- Provide granular coaching progress tracking

**Link tables:**
- Associate educational content with specific coaching days
- Link logbook entries/quizzes to days for tracking
- Enable queries like "what content was viewed on this day?"
- Support analytics on content engagement within coaching programs

### Export Order & Dependencies
Migration follows strict dependency order:

1. **Order 1**: Base entities (no dependencies)
   - `ingredients`, `appointment_types`, `companies`, `offers`, `categories`, `targets`, `recipes`, `quizzs`, `quizzs_questions`, `contents`

2. **Order 2**: Core entities with simple foreign keys
   - `events`, `users`, `menus`, `quizzs_links_questions`

3. **Order 3**: Relationship tables and core arrays
   - `user_events`, `users_targets`, `messages`, `coachings`, `users_logbook`, `menu_recipes`, `users_quizzs`, `users_quizzs_questions`, `users_contents_reads`

4. **Order 4**: Complex dependent data
   - `appointments`, `periods`, `items`, `users_quizzs_links_questions`

5. **Order 5**: Coaching days
   - `days`

6. **Order 6**: Day relationship arrays
   - `days_contents_links`, `days_logbooks_links`

## Dependencies

```
dnspython==2.7.0
psycopg2==2.9.10
pymongo==4.11.1
PyYAML==6.0.2
python-dotenv==1.1.1
sshtunnel==0.4.0
paramiko==3.4.0
```

## Configuration

### Transfer Mode Configuration

The migration tool supports flexible source and destination configuration through environment variables:

**Transfer Direction Settings:**
- `TRANSFER_SOURCE` - Where to read MongoDB data from: `"local"` (default) or `"remote"`
- `TRANSFER_DESTINATION` - Where to write PostgreSQL data to: `"local"` (default) or `"remote"`

**Common Transfer Scenarios:**

1. **Local → Local** (Default, Development)
   ```bash
   TRANSFER_SOURCE=local
   TRANSFER_DESTINATION=local
   ```
   - Reads from local MongoDB (MONGODB_URL)
   - Writes to local PostgreSQL (POSTGRES_*)

2. **Local → Remote** (Push local data to production)
   ```bash
   TRANSFER_SOURCE=local
   TRANSFER_DESTINATION=remote
   ```
   - Reads from local MongoDB
   - Writes to production PostgreSQL via SSH tunnel

3. **Remote → Local** (Pull production data locally)
   ```bash
   TRANSFER_SOURCE=remote
   TRANSFER_DESTINATION=local
   ```
   - Reads from production MongoDB via SSH tunnel
   - Writes to local PostgreSQL

4. **Remote → Remote** (Production migration)
   ```bash
   TRANSFER_SOURCE=remote
   TRANSFER_DESTINATION=remote
   ```
   - Reads from production MongoDB via SSH tunnel
   - Writes to production PostgreSQL via SSH tunnel

### SSH Tunnel Configuration

When using `remote` mode, the system automatically establishes SSH tunnels using these credentials:

**SSH Server:**
- `REMOTE_SERVER_URL` - Remote server IP/hostname
- `REMOTE_SERVER_USER` - SSH username
- `REMOTE_SERVER_PASSWORD` - SSH password

**Remote MongoDB (when TRANSFER_SOURCE=remote):**
- `REMOTE_MONGODB_URL` - MongoDB URL on remote server (typically mongodb://localhost:27017)
- `REMOTE_MONGODB_DATABASE` - Remote MongoDB database name

**Remote PostgreSQL (when TRANSFER_DESTINATION=remote):**
- `REMOTE_POSTGRES_PORT` - PostgreSQL port on remote server (default: 5432)
- `REMOTE_POSTGRES_DATABASE` - Remote PostgreSQL database name
- `REMOTE_POSTGRES_USER` - Remote PostgreSQL username
- `REMOTE_POSTGRES_PASSWORD` - Remote PostgreSQL password

### Local Database Configuration

**Local MongoDB:**
- `MONGODB_URL` - MongoDB connection string (default: mongodb://localhost:27017)
- `MONGODB_DATABASE` - MongoDB database name (default: default)

**Local PostgreSQL:**
- `POSTGRES_HOST` - PostgreSQL host (default: localhost)
- `POSTGRES_PORT` - PostgreSQL port (default: 5432)
- `POSTGRES_DATABASE` - PostgreSQL database name
- `POSTGRES_USER` - PostgreSQL username
- `POSTGRES_PASSWORD` - PostgreSQL password

## Usage

### Full Migration
```bash
python transfert_data.py
```

### Database Refresh
```bash
python refresh_mongo_db.py
python refresh_postgres_db.py
```

### Check Differences
```bash
python check_db_differences.py
```

## Testing
```bash
# Run tests from project root
python -m pytest tests/
```

## Common Development Tasks

### Adding New Entity Migration
1. Define schema in `config/schemas.yaml`
2. Set `export_order` and any `import_strategy` name
3. For complex data, create custom import strategy

### Modifying Import Logic
- Edit `src/migration/data_export.py` for core export logic
- Add custom strategies in `src/migration/strategies/` and reference them from schemas
- Strategy organization by domain:
  - `user_strategies.py` - User-focused strategies (user_events, users_targets)
  - `quiz_strategies.py` - Quiz-focused strategies (quizzs_links_questions, users_quizzs_links_questions)
  - `content_strategies.py` - Content-focused strategies (users_contents_reads)
  - `coaching_strategies.py` - Coaching-focused strategies (days_contents_links, days_logbooks_links)

### Database Connection Issues
- Check environment variables in `.env` file
- Verify MongoDB and PostgreSQL services are running
- Review connection settings in `src/connections/`
