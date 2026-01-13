# DBWappizyToSmartDiet - Database Migration Tool

This project is a Python-based database migration tool that transfers data from MongoDB to PostgreSQL. It's designed to migrate a diet/wellness application database with complex relationships between users, quizzes, appointments, and coaching data.

## Project Structure

```
├── config/                    # Configuration files
├── logs/                      # Application logs
├── sql_exports/               # SQL export files
├── src/
│   ├── connections/
│   │   ├── mongo_connection.py    # MongoDB connection singleton
│   │   └── postgres_connection.py # PostgreSQL connection setup
│   ├── migration/
│   │   ├── data_export.py         # Core data export logic
│   │   ├── import_strategies.py   # Custom import strategies
│   │   └── import_summary.py      # Migration reporting
│   ├── schemas/
│   │   ├── schemas.py            # Table definitions and mappings
│   │   └── table_schemas.py      # Base schema classes
│   └── utils/                    # Utility modules
└── tests/                     # Test files
```

## Key Components

### Main Scripts
- `transfert_data.py` - Main migration script that processes all tables
- `refresh_mongo_db.py` - MongoDB database refresh utility
- `refresh_postgres_db.py` - PostgreSQL database refresh utility
- `check_db_differences.py` - Database comparison tool

### Database Schema
The project migrates the following entities (in order):
1. **Base entities**: ingredients, appointment_types, companies, offers, categories, targets, recipes
2. **Users & Events**: events, users, menus
3. **Relationships**: user_events, users_targets, messages, coachings, users_logbook, menu_recipes
4. **Logbooks moments**: users_logbooks_moments
5. **Logbooks moments details**: users_logbooks_moments_details
6. **Complex data**: appointments

### Special Features
- **Array Extraction**: Handles MongoDB arrays as separate PostgreSQL tables
- **Incremental Migration**: Only imports records created or updated after the last migration
- **Upsert Strategy**: Automatically updates existing records instead of skipping them
- **Foreign Key Management**: Maintains referential integrity during migration
- **Custom Strategies**: Specialized import logic for complex data structures
- **Multi-Array Consolidation**: Combines multiple MongoDB arrays into single PostgreSQL tables with type discrimination (e.g., users_targets)
- **Cross-Database JOINs**: Performs complex lookups across MongoDB and PostgreSQL during migration (e.g., users_logbooks_moments linking through coachinglogbooks)
- **Nested Document Traversal**: Follows multi-level references across collections to extract normalized data (e.g., userquizzs → userquizzquestions → quizzquestions → items)

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
1. **Get last migration date** (handled by transfert_data.py)
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
    └── UsersTargetsStrategy
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

### Users Logbooks Moments Feature

The `users_logbooks_moments` and `users_logbooks_moments_details` tables represent a comprehensive question-answer tracking system for user coaching logbooks. These tables capture quiz-based assessments that users complete as part of their coaching journey.

#### Data Flow Overview

The migration involves five MongoDB collections working together:
```
coachinglogbooks → logbook field → userquizzs
                                      ↓
                               questions[] array
                                      ↓
                            userquizzquestions
                                      ↓
                            quizzQuestion field → quizzquestions (title)
                            userQuizzQuestion ← items (text)
```

#### MongoDB Collections Structure

**1. coachinglogbooks**
```javascript
{
  _id: ObjectId("..."),
  day: ISODate("2024-03-06T23:00:00Z"),
  user: ObjectId("..."),
  logbook: ObjectId("...") // References userquizzs._id
}
```

**2. userquizzs** (User Quiz instances)
```javascript
{
  _id: ObjectId("..."),
  name: "Morning Check-in",  // Becomes 'type' in PostgreSQL
  questions: [ObjectId("..."), ObjectId("...")],  // Array of userquizzquestions
  creation_date: ISODate("2024-03-08T17:00:04.143Z"),
  update_date: ISODate("2024-03-08T17:00:04.143Z")
}
```

**3. userquizzquestions** (User's answer instances)
```javascript
{
  _id: ObjectId("..."),
  quizzQuestion: ObjectId("...") // References quizzquestions._id
}
```

**4. quizzquestions** (Question templates)
```javascript
{
  _id: ObjectId("..."),
  title: "How are you feeling today?", // Question text
  creation_date: ISODate("..."),
  update_date: ISODate("...")
}
```

**5. items** (User's answers)
```javascript
{
  _id: ObjectId("..."),
  text: "I feel great!",  // Answer text
  userQuizzQuestion: ObjectId("..."), // References userquizzquestions._id
  creation_date: ISODate("..."),
  update_date: ISODate("...")
}
```

#### PostgreSQL Structure

**users_logbooks_moments**
```sql
CREATE TABLE users_logbooks_moments (
  id VARCHAR PRIMARY KEY,                    -- From userquizzs._id
  user_logbook_id INTEGER NOT NULL,          -- FK to users_logbook.id
  type VARCHAR(255) NOT NULL,                -- From userquizzs.name
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  FOREIGN KEY (user_logbook_id) REFERENCES users_logbook(id)
);
```

**users_logbooks_moments_details**
```sql
CREATE TABLE users_logbooks_moments_details (
  id VARCHAR PRIMARY KEY,                      -- From items._id
  user_logbook_moment_id VARCHAR NOT NULL,     -- FK to users_logbooks_moments.id
  question TEXT NOT NULL,                      -- From quizzquestions.title
  answer TEXT NOT NULL,                        -- From items.text
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  FOREIGN KEY (user_logbook_moment_id) REFERENCES users_logbooks_moments(id)
);
```

#### Key Design Decisions

**users_logbooks_moments:**
- **Complex JOIN pattern**: Links `userquizzs` → `coachinglogbooks` → `users_logbook` via (user_id, day)
- **Handles deduplication**: Multiple coaching logbooks per day → one users_logbook row → multiple moments per day
- **Type discrimination**: Uses `userquizzs.name` as the moment type

**users_logbooks_moments_details:**
- **Multiple answers per question**: One question can have multiple answer rows (1-to-many relationship)
- **Flattened structure**: Normalizes nested MongoDB documents into flat relational rows
- **Question text denormalization**: Stores question text directly (from `quizzquestions.title`) for query efficiency

#### Custom Import Strategies

**Performance Optimization: Single-Pass Processing**

To avoid duplicate MongoDB queries, both tables are processed in a single pass:
- **UsersLogbooksMomentsStrategy** generates BOTH moments AND details data simultaneously
- Details are stored in a module-level cache (`_moments_details_batch_cache`)
- **UsersLogbooksMomentsDetailsStrategy** retrieves and inserts the pre-computed details
- This eliminates redundant lookups across 5 collections (userquizzs, userquizzquestions, quizzquestions, items, coachinglogbooks)

**UsersLogbooksMomentsStrategy** (extends `DirectTranslationStrategy`)

Process for each `userquizz` document:
1. **Filter validation**: Check if at least one question meets ALL criteria:
   - Question type is `QUIZZ_TYPE_LOGBOOK`
   - Has at least one non-empty answer (from `items.text` or inline fields)
2. Query `coachinglogbooks` collection where `logbook = userquizz._id`
3. Extract `user` and `day` from the matched coaching logbook
4. Execute PostgreSQL JOIN: `SELECT id FROM users_logbook WHERE user_id = ? AND day = ?`
5. Use the returned `users_logbook.id` as the foreign key
6. **Generate moment row** for users_logbooks_moments
7. **ALSO generate detail rows** for users_logbooks_moments_details (stored in cache)
8. Skip records where:
   - No valid questions found (filter validation fails)
   - No matching coaching logbook exists
   - No users_logbook entry exists

This strategy handles the complex scenario where:
- Multiple `coachinglogbooks` can exist for the same user+day (different logbooks)
- `users_logbook` deduplicates to one row per user+day
- Each `userquizz` becomes a separate moment linked to the deduplicated logbook entry
- **Only logbook-type questions with actual content are included**
- **Generates data for both tables in a single pass** (optimization)

**UsersLogbooksMomentsDetailsStrategy** (extends `ImportStrategy`)

This is a **stub strategy** that retrieves pre-computed details from the module-level cache:
1. Returns 0 from `count_total_documents()` (no documents to process)
2. Returns empty list from `get_documents()` (no iteration needed)
3. Overrides `export_data()` to:
   - Retrieve pre-computed details from `_moments_details_batch_cache`
   - Batch insert all details at once
   - Clear the cache after successful insertion

**Why this optimization?**
- Both strategies originally processed the same userquizzs documents
- Each required lookups across 5 MongoDB collections
- By combining the processing, we:
  - Reduce MongoDB queries by ~50%
  - Improve migration performance significantly
  - Maintain data consistency (same source documents for both tables)

#### Migration Behavior

**Full import:**
```
userquizzs documents → users_logbooks_moments rows (filtered by successful JOIN)
  ↓
questions[] arrays → users_logbooks_moments_details rows (one per answer/item)
```

**Incremental import:**
- Uses standard date filtering: `creation_date >= after_date OR update_date >= after_date`
- Updates existing records via `ON CONFLICT DO UPDATE`
- Re-processes all questions for updated userquizzs

**Data relationships:**
```
One users_logbook row (deduplicated by user+day)
  ↓
Multiple users_logbooks_moments (one per userquizz/logbook)
  ↓
Multiple users_logbooks_moments_details (one per answer)
```

**Example scenario:**
```
User X has 2 coaching logbooks on 2024-03-06:
- Logbook A (Morning Check-in with 3 questions, 5 answers total)
- Logbook B (Evening Reflection with 2 questions, 3 answers total)

PostgreSQL structure:
- users_logbook: 1 row (user_id=X, day=2024-03-06)
- users_logbooks_moments: 2 rows (one for A, one for B)
- users_logbooks_moments_details: 8 rows (5 for A + 3 for B)
```

#### Use Cases

**users_logbooks_moments:**
- Track different types of check-ins/assessments per day
- Identify which quiz templates users completed
- Analyze completion patterns by moment type
- Support multiple assessments per day without deduplication

**users_logbooks_moments_details:**
- Store and retrieve user responses to coaching questions
- Analyze sentiment and patterns in free-text answers
- Generate reports showing user progress over time
- Support multiple answers per question (e.g., multi-select responses)
- Full-text search across user responses

#### Important Notes

1. **Dependency chain**: Both tables require `users_logbook` to be populated first (export_order 3 → 4 → 5)
2. **Question type filtering**: Only questions with `type = 'QUIZZ_TYPE_LOGBOOK'` are included
3. **Answer validation**: Only items with non-empty text are migrated (empty strings and null values are excluded)
4. **Skipped records**: Moments are skipped when:
   - No valid QUIZZ_TYPE_LOGBOOK questions with non-empty answers exist
   - No matching users_logbook entry exists
   - No matching coachinglogbooks entry exists
5. **Question denormalization**: Question text is copied to details table for performance (avoids JOIN on every query)
6. **Multiple answers**: System correctly handles multiple items per question (common in checkbox-style questions)

### Export Order & Dependencies
Migration follows strict dependency order:

1. **Order 1**: Base entities (no dependencies)
   - `ingredients`, `appointment_types`, `companies`, `offers`, `categories`, `targets`, `recipes`

2. **Order 2**: Core entities with simple foreign keys
   - `events`, `users`, `menus`

3. **Order 3**: Relationship tables and arrays
   - `user_events`, `users_targets`, `messages`, `coachings`, `users_logbook`, `menu_recipes`

4. **Order 4**: Users logbooks moments (depends on users_logbook)
   - `users_logbooks_moments`

5. **Order 5**: Users logbooks moments details (depends on users_logbooks_moments)
   - `users_logbooks_moments_details`

6. **Order 6**: Complex dependent data
   - `appointments`

## Dependencies

```
dnspython==2.7.0
psycopg2==2.9.10
pymongo==4.11.1
python-dotenv==1.1.1
```

## Configuration

The project uses environment variables for database connections:
- `MONGODB_URL` - MongoDB connection string (default: mongodb://localhost:27017)
- `MONGODB_DATABASE` - MongoDB database name (default: default)
- PostgreSQL connection variables (configured in postgres_connection.py)

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
1. Define schema in `src/schemas/schemas.py`
2. Add to `create_schemas()` function with appropriate `export_order`
3. For complex data, create custom import strategy

### Modifying Import Logic
- Edit `src/migration/data_export.py` for core export logic
- Add custom strategies in `src/migration/import_strategies.py`

### Database Connection Issues
- Check environment variables in `.env` file
- Verify MongoDB and PostgreSQL services are running
- Review connection settings in `src/connections/`