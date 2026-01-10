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
3. **Relationships**: user_events, users_targets, messages, coachings, menu_recipes
4. **Complex data**: appointments

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

#### DirectTranslationStrategy
Simple 1:1 mapping for basic entities:
- Collections like `users`, `companies`, `ingredients`
- Direct field mapping with type conversion
- Handles missing fields as NULL values
- Uses `ON CONFLICT DO UPDATE` for automatic upserts

#### ArrayExtractionStrategy
Complex array normalization for direct collections:
- `menu.recipes[]` → `menu_recipes` table (if stored as arrays)
- Uses `ON CONFLICT DO UPDATE` on unique constraints
- Updates timestamps when relationships already exist

#### CustomUserEventsStrategy
Array extraction with delete-and-insert pattern:
- `users.registered_events[]` → `user_events` table
- Uses incremental delete-and-insert pattern for changed users
- Handles event registration and unregistration correctly
- Unique constraint on (user_id, event_id)

#### CustomUsersTargetsStrategy
Multi-array extraction with type categorization:
- `users.targets[]` → `users_targets` table (type='basic')
- `users.specificity_targets[]` → `users_targets` table (type='specificity')
- `users.health_targets[]` → `users_targets` table (type='health')
- Uses incremental delete-and-insert pattern for changed users
- Unique constraint on (user_id, target_id, type)

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
| ArrayExtractionStrategy | `ON CONFLICT (unique_constraint) DO UPDATE` | Timestamps updated (for separate collections like menu_recipes) |
| CustomUserEventsStrategy | Delete + Insert (no conflict clause) | Full relationship refresh per user |
| CustomUsersTargetsStrategy | Delete + Insert (no conflict clause) | Full relationship refresh per user |

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

### Export Order & Dependencies
Migration follows strict dependency order:

1. **Order 1**: Base entities (no dependencies)
   - `ingredients`, `appointment_types`, `companies`, `offers`, `categories`, `targets`, `recipes`

2. **Order 2**: Core entities with simple foreign keys
   - `events`, `users`, `menus`

3. **Order 3**: Relationship tables and arrays
   - `user_events`, `users_targets`, `messages`, `coachings`, `menu_recipes`

4. **Order 4**: Complex dependent data
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