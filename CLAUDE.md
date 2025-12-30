# DBWappizyToSmartDiet - Database Migration Tool

This project is a Python-based database migration tool that transfers data from MongoDB to PostgreSQL. It's designed to migrate a diet/wellness application database with complex relationships between users, quizzes, appointments, and coaching data.

## Project Structure

```
src/
├── connections/
│   ├── mongo_connection.py    # MongoDB connection singleton
│   └── postgres_connection.py # PostgreSQL connection setup
├── migration/
│   ├── data_export.py         # Core data export logic
│   ├── import_strategies.py   # Custom import strategies
│   └── import_summary.py      # Migration reporting
├── schemas/
│   ├── schemas.py            # Table definitions and mappings
│   └── table_schemas.py      # Base schema classes
└── utils/
```

## Key Components

### Main Scripts
- `transfert_data.py` - Main migration script that processes all tables
- `refresh_mongo_db.py` - MongoDB database refresh utility
- `refresh_postgres_db.py` - PostgreSQL database refresh utility
- `check_db_differences.py` - Database comparison tool

### Database Schema
The project migrates the following entities (in order):
1. **Base entities**: ingredients, appointment_types, companies, offers, categories, targets
2. **Users & Events**: events, users, quizzs
3. **Relationships**: quizz_questions, user_events, user_quizzs, messages, coachings
4. **Complex data**: user_quizz_questions, appointments, coachings_logbooks, quizz_items

### Special Features
- **Array Extraction**: Handles MongoDB arrays as separate PostgreSQL tables
- **Incremental Migration**: Only imports records created after the last migration
- **Foreign Key Management**: Maintains referential integrity during migration
- **Custom Strategies**: Specialized import logic for complex data structures

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