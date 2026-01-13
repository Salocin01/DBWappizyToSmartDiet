from .table_schemas import ColumnDefinition, BaseEntitySchema, TableSchema




def _create_user_events_strategy():
    """Create strategy for user_events array extraction with delete-and-insert pattern"""
    from src.migration.import_strategies import DeleteAndInsertStrategy, ImportConfig, ImportUtils

    class UserEventsStrategy(DeleteAndInsertStrategy):
        def count_total_documents(self, collection, config: ImportConfig) -> int:
            """Count users that have registered_events array"""
            mongo_filter = {'registered_events': {'$exists': True, '$ne': []}}
            mongo_filter.update(ImportUtils.build_date_filter(config.after_date))
            return collection.count_documents(mongo_filter)

        def get_documents(self, collection, config: ImportConfig, offset: int = 0):
            """Get user documents with registered_events array"""
            mongo_filter = {'registered_events': {'$exists': True, '$ne': []}}
            mongo_filter.update(ImportUtils.build_date_filter(config.after_date))

            return list(collection.find(
                mongo_filter,
                {'_id': 1, 'registered_events': 1, 'creation_date': 1, 'update_date': 1}
            ).skip(offset).limit(config.batch_size))

        def extract_data_for_sql(self, document, config: ImportConfig):
            """Extract all registered events from a user document"""
            user_id = str(document['_id'])
            creation_date = document.get('creation_date')
            update_date = document.get('update_date')

            batch_values = []

            # Extract registered events
            for event_item in document.get('registered_events', []):
                # Handle both ObjectId and embedded document formats
                if hasattr(event_item, 'get'):
                    # It's a document with potential fields
                    event_id = str(event_item.get('event', event_item.get('_id', event_item)))
                    event_date = event_item.get('date', creation_date)
                else:
                    # It's just an ObjectId
                    event_id = str(event_item)
                    event_date = creation_date

                batch_values.append([
                    user_id,
                    event_id,
                    event_date or creation_date,
                    update_date or event_date or creation_date
                ])

            return batch_values, ['user_id', 'event_id', 'created_at', 'updated_at']

        def get_parent_id_from_document(self, document) -> str:
            """Extract user_id from user document"""
            return str(document['_id'])

        def get_delete_table_name(self, config: ImportConfig) -> str:
            """Table to delete from is user_events"""
            return 'user_events'

        def get_delete_column_name(self) -> str:
            """Delete based on user_id column"""
            return 'user_id'

        def get_progress_message(self, processed: int, total: int, table_name: str, **kwargs) -> str:
            """Custom progress message for user events"""
            total_records = kwargs.get('total_records', 0)
            return f"Processed {processed}/{total} users, {total_records} user-event relationships"

    return UserEventsStrategy()


def _create_users_logbook_strategy():
    """Create strategy for users_logbook with user field filter"""
    from src.migration.import_strategies import DirectTranslationStrategy, ImportConfig, ImportUtils

    class UsersLogbookStrategy(DirectTranslationStrategy):
        def count_total_documents(self, collection, config: ImportConfig) -> int:
            """Count documents that have a user field (not None) and after 2026-01-05"""
            from datetime import datetime

            # Hardcoded minimum date filter
            min_date = datetime(2026, 1, 5, 0, 0, 0)

            mongo_filter = {
                'user': {'$exists': True, '$ne': None},
                '$or': [
                    {'creation_date': {'$gte': min_date}},
                    {'update_date': {'$gte': min_date}}
                ]
            }

            # Apply incremental filter if it's more restrictive
            date_filter = ImportUtils.build_date_filter(config.after_date)
            if date_filter and config.after_date and config.after_date > min_date:
                mongo_filter.update(date_filter)

            return collection.count_documents(mongo_filter)

        def get_documents(self, collection, config: ImportConfig, offset: int = 0):
            """Get documents with user field and after 2026-01-05"""
            from datetime import datetime

            # Hardcoded minimum date filter
            min_date = datetime(2026, 1, 5, 0, 0, 0)

            mongo_filter = {
                'user': {'$exists': True, '$ne': None},
                '$or': [
                    {'creation_date': {'$gte': min_date}},
                    {'update_date': {'$gte': min_date}}
                ]
            }

            # Apply incremental filter if it's more restrictive
            date_filter = ImportUtils.build_date_filter(config.after_date)
            if date_filter and config.after_date and config.after_date > min_date:
                mongo_filter.update(date_filter)

            return list(collection.find(mongo_filter).skip(offset).limit(config.batch_size))

    return UsersLogbookStrategy()


def _create_users_targets_strategy():
    """Create strategy for users_targets array extraction from multiple target fields"""
    from src.migration.import_strategies import DeleteAndInsertStrategy, ImportConfig, ImportUtils

    class UsersTargetsStrategy(DeleteAndInsertStrategy):
        def count_total_documents(self, collection, config: ImportConfig) -> int:
            """Count users that have any target arrays"""
            mongo_filter = {'$or': [
                {'targets': {'$exists': True, '$ne': []}},
                {'specificity_targets': {'$exists': True, '$ne': []}},
                {'health_targets': {'$exists': True, '$ne': []}}
            ]}
            mongo_filter.update(ImportUtils.build_date_filter(config.after_date))
            return collection.count_documents(mongo_filter)

        def get_documents(self, collection, config: ImportConfig, offset: int = 0):
            """Get user documents with target arrays"""
            mongo_filter = {'$or': [
                {'targets': {'$exists': True, '$ne': []}},
                {'specificity_targets': {'$exists': True, '$ne': []}},
                {'health_targets': {'$exists': True, '$ne': []}}
            ]}
            mongo_filter.update(ImportUtils.build_date_filter(config.after_date))

            return list(collection.find(
                mongo_filter,
                {'_id': 1, 'targets': 1, 'specificity_targets': 1, 'health_targets': 1, 'creation_date': 1, 'update_date': 1}
            ).skip(offset).limit(config.batch_size))

        def extract_data_for_sql(self, document, config: ImportConfig):
            """Extract all target relationships from a user document"""
            user_id = str(document['_id'])
            creation_date = document.get('creation_date')
            update_date = document.get('update_date')

            batch_values = []

            # Extract basic targets
            for target_id in document.get('targets', []):
                batch_values.append([
                    user_id,
                    str(target_id),
                    'basic',
                    creation_date,
                    update_date
                ])

            # Extract specificity targets
            for target_id in document.get('specificity_targets', []):
                batch_values.append([
                    user_id,
                    str(target_id),
                    'specificity',
                    creation_date,
                    update_date
                ])

            # Extract health targets
            for target_id in document.get('health_targets', []):
                batch_values.append([
                    user_id,
                    str(target_id),
                    'health',
                    creation_date,
                    update_date
                ])

            return batch_values, ['user_id', 'target_id', 'type', 'created_at', 'updated_at']

        def get_parent_id_from_document(self, document) -> str:
            """Extract user_id from user document"""
            return str(document['_id'])

        def get_delete_table_name(self, config: ImportConfig) -> str:
            """Table to delete from is users_targets"""
            return 'users_targets'

        def get_delete_column_name(self) -> str:
            """Delete based on user_id column"""
            return 'user_id'

        def get_progress_message(self, processed: int, total: int, table_name: str, **kwargs) -> str:
            """Custom progress message for user targets"""
            total_records = kwargs.get('total_records', 0)
            return f"Processed {processed}/{total} users, {total_records} user-target relationships"

    return UsersTargetsStrategy()


# Module-level storage for sharing batch data between moments and details strategies
_moments_details_batch_cache = []

def _create_users_logbooks_moments_strategy():
    """Create strategy for users_logbooks_moments linking userquizzs to users_logbooks"""
    from src.migration.import_strategies import DirectTranslationStrategy, ImportConfig, ImportUtils
    from src.connections.mongo_connection import get_mongo_collection
    import psycopg2

    class UsersLogbooksMomentsStrategy(DirectTranslationStrategy):
        def count_total_documents(self, collection, config: ImportConfig) -> int:
            """Count userquizzs with type='QUIZZ_TYPE_LOGBOOK', questions array, and after 2026-01-05

            Note: Additional filtering for answers happens in extract_data_for_sql.
            """
            from datetime import datetime

            # Clear the cache at the start of processing
            global _moments_details_batch_cache
            _moments_details_batch_cache = []

            # Hardcoded minimum date filter
            min_date = datetime(2026, 1, 5, 0, 0, 0)

            mongo_filter = {
                'type': 'QUIZZ_TYPE_LOGBOOK',
                'questions': {'$exists': True, '$ne': []},
                '$or': [
                    {'creation_date': {'$gte': min_date}},
                    {'update_date': {'$gte': min_date}}
                ]
            }

            # Apply incremental filter if it's more restrictive
            date_filter = ImportUtils.build_date_filter(config.after_date)
            if date_filter and config.after_date and config.after_date > min_date:
                mongo_filter.update(date_filter)

            return collection.count_documents(mongo_filter)

        def get_documents(self, collection, config: ImportConfig, offset: int = 0):
            """Get userquizzs with type='QUIZZ_TYPE_LOGBOOK', questions array, and after 2026-01-05

            Note: Additional filtering for answers happens in extract_data_for_sql.
            """
            from datetime import datetime

            # Hardcoded minimum date filter
            min_date = datetime(2026, 1, 5, 0, 0, 0)

            mongo_filter = {
                'type': 'QUIZZ_TYPE_LOGBOOK',
                'questions': {'$exists': True, '$ne': []},
                '$or': [
                    {'creation_date': {'$gte': min_date}},
                    {'update_date': {'$gte': min_date}}
                ]
            }

            # Apply incremental filter if it's more restrictive
            date_filter = ImportUtils.build_date_filter(config.after_date)
            if date_filter and config.after_date and config.after_date > min_date:
                mongo_filter.update(date_filter)

            return list(collection.find(mongo_filter).skip(offset).limit(config.batch_size))

        def extract_data_for_sql(self, document, config: ImportConfig):
            """Extract userquizz data and find matching users_logbook via coachinglogbooks

            Note: MongoDB query already filtered for type='QUIZZ_TYPE_LOGBOOK'.
            Just need to validate non-empty items exist.
            """
            from src.connections.postgres_connection import connect_postgres

            userquizz_id = str(document['_id'])
            userquizz_name = document.get('name')
            creation_date = document.get('creation_date')
            update_date = document.get('update_date')
            questions = document.get('questions', [])

            # Verify we have questions (should already be filtered by MongoDB query)
            if not questions:
                if not hasattr(config, '_skip_count'):
                    config._skip_count = {}
                config._skip_count['no_questions'] = config._skip_count.get('no_questions', 0) + 1
                return None, None

            # Check if at least one question has an answer
            # Answers can be stored in TWO places:
            # 1. items collection via items.userQuizzQuestion
            # 2. Inline in userquizzquestions fields (qcm_answers, single_enum_answer, etc.)

            items_collection = get_mongo_collection('items')
            userquizzquestions_collection = get_mongo_collection('userquizzquestions')

            # Check items collection first
            items_count = items_collection.count_documents({
                'userQuizzQuestion': {'$in': questions},
                'text': {'$exists': True, '$ne': '', '$ne': None}
            })

            # If no items, check inline answers in userquizzquestions
            inline_answers_count = 0
            if items_count == 0:
                inline_answers_count = userquizzquestions_collection.count_documents({
                    '_id': {'$in': questions},
                    '$or': [
                        {'qcm_answers': {'$exists': True, '$ne': []}},
                        {'scale_1_10_answer': {'$exists': True, '$ne': None}},
                        {'single_enum_answer': {'$exists': True, '$ne': None}},
                        {'boolean_answer': {'$exists': True, '$ne': None}},
                        {'document_answer': {'$exists': True, '$ne': None}}
                    ]
                })

            # If neither items nor inline answers exist, skip
            if items_count == 0 and inline_answers_count == 0:
                if not hasattr(config, '_skip_count'):
                    config._skip_count = {}
                config._skip_count['no_answers'] = config._skip_count.get('no_answers', 0) + 1
                return None, None

            # Find coachinglogbooks that reference this userquizz
            coachinglogbooks_collection = get_mongo_collection('coachinglogbooks')
            coaching_logbook = coachinglogbooks_collection.find_one(
                {'logbook': document['_id']},
                {'user': 1, 'day': 1}
            )

            if not coaching_logbook:
                if not hasattr(config, '_skip_count'):
                    config._skip_count = {}
                config._skip_count['no_coachinglogbook'] = config._skip_count.get('no_coachinglogbook', 0) + 1
                return None, None

            user_id = coaching_logbook.get('user')
            day = coaching_logbook.get('day')

            if not user_id or not day:
                if not hasattr(config, '_skip_count'):
                    config._skip_count = {}
                config._skip_count['no_user_or_day'] = config._skip_count.get('no_user_or_day', 0) + 1
                return None, None

            # Query PostgreSQL to find matching users_logbook.id
            # Note: day might be a datetime, convert to date for comparison
            if hasattr(day, 'date'):
                day_date = day.date()
            else:
                day_date = day

            conn = connect_postgres()
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT id FROM users_logbook WHERE user_id = %s AND day = %s",
                    (str(user_id), day_date)
                )
                result = cursor.fetchone()
                if not result:
                    if not hasattr(config, '_skip_count'):
                        config._skip_count = {}
                    config._skip_count['no_users_logbook_match'] = config._skip_count.get('no_users_logbook_match', 0) + 1
                    return None, None

                user_logbook_id = result[0]
            finally:
                cursor.close()
                conn.close()

            # Generate moment row
            moment_values = [
                userquizz_id,
                user_logbook_id,
                userquizz_name,
                creation_date,
                update_date
            ]
            moment_columns = ['id', 'user_logbook_id', 'type', 'created_at', 'updated_at']

            # ALSO generate details rows for this moment (to avoid duplicate processing)
            details_rows = self._extract_details_for_moment(
                userquizz_id, questions, items_collection, userquizzquestions_collection
            )

            # Store details in module-level cache for later batch insert
            global _moments_details_batch_cache
            _moments_details_batch_cache.extend(details_rows)

            return moment_values, moment_columns

        def _extract_details_for_moment(self, userquizz_id, questions, items_collection, userquizzquestions_collection):
            """Extract all question-answer details for this moment"""
            quizzquestions_collection = get_mongo_collection('quizzquestions')
            details_rows = []

            for question_id in questions:
                # Get userquizzquestion
                userquizzquestion = userquizzquestions_collection.find_one({'_id': question_id})
                if not userquizzquestion:
                    continue

                # Get template question
                template_question_id = userquizzquestion.get('quizz_question')
                if not template_question_id:
                    continue

                quizzquestion = quizzquestions_collection.find_one({'_id': template_question_id})
                if not quizzquestion or quizzquestion.get('type') != 'QUIZZ_TYPE_LOGBOOK':
                    continue

                question_title = quizzquestion.get('title', '')

                # Check items collection
                items = items_collection.find({
                    'userQuizzQuestion': question_id,
                    'text': {'$exists': True, '$ne': '', '$ne': None}
                })

                items_found = False
                for item in items:
                    items_found = True
                    details_rows.append([
                        str(item['_id']),
                        userquizz_id,
                        question_title,
                        item.get('text', ''),
                        item.get('creation_date'),
                        item.get('update_date')
                    ])

                # If no items, check inline answers
                if not items_found:
                    answer_text = None
                    if userquizzquestion.get('qcm_answers') and len(userquizzquestion.get('qcm_answers', [])) > 0:
                        answer_text = ', '.join([str(a) for a in userquizzquestion['qcm_answers']])
                    elif userquizzquestion.get('scale_1_10_answer') is not None:
                        answer_text = str(userquizzquestion['scale_1_10_answer'])
                    elif userquizzquestion.get('single_enum_answer') is not None:
                        answer_text = str(userquizzquestion['single_enum_answer'])
                    elif userquizzquestion.get('boolean_answer') is not None:
                        answer_text = str(userquizzquestion['boolean_answer'])
                    elif userquizzquestion.get('document_answer') is not None:
                        answer_text = str(userquizzquestion['document_answer'])

                    if answer_text:
                        details_rows.append([
                            str(userquizzquestion['_id']),
                            userquizz_id,
                            question_title,
                            answer_text,
                            userquizzquestion.get('creation_date'),
                            userquizzquestion.get('update_date')
                        ])

            return details_rows

    return UsersLogbooksMomentsStrategy()


def _create_users_logbooks_moments_details_strategy():
    """Create strategy for users_logbooks_moments_details extracting question-answer pairs

    NOTE: This strategy uses pre-computed data from UsersLogbooksMomentsStrategy
    to avoid duplicate processing. The moments strategy generates both moments
    and details in a single pass and stores details in config._moments_details_batch.
    """
    from src.migration.import_strategies import ImportStrategy, ImportConfig, ImportUtils
    from src.connections.mongo_connection import get_mongo_collection

    class UsersLogbooksMomentsDetailsStrategy(ImportStrategy):
        """
        Stub strategy that inserts pre-computed details from config._moments_details_batch.
        The actual data extraction is done by UsersLogbooksMomentsStrategy.
        """

        def count_total_documents(self, collection, config: ImportConfig) -> int:
            """Return 0 - data is pre-computed by moments strategy"""
            return 0

        def get_documents(self, collection, config: ImportConfig, offset: int = 0):
            """Return empty - data is pre-computed by moments strategy"""
            return []

        def extract_data_for_sql(self, document, config: ImportConfig):
            """Not used - data is pre-computed"""
            return None, None

        def get_use_on_conflict(self) -> bool:
            return True

        def export_data(self, conn, collection, config: ImportConfig):
            """
            Override export_data to insert pre-computed details from module-level cache.
            This batch was populated by UsersLogbooksMomentsStrategy during moments processing.
            """
            from src.migration.import_strategies import DIRECT_IMPORT, ImportUtils

            # Access the module-level cache
            global _moments_details_batch_cache

            # Check if we have pre-computed details
            if not _moments_details_batch_cache:
                print(f"No pre-computed details found in cache")
                return 0

            batch_values = _moments_details_batch_cache
            columns = ['id', 'user_logbook_moment_id', 'question', 'answer', 'created_at', 'updated_at']

            print(f"Inserting {len(batch_values)} pre-computed moment details...")

            # Insert all pre-computed details in one batch
            actual_insertions = ImportUtils.execute_batch(
                conn, batch_values, columns, config.table_name,
                config.summary_instance, use_on_conflict=self.get_use_on_conflict(),
                on_conflict_clause=self.get_on_conflict_clause(config.table_name, columns)
            )

            print(f"Inserted {actual_insertions}/{len(batch_values)} moment details")

            # Clear the cache after processing
            _moments_details_batch_cache = []

            return actual_insertions

    return UsersLogbooksMomentsDetailsStrategy()


def create_schemas():
    schemas = {
        'ingredients': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
            ],
            export_order=1
        ),
        
        'appointment_types': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('title', 'VARCHAR(255)', nullable=False),
            ],
            mongo_collection='appointmenttypes',
            export_order=1
        ),
        
        'companies': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
            ],
            export_order=1
        ),
        
        'offers': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('duration', 'SMALLINT', nullable=False),
                ColumnDefinition('coaching_credit', 'SMALLINT', nullable=False),
            ],
            export_order=1
        ),
        
        'categories': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('type', 'VARCHAR(255)', nullable=False),
            ],
            export_order=1
        ),
        
        'targets': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
            ],
            export_order=1
        ),
        
        'events': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('type', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('start_date', 'TIMESTAMP', nullable=False),
                ColumnDefinition('end_date', 'TIMESTAMP', nullable=False),
                ColumnDefinition('company_id', 'VARCHAR', foreign_key='companies(id)')
            ],
            additional_mappings={
                'company': 'company_id',
                '__t': 'type',
            },
            export_order=2
        ),
        
        'users': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('firstname', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('lastname', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('email', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('birthdate', 'DATE'),
                ColumnDefinition('company_id', 'VARCHAR', foreign_key='companies(id)'),
                ColumnDefinition('role', 'VARCHAR(100)', nullable=False),
            ],
            additional_mappings={
                'company': 'company_id'
            },
            export_order=2
        ),
        
        
        'user_events': TableSchema.create(
            columns=[
                ColumnDefinition('user_id', 'VARCHAR', nullable=False, foreign_key='users(id)'),
                ColumnDefinition('event_id', 'VARCHAR', nullable=False, foreign_key='events(id)'),
                ColumnDefinition('created_at', 'TIMESTAMP', nullable=False),
                ColumnDefinition('updated_at', 'TIMESTAMP', nullable=False)
            ],
            mongo_collection='users',
            explicit_mappings={
                'creation_date': 'created_at',
                'update_date': 'updated_at'
            },
            export_order=3,
            import_strategy=_create_user_events_strategy(),
            unique_constraints=[['user_id', 'event_id']]
        ),
        
        'users_targets': TableSchema.create(
            columns=[
                ColumnDefinition('user_id', 'VARCHAR', nullable=False, foreign_key='users(id)'),
                ColumnDefinition('target_id', 'VARCHAR', nullable=False, foreign_key='targets(id)'),
                ColumnDefinition('type', 'VARCHAR(50)', nullable=False),
                ColumnDefinition('created_at', 'TIMESTAMP', nullable=False),
                ColumnDefinition('updated_at', 'TIMESTAMP', nullable=False)
            ],
            mongo_collection='users',
            explicit_mappings={
                'creation_date': 'created_at',
                'update_date': 'updated_at'
            },
            export_order=3,
            import_strategy=_create_users_targets_strategy(),
            unique_constraints=[['user_id', 'target_id', 'type']]
        ),
        
        
        'messages': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('sender_id', 'VARCHAR', foreign_key='users(id)'),
                ColumnDefinition('receiver_id', 'VARCHAR', foreign_key='users(id)'),
                ColumnDefinition('content', 'TEXT', nullable=False),
            ],
            additional_mappings={
                'sender': 'sender_id',
                'receiver': 'receiver_id'
            },
            export_order=3
        ),
        
        'coachings': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('user_id', 'VARCHAR', foreign_key='users(id)'),
                ColumnDefinition('diet_id', 'VARCHAR', foreign_key='users(id)'),
                ColumnDefinition('offer_id', 'VARCHAR', foreign_key='offers(id)'),
                ColumnDefinition('status', 'VARCHAR(100)', nullable=False),
            ],
            additional_mappings={
                'user': 'user_id',
                'diet': 'diet_id',
                'offer': 'offer_id'
            },
            export_order=3
        ),

        'users_logbook': TableSchema.create(
            columns=[
                ColumnDefinition('id', 'SERIAL', primary_key=True),
                ColumnDefinition('user_id', 'VARCHAR', nullable=False, foreign_key='users(id)'),
                ColumnDefinition('day', 'DATE', nullable=False),
                ColumnDefinition('created_at', 'TIMESTAMP', nullable=False),
                ColumnDefinition('updated_at', 'TIMESTAMP', nullable=False)
            ],
            mongo_collection='coachinglogbooks',
            explicit_mappings={
                'user': 'user_id',
                'creation_date': 'created_at',
                'update_date': 'updated_at'
            },
            export_order=3,
            import_strategy=_create_users_logbook_strategy(),
            unique_constraints=[['user_id', 'day']]
        ),

        'users_logbooks_moments': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('user_logbook_id', 'INTEGER', nullable=False, foreign_key='users_logbook(id)'),
                ColumnDefinition('type', 'VARCHAR(255)', nullable=False),
            ],
            mongo_collection='userquizzs',
            export_order=4,
            import_strategy=_create_users_logbooks_moments_strategy()
        ),

        'users_logbooks_moments_details': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('user_logbook_moment_id', 'VARCHAR', nullable=False, foreign_key='users_logbooks_moments(id)'),
                ColumnDefinition('question', 'TEXT', nullable=False),
                ColumnDefinition('answer', 'TEXT', nullable=False),
            ],
            mongo_collection='userquizzs',
            export_order=5,
            import_strategy=_create_users_logbooks_moments_details_strategy()
        ),


        'appointments': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('user_id', 'VARCHAR', foreign_key='users(id)'),
                ColumnDefinition('coaching_id', 'VARCHAR', foreign_key='coachings(id)'),
                ColumnDefinition('type_id', 'VARCHAR', foreign_key='appointment_types(id)'),
                ColumnDefinition('start_date', 'DATE', nullable=False),
                ColumnDefinition('end_date', 'DATE', nullable=False),
                ColumnDefinition('validated', 'BOOLEAN'),
                ColumnDefinition('order_nb', 'SMALLINT', nullable=False),
            ],
            additional_mappings={
                'user': 'user_id',
                'coaching': 'coaching_id',
                'appointment_type': 'type_id',
                'order': 'order_nb'
            },
            export_order=6
        ),
        
        
        
        'recipes': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
            ],
            export_order=1
        ),
        
        'menus': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
            ],
            export_order=2
        ),
        
        'menu_recipes': TableSchema.create(
            columns=[
                ColumnDefinition('menu_id', 'VARCHAR', nullable=False, foreign_key='menus(id)'),
                ColumnDefinition('recipe_id', 'VARCHAR', nullable=False, foreign_key='recipes(id)'),
                ColumnDefinition('created_at', 'TIMESTAMP', nullable=False),
                ColumnDefinition('updated_at', 'TIMESTAMP', nullable=False)
            ],
            mongo_collection='menu_recipes',
            explicit_mappings={
                'creation_date': 'created_at',
                'update_date': 'updated_at',
                'menu': 'menu_id',
                'recipe': 'recipe_id'
            },
            export_order=3,
            unique_constraints=[['menu_id', 'recipe_id']]
        )
    }
    
    # Set table names from schema keys if not provided
    for key, schema in schemas.items():
        if schema.name is None:
            schema.name = key
            # Also update mongo_collection if it was None
            if schema.mongo_collection is None:
                schema.mongo_collection = key
    
    return schemas

TABLE_SCHEMAS = create_schemas()