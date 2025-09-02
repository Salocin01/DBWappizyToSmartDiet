from .table_schemas import ColumnDefinition, BaseEntitySchema


def _create_quizz_questions_strategy():
    """Create strategy for quizz_questions array extraction"""
    from src.migration.import_strategies import ArrayExtractionStrategy, ArrayExtractionConfig
    
    def quizz_questions_transformer(parent_id, child_doc):
        return [
            str(child_doc['_id']),
            parent_id,
            child_doc.get('title', ''),
            child_doc.get('type', None),
            child_doc.get('creation_date', None),
            child_doc.get('update_date', None)
        ]
    
    config = ArrayExtractionConfig(
        parent_collection='quizzs',
        array_field='questions',
        child_collection='quizzquestions',
        parent_filter_fields={'_id': 1, 'questions': 1},
        child_projection_fields={'_id': 1, 'title': 1, 'type': 1, 'creation_date': 1, 'update_date': 1},
        sql_columns=['id', 'quizz_id', 'title', 'type', 'created_at', 'updated_at'],
        value_transformer=quizz_questions_transformer
    )
    
    return ArrayExtractionStrategy(config)


def _create_user_quizz_questions_strategy():
    """Create strategy for user_quizz_questions array extraction"""
    from src.migration.import_strategies import ArrayExtractionStrategy, ArrayExtractionConfig
    
    def user_quizz_questions_transformer(parent_id, child_doc):
        return [
            str(child_doc['_id']),
            parent_id,
            str(child_doc['quizz_question']) if child_doc.get('quizz_question') else None,
            child_doc.get('creation_date', None),
            child_doc.get('update_date', None)
        ]
    
    config = ArrayExtractionConfig(
        parent_collection='userquizzs',
        array_field='questions',
        child_collection='userquizzquestions',
        parent_filter_fields={'_id': 1, 'questions': 1},
        child_projection_fields={'_id': 1, 'quizz_question': 1, 'creation_date': 1, 'update_date': 1},
        sql_columns=['id', 'user_quizz_id', 'quizz_question_id', 'created_at', 'updated_at'],
        value_transformer=user_quizz_questions_transformer
    )
    
    return ArrayExtractionStrategy(config)


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
        
        'quizzs': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('type', 'VARCHAR(255)', nullable=False),
            ],
            export_order=2
        ),
        
        'quizz_questions': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('quizz_id', 'VARCHAR', foreign_key='quizzs(id)'),
                ColumnDefinition('title', 'VARCHAR', nullable=False),
                ColumnDefinition('type', 'VARCHAR(100)'),
            ],
            mongo_collection='quizzquestions',
            export_order=3,
            import_strategy=_create_quizz_questions_strategy()
        ),
        
        'user_quizzs': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('quizz_id', 'VARCHAR', foreign_key='quizzs(id)'),
                ColumnDefinition('name', 'VARCHAR', nullable=False),
                ColumnDefinition('type', 'VARCHAR(100)'),
            ],
            additional_mappings={
                'quizz': 'quizz_id'
            },
            mongo_collection='userquizzs',
            export_order=3
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
        
        'user_quizz_questions': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('user_quizz_id', 'VARCHAR', foreign_key='user_quizzs(id)'),
                ColumnDefinition('quizz_question_id', 'VARCHAR', foreign_key='quizz_questions(id)'),
            ],
            mongo_collection='userquizzquestions',
            additional_mappings={
                'quizz_question': 'quizz_question_id'
            },
            export_order=4,
            import_strategy=_create_user_quizz_questions_strategy()
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
            export_order=4
        ),
        
        'coachings_logbooks': BaseEntitySchema.create_with_base(
            additional_columns=[
                ColumnDefinition('day', 'DATE', nullable=False),
                ColumnDefinition('user_id', 'VARCHAR', foreign_key='users(id)'),
                ColumnDefinition('coaching_id', 'VARCHAR', foreign_key='coachings(id)'),
                ColumnDefinition('user_quizz_id', 'VARCHAR', foreign_key='user_quizzs(id)'),
            ],
            additional_mappings={
                'user': 'user_id',
                'coaching': 'coaching_id',
                'logbook': 'user_quizz_id'
            },
            mongo_collection='coachinglogbooks',
            export_order=4
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