from table_schemas import TableSchema, ColumnDefinition

def create_schemas():
    schemas = {
        'ingredients': TableSchema.create(
            columns=[
                ColumnDefinition('id', 'VARCHAR', primary_key=True),
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False)
            ],
            export_order=1
        ),
        
        'appointment_types': TableSchema.create(
            columns=[
                ColumnDefinition('id', 'VARCHAR', primary_key=True),
                ColumnDefinition('title', 'VARCHAR(255)', nullable=False)
            ],
            mongo_collection='appointmenttypes',
            export_order=1
        ),
        
        'companies': TableSchema.create(
            columns=[
                ColumnDefinition('id', 'VARCHAR', primary_key=True),
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False)
            ],
            export_order=1
        ),
        
        'offers': TableSchema.create(
            columns=[
                ColumnDefinition('id', 'VARCHAR', primary_key=True),
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('duration', 'SMALLINT', nullable=False),
                ColumnDefinition('coaching_credit', 'SMALLINT', nullable=False)
            ],
            export_order=1
        ),
        
        'categories': TableSchema.create(
            columns=[
                ColumnDefinition('id', 'VARCHAR', primary_key=True),
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('type', 'VARCHAR(255)', nullable=False)
            ],
            export_order=1
        ),
        
        'targets': TableSchema.create(
            columns=[
                ColumnDefinition('id', 'VARCHAR', primary_key=True),
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False)
            ],
            export_order=1
        ),
        
        'users': TableSchema.create(
            columns=[
                ColumnDefinition('id', 'VARCHAR', primary_key=True),
                ColumnDefinition('firstname', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('lastname', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('email', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('birthdate', 'DATE'),
                ColumnDefinition('created_at', 'DATE', nullable=False),
                ColumnDefinition('company_id', 'VARCHAR', foreign_key='companies(id)'),
                ColumnDefinition('role', 'VARCHAR(100)', nullable=False)
            ],
            explicit_mappings={
                'creation_date': 'created_at',
                'company': 'company_id'
            },
            export_order=2
        ),
        
        'quizzs': TableSchema.create(
            columns=[
                ColumnDefinition('id', 'VARCHAR', primary_key=True),
                ColumnDefinition('name', 'VARCHAR(255)', nullable=False),
                ColumnDefinition('type', 'VARCHAR(255)', nullable=False)
            ],
            export_order=2
        ),
        
        'quizz_questions': TableSchema.create(
            columns=[
                ColumnDefinition('id', 'VARCHAR', primary_key=True),
                ColumnDefinition('quizz_id', 'VARCHAR', foreign_key='quizzs(id)'),
                ColumnDefinition('title', 'VARCHAR', nullable=False),
                ColumnDefinition('type', 'VARCHAR(100)')
            ],
            mongo_collection='quizzquestions',
            export_order=3
        ),
        
        'messages': TableSchema.create(
            columns=[
                ColumnDefinition('id', 'VARCHAR', primary_key=True),
                ColumnDefinition('sender_id', 'VARCHAR', foreign_key='users(id)'),
                ColumnDefinition('receiver_id', 'VARCHAR', foreign_key='users(id)'),
                ColumnDefinition('content', 'TEXT', nullable=False),
                ColumnDefinition('created_at', 'DATE', nullable=False)
            ],
            explicit_mappings={
                'creation_date': 'created_at',
                'sender': 'sender_id',
                'receiver': 'receiver_id'
            },
            export_order=3
        ),
        
        'coachings': TableSchema.create(
            columns=[
                ColumnDefinition('id', 'VARCHAR', primary_key=True),
                ColumnDefinition('user_id', 'VARCHAR', foreign_key='users(id)'),
                ColumnDefinition('diet_id', 'VARCHAR', foreign_key='users(id)'),
                ColumnDefinition('offer_id', 'VARCHAR', foreign_key='offers(id)'),
                ColumnDefinition('status', 'VARCHAR(100)', nullable=False),
                ColumnDefinition('created_at', 'DATE', nullable=False)
            ],
            explicit_mappings={
                'creation_date': 'created_at',
                'user': 'user_id',
                'diet': 'diet_id',
                'offer': 'offer_id'
            },
            export_order=3
        ),
        
        'appointments': TableSchema.create(
            columns=[
                ColumnDefinition('id', 'VARCHAR', primary_key=True),
                ColumnDefinition('user_id', 'VARCHAR', foreign_key='users(id)'),
                ColumnDefinition('coaching_id', 'VARCHAR', foreign_key='coachings(id)'),
                ColumnDefinition('type_id', 'VARCHAR', foreign_key='appointment_types(id)'),
                ColumnDefinition('start_date', 'DATE', nullable=False),
                ColumnDefinition('end_date', 'DATE', nullable=False),
                ColumnDefinition('created_at', 'DATE', nullable=False),
                ColumnDefinition('validated', 'BOOLEAN'),
                ColumnDefinition('order_nb', 'SMALLINT', nullable=False)
            ],
            explicit_mappings={
                'creation_date': 'created_at',
                'user': 'user_id',
                'coaching': 'coaching_id',
                'appointment_type': 'type_id',
                'order': 'order_nb'
            },
            export_order=4
        ),
        
        'coachings_logbooks': TableSchema.create(
            columns=[
                ColumnDefinition('id', 'VARCHAR', primary_key=True),
                ColumnDefinition('day', 'DATE', nullable=False),
                ColumnDefinition('created_at', 'DATE', nullable=False),
                ColumnDefinition('user_id', 'VARCHAR', foreign_key='users(id)'),
                ColumnDefinition('coaching_id', 'VARCHAR', foreign_key='coachings(id)')
            ],
            explicit_mappings={
                'creation_date': 'created_at',
                'user': 'user_id',
                'coaching': 'coaching_id'
            },
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