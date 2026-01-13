from .table_schemas import ColumnDefinition, BaseEntitySchema, TableSchema
from src.migration.strategies.user_strategies import (
    create_user_events_strategy,
    create_users_logbook_strategy,
    create_users_targets_strategy,
)


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
            import_strategy=create_user_events_strategy(),
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
            import_strategy=create_users_targets_strategy(),
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
            import_strategy=create_users_logbook_strategy(),
            unique_constraints=[['user_id', 'day']]
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
