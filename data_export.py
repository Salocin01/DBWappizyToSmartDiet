from schemas import TABLE_SCHEMAS
from bson import ObjectId
import psycopg2
from mongo_connection import get_mongo_collection
from import_summary import ImportSummary

# Global instance for backward compatibility
import_summary = ImportSummary()

def print_import_summary(entities=None, summary_instance=None):
    """Print a summary of import statistics by entity
    
    Args:
        entities: String or list of entity names to show. If None, shows all.
        summary_instance: ImportSummary instance to use. If None, uses global instance.
    """
    summary = summary_instance or import_summary
    summary.print_summary(entities)

def export_table_data(conn, table_name, collection, custom_filter=None, summary_instance=None):
    schema = TABLE_SCHEMAS[table_name]
    
    # Special handling for quizz_questions
    if table_name == 'quizz_questions':
        export_quizz_questions(conn, collection, summary_instance)
        return
    elif table_name == 'user_quizz_questions':
        export_user_quizz_questions(conn, collection, summary_instance)
        return
    
    documents = collection.find()
    cursor = conn.cursor()
    
    for doc in documents:
        if custom_filter and not custom_filter(doc):
            print(doc)
            continue
            
        values = []
        placeholders = []
        columns = []
        
        for mongo_field, pg_field in schema.field_mappings.items():
            if mongo_field == '_id':
                value = str(doc['_id'])
            elif mongo_field in doc:
                value = doc[mongo_field]
                if isinstance(value, ObjectId):
                    value = str(value)
            else:
                value = None
                
            values.append(value)
            placeholders.append('%s')
            columns.append(pg_field)
        
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)}) ON CONFLICT (id) DO NOTHING"
        try:
            cursor.execute(sql, values)
            summary = summary_instance or import_summary
            summary.record_success(table_name)
        except psycopg2.IntegrityError as e:
            error_message = str(e).lower()
            if "foreign key constraint" in error_message:
                summary = summary_instance or import_summary
                summary.record_error(table_name, 'Foreign key constraint')
                conn.rollback()
                continue
            elif "null value" in error_message or "not-null constraint" in error_message:
                summary = summary_instance or import_summary
                summary.record_error(table_name, 'NULL constraint')
                conn.rollback()
                continue
            else:
                raise e
    
    conn.commit()


def export_quizz_questions(conn, questions_collection, summary_instance=None, total=None, limit=20000, offset=0):
    """Special export for quizz_questions that handles the Questions array relationship"""
    cursor = conn.cursor()
    quizzs_collection = get_mongo_collection('quizzs')
    
    # Get all quizzs with their Questions arrays
    quizzs = quizzs_collection.find({'questions': {'$exists': True, '$ne': []}})
    
    for quiz in quizzs:
        quiz_id = str(quiz['_id'])
        questions_ids = quiz.get('questions', [])
        
        for question_id in questions_ids:
            # Find the corresponding question document
            question_doc = questions_collection.find_one({'_id': ObjectId(question_id)})
            
            if question_doc:
                # Prepare values for insertion
                values = [
                    str(question_doc['_id']),  # id
                    quiz_id,  # quizz_id (foreign key)
                    question_doc.get('title', ''),  # question text
                    question_doc.get('type', None)
                ]
                
                sql = """INSERT INTO quizz_questions (id, quizz_id, title, type) 
                         VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING"""
                
                try:
                    cursor.execute(sql, values)
                    summary = summary_instance or import_summary
                    summary.record_success('quizz_questions')
                except psycopg2.IntegrityError as e:
                    error_message = str(e).lower()
                    if "foreign key constraint" in error_message:
                        summary = summary_instance or import_summary
                        summary.record_error('quizz_questions', 'Foreign key constraint')
                        conn.rollback()
                        continue
                    else:
                        raise e
            else:
                summary = summary_instance or import_summary
                summary.record_error('quizz_questions', 'Question not found')
    
    conn.commit()


def export_user_quizz_questions(conn, questions_collection, summary_instance=None, total=None, limit=2000, offset=0):
    """Special export for quizz_questions that handles the Questions array relationship"""
    cursor = conn.cursor()
    user_quizzs_collection = get_mongo_collection('userquizzs')

    if total is None:
        '''
        pipeline_count = [
            {
                "$group": {
                "_id": None,
                "count": {
                    "$sum": 1
                }
                }
            },
            {
                "$sort": {
                "_id": 1
                }
            },
            {
                "$project": {
                "_id": False,
                "count": True
                }
            }
        ]
        total = list(questions_collection.aggregate(pipeline_count))
        '''
        total = 50000
        
    pipeline = [
        {
            "$lookup": {
                "from": "userquizzquestions",
                "localField": "questions",
                "foreignField": "_id",
                "as": "questions_answers"
            }
        },
        { "$unwind": "$questions_answers" },
        {
            "$project": {
                "_id": "$questions_answers._id",
                "user_quizz_id": "$_id",
                "creation_date": "$questions_answers.creation_date"
            }
        },
        { "$skip": offset },   # offset (exemple)
        { "$limit": limit }
    ]
    
    # Get all quizzs with their Questions arrays
    quizzs = user_quizzs_collection.aggregate(pipeline)
    
    for quizz in quizzs:
        quiz_id = str(quizz['_id'])
        user_quizz_id = str(quizz['user_quizz_id'])
        
        # Prepare values for insertion
        values = [
            quiz_id,  # id
            user_quizz_id,  # quizz_id (foreign key)
            quizz.get('creation_date', None)
        ]
        
        sql = """INSERT INTO user_quizz_questions (id, user_quizz_id, created_at) 
                    VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING"""
        
        try:
            cursor.execute(sql, values)
            summary = summary_instance or import_summary
            summary.record_success('user_quizz_questions')
        except psycopg2.IntegrityError as e:
            error_message = str(e).lower()
            if "foreign key constraint" in error_message:
                summary = summary_instance or import_summary
                summary.record_error('user_quizz_questions', 'Foreign key constraint')
                conn.rollback()
                continue
            else:
                raise e
    
    conn.commit()

    print_import_summary('user_quizz_questions', summary_instance)

    if offset + limit < total:
        export_user_quizz_questions(conn, questions_collection, summary_instance, total, limit, limit + offset)