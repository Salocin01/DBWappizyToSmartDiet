from schemas import TABLE_SCHEMAS
from bson import ObjectId
import psycopg2
from mongo_connection import get_mongo_collection


def export_table_data(conn, table_name, collection, custom_filter=None):
    schema = TABLE_SCHEMAS[table_name]
    
    # Special handling for quizz_questions
    if table_name == 'quizz_questions':
        export_quizz_questions(conn, collection)
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
        except psycopg2.IntegrityError as e:
            error_message = str(e).lower()
            if "foreign key constraint" in error_message:
                print(f"❌ Foreign key error for {table_name} record with id {doc.get('_id', 'unknown')}: {str(e)}")
                conn.rollback()
                continue
            elif "null value" in error_message or "not-null constraint" in error_message:
                print(f"❌ NULL constraint error for {table_name} record with id {doc.get('_id', 'unknown')}: {str(e)}")
                conn.rollback()
                continue
            else:
                raise e
    
    conn.commit()
    print(f"✅ {table_name.title()} imported from MongoDB to PostgreSQL")


def export_quizz_questions(conn, questions_collection):
    """Special export for quizz_questions that handles the Questions array relationship"""
    cursor = conn.cursor()
    quizzs_collection = get_mongo_collection('quizzs')
    
    # Get all quizzs with their Questions arrays
    quizzs = quizzs_collection.find({'Questions': {'$exists': True, '$ne': []}})
    
    for quiz in quizzs:
        quiz_id = str(quiz['_id'])
        questions_ids = quiz.get('Questions', [])
        
        for question_id in questions_ids:
            # Find the corresponding question document
            question_doc = questions_collection.find_one({'_id': ObjectId(question_id)})
            
            if question_doc:
                # Prepare values for insertion
                values = [
                    str(question_doc['_id']),  # id
                    quiz_id,  # quizz_id (foreign key)
                    question_doc.get('question', ''),  # question text
                    question_doc.get('type', None)
                ]
                
                sql = """INSERT INTO quizz_questions (id, quizz_id, question, type) 
                         VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING"""
                
                try:
                    cursor.execute(sql, values)
                except psycopg2.IntegrityError as e:
                    error_message = str(e).lower()
                    if "foreign key constraint" in error_message:
                        print(f"❌ Foreign key error for quizz_questions record with id {question_doc['_id']}: {str(e)}")
                        conn.rollback()
                        continue
                    else:
                        raise e
    
    conn.commit()
    print("✅ Quizz_Questions imported from MongoDB to PostgreSQL")