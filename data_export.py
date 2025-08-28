from schemas import TABLE_SCHEMAS
from bson import ObjectId
import psycopg2
from mongo_connection import get_mongo_collection
from import_summary import ImportSummary

# Global instance for backward compatibility
import_summary = ImportSummary()

def get_last_insert_date(conn, table_name):
    """Get the latest created_at date from a table to use as starting point for incremental imports"""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT MAX(created_at) FROM {table_name}")
        result = cursor.fetchone()
        return result[0] if result and result[0] else None
    except psycopg2.Error as e:
        print(f"Error getting last insert date for {table_name}: {e}")
        return None
    finally:
        cursor.close()

def print_import_summary(entities=None, summary_instance=None):
    """Print a summary of import statistics by entity
    
    Args:
        entities: String or list of entity names to show. If None, shows all.
        summary_instance: ImportSummary instance to use. If None, uses global instance.
    """
    summary = summary_instance or import_summary
    summary.print_summary(entities)

def export_table_data(conn, table_name, collection, custom_filter=None, summary_instance=None, after_date=None, batch_size=5000):
    schema = TABLE_SCHEMAS[table_name]
    
    # Special handling for quizz_questions
    if table_name == 'quizz_questions':
        export_quizz_questions(conn, collection, summary_instance, after_date=after_date)
        return
    elif table_name == 'user_quizz_questions':
        export_user_quizz_questions(conn, collection, summary_instance, after_date=after_date)
        return
    
    # Build MongoDB query filter
    mongo_filter = {}
    if after_date:
        from datetime import datetime, time
        if hasattr(after_date, 'date'):  # It's already a datetime
            mongo_filter['creation_date'] = {'$gt': after_date}
        else:  # It's a date, convert to datetime
            mongo_filter['creation_date'] = {'$gt': datetime.combine(after_date, time.min)}
    
    cursor = conn.cursor()
    
    # Get total count for progress tracking
    total_docs = collection.count_documents(mongo_filter)
    processed_docs = 0
    
    # Process documents in batches
    offset = 0
    while True:
        documents = list(collection.find(mongo_filter).skip(offset).limit(batch_size))
        
        if not documents:
            break
        
        batch_values = []
        columns = None
        
        for doc in documents:
            if custom_filter and not custom_filter(doc):
                continue
                
            values = []
            if columns is None:
                columns = []
                for mongo_field, pg_field in schema.field_mappings.items():
                    columns.append(pg_field)
            
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
            
            batch_values.append(values)
        
        if batch_values:
            placeholders = ', '.join(['%s'] * len(columns))
            sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING"
            
            try:
                cursor.executemany(sql, batch_values)
                summary = summary_instance or import_summary
                summary.record_success(table_name, len(batch_values))
                processed_docs += len(batch_values)
                conn.commit()
                print(f"Processed {processed_docs}/{total_docs} documents for {table_name}")
                
            except psycopg2.IntegrityError as e:
                conn.rollback()
                error_message = str(e).lower()
                
                # Fall back to individual inserts for error handling
                for values in batch_values:
                    try:
                        cursor.execute(sql.replace(f"({placeholders})", "(%s" + ", %s" * (len(values)-1) + ")"), values)
                        summary = summary_instance or import_summary
                        summary.record_success(table_name)
                        processed_docs += 1
                    except psycopg2.IntegrityError as individual_e:
                        individual_error = str(individual_e).lower()
                        if "foreign key constraint" in individual_error:
                            summary = summary_instance or import_summary
                            summary.record_error(table_name, 'Foreign key constraint')
                        elif "null value" in individual_error or "not-null constraint" in individual_error:
                            summary = summary_instance or import_summary
                            summary.record_error(table_name, 'NULL constraint')
                        else:
                            raise individual_e
                        conn.rollback()
                        continue
                
                conn.commit()
        
        offset += batch_size
        
        if len(documents) < batch_size:
            break
    
    print(f"Completed processing {processed_docs} documents for {table_name}")


def export_quizz_questions(conn, questions_collection, summary_instance=None, after_date=None, batch_size=5000):
    """Special export for quizz_questions that handles the Questions array relationship"""
    cursor = conn.cursor()
    quizzs_collection = get_mongo_collection('quizzs')
    
    # Build query filter for quizzs
    quiz_filter = {'questions': {'$exists': True, '$ne': []}}
    if after_date:
        from datetime import datetime, time
        if hasattr(after_date, 'date'):  # It's already a datetime
            quiz_filter['creation_date'] = {'$gt': after_date}
        else:  # It's a date, convert to datetime
            quiz_filter['creation_date'] = {'$gt': datetime.combine(after_date, time.min)}
    
    # Get total count for progress tracking
    total_quizzs = quizzs_collection.count_documents(quiz_filter)
    processed_quizzs = 0
    total_questions = 0
    
    # Process quizzs in batches
    offset = 0
    while True:
        quizzs = list(quizzs_collection.find(quiz_filter).skip(offset).limit(batch_size))
        
        if not quizzs:
            break
        
        # Collect all question IDs from this batch of quizzes
        all_question_ids = []
        quiz_to_questions = {}
        
        for quiz in quizzs:
            quiz_id = str(quiz['_id'])
            questions_ids = quiz.get('questions', [])
            quiz_to_questions[quiz_id] = questions_ids
            all_question_ids.extend(questions_ids)
        
        # Fetch all question documents in one query
        questions_docs = {}
        if all_question_ids:
            question_cursor = questions_collection.find(
                {'_id': {'$in': all_question_ids}},
                {'_id': 1, 'title': 1, 'type': 1, 'creation_date': 1, 'update_date': 1}
            )
            for question_doc in question_cursor:
                questions_docs[question_doc['_id']] = question_doc
        
        # Build batch values
        batch_values = []
        for quiz_id, question_ids in quiz_to_questions.items():
            for question_id in question_ids:
                if question_id in questions_docs:
                    question_doc = questions_docs[question_id]
                    batch_values.append([
                        str(question_doc['_id']),  # id
                        quiz_id,  # quizz_id (foreign key)
                        question_doc.get('title', ''),  # question text
                        question_doc.get('type', None),  
                        question_doc.get('creation_date', None),  
                        question_doc.get('update_date', None)
                    ])
                else:
                    summary = summary_instance or import_summary
                    summary.record_error('quizz_questions', 'Question not found')
        
        if batch_values:
            sql = """INSERT INTO quizz_questions (id, quizz_id, title, type, created_at, updated_at) 
                     VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING"""
            
            try:
                cursor.executemany(sql, batch_values)
                summary = summary_instance or import_summary
                summary.record_success('quizz_questions', len(batch_values))
                total_questions += len(batch_values)
                conn.commit()
                
            except psycopg2.IntegrityError as e:
                conn.rollback()
                # Fall back to individual inserts for error handling
                for values in batch_values:
                    try:
                        cursor.execute(sql, values)
                        summary = summary_instance or import_summary
                        summary.record_success('quizz_questions')
                        total_questions += 1
                    except psycopg2.IntegrityError as individual_e:
                        error_message = str(individual_e).lower()
                        if "foreign key constraint" in error_message:
                            summary = summary_instance or import_summary
                            summary.record_error('quizz_questions', 'Foreign key constraint')
                        else:
                            raise individual_e
                        conn.rollback()
                        continue
                
                conn.commit()
        
        processed_quizzs += len(quizzs)
        print(f"Processed {processed_quizzs}/{total_quizzs} quizzs, {total_questions} questions for quizz_questions")
        
        offset += batch_size
        
        if len(quizzs) < batch_size:
            break
    
    print(f"Completed processing {total_questions} questions from {processed_quizzs} quizzs")


def export_user_quizz_questions(conn, questions_collection, summary_instance=None, batch_size=5000, after_date=None):
    """Special export for user_quizz_questions using direct queries for better performance
    
    Args:
        after_date: Only export userquizz created after this date (datetime object or string)
    """
    cursor = conn.cursor()
    user_quizzs_collection = get_mongo_collection('userquizzs')
    user_quizz_questions_collection = get_mongo_collection('userquizzquestions')
    
    offset = 0
    processed_records = 0
    
    # Build query filter
    query_filter = {'questions': {'$exists': True, '$ne': []}}
    if after_date:
        # Convert date to datetime for MongoDB compatibility
        from datetime import datetime, time
        if hasattr(after_date, 'date'):  # It's already a datetime
            query_filter['creation_date'] = {'$gt': after_date}
        else:  # It's a date, convert to datetime
            query_filter['creation_date'] = {'$gt': datetime.combine(after_date, time.min)}
    
    while True:
        # First, get user quizzes in batches, ordered by creation_date ascending
        user_quizzs = list(user_quizzs_collection.find(
            query_filter,
            {'_id': 1, 'questions': 1}
        ).sort('creation_date', 1).skip(offset).limit(batch_size))
        
        if not user_quizzs:
            break
        
        # Collect all question IDs from this batch
        all_question_ids = []
        user_quiz_to_questions = {}
        
        for user_quiz in user_quizzs:
            user_quiz_id = str(user_quiz['_id'])
            question_ids = user_quiz.get('questions', [])
            user_quiz_to_questions[user_quiz_id] = question_ids
            all_question_ids.extend(question_ids)
        
        # Fetch all question documents in one query
        questions_docs = {}
        if all_question_ids:
            question_cursor = user_quizz_questions_collection.find(
                {'_id': {'$in': all_question_ids}},
                {'_id': 1, 'quizz_question': 1, 'creation_date': 1, 'update_date': 1}
            )
            for question_doc in question_cursor:
                questions_docs[question_doc['_id']] = question_doc
        
        # Build batch values
        batch_values = []
        for user_quiz_id, question_ids in user_quiz_to_questions.items():
            for question_id in question_ids:
                if question_id in questions_docs:
                    question_doc = questions_docs[question_id]
                    batch_values.append([
                        str(question_doc['_id']),
                        user_quiz_id,
                        str(question_doc['quizz_question']) if question_doc.get('quizz_question') else None,
                        question_doc.get('creation_date', None),
                        question_doc.get('update_date', None)
                    ])
        
        if batch_values:
            sql = """INSERT INTO user_quizz_questions (id, user_quizz_id, quizz_question_id, created_at, updated_at) 
                        VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING"""
            
            try:
                cursor.executemany(sql, batch_values)
                summary = summary_instance or import_summary
                summary.record_success('user_quizz_questions', len(batch_values))
                processed_records += len(batch_values)
                conn.commit()
            except psycopg2.IntegrityError as e:
                conn.rollback()
                error_message = str(e).lower()
                if "foreign key constraint" in error_message:
                    summary = summary_instance or import_summary
                    summary.record_error('user_quizz_questions', f'Foreign key constraint (batch of {len(batch_values)})')
                    for values in batch_values:
                        try:
                            cursor.execute(sql, values)
                            summary.record_success('user_quizz_questions')
                            processed_records += 1
                        except psycopg2.IntegrityError:
                            summary.record_error('user_quizz_questions', 'Foreign key constraint')
                            conn.rollback()
                            continue
                    conn.commit()
                else:
                    raise e
        
        offset += batch_size
        
        print(f"Processed {processed_records} records so far...")
        
        if len(user_quizzs) < batch_size:
            break
    
    print_import_summary('user_quizz_questions', summary_instance)
    print(f"Total records processed: {processed_records}")