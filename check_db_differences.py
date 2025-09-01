#!/usr/bin/env python3
"""
Database comparison script to check differences between MongoDB and PostgreSQL
Displays missing IDs in PostgreSQL by entity.
"""

import sys
import os
from typing import Dict, List, Set, Tuple
from datetime import datetime

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from connections.mongo_connection import get_mongo_db
from connections.postgres_connection import connect_postgres
from schemas.schemas import TABLE_SCHEMAS


class DatabaseComparator:
    def __init__(self):
        self.mongo_db = None
        self.postgres_conn = None
        self.results = {}
    
    def connect_databases(self):
        """Connect to both MongoDB and PostgreSQL"""
        try:
            print("Connecting to MongoDB...")
            self.mongo_db = get_mongo_db()
            print("✅ MongoDB connected")
            
            print("Connecting to PostgreSQL...")
            self.postgres_conn = connect_postgres()
            print("✅ PostgreSQL connected")
            
        except Exception as e:
            print(f"❌ Database connection error: {e}")
            sys.exit(1)
    
    def get_mongo_ids(self, collection_name: str) -> Set[str]:
        """Get all IDs from a MongoDB collection"""
        try:
            collection = self.mongo_db[collection_name]
            cursor = collection.find({}, {"_id": 1})
            return {str(doc["_id"]) for doc in cursor}
        except Exception as e:
            print(f"❌ Error fetching MongoDB IDs for {collection_name}: {e}")
            return set()
    
    def get_postgres_ids(self, table_name: str) -> Set[str]:
        """Get all IDs from a PostgreSQL table"""
        try:
            cursor = self.postgres_conn.cursor()
            cursor.execute(f"SELECT id FROM {table_name}")
            return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            print(f"❌ Error fetching PostgreSQL IDs for {table_name}: {e}")
            return set()
    
    def compare_entity(self, entity_name: str, schema) -> Dict:
        """Compare a single entity between MongoDB and PostgreSQL"""
        mongo_collection = schema.mongo_collection or entity_name
        
        print(f"\n🔍 Comparing {entity_name} (MongoDB: {mongo_collection} → PostgreSQL: {entity_name})")
        
        # Get IDs from both databases
        mongo_ids = self.get_mongo_ids(mongo_collection)
        postgres_ids = self.get_postgres_ids(entity_name)
        
        # Find missing IDs
        missing_in_postgres = mongo_ids - postgres_ids
        extra_in_postgres = postgres_ids - mongo_ids
        
        result = {
            'mongo_collection': mongo_collection,
            'postgres_table': entity_name,
            'mongo_count': len(mongo_ids),
            'postgres_count': len(postgres_ids),
            'missing_in_postgres': sorted(list(missing_in_postgres)),
            'extra_in_postgres': sorted(list(extra_in_postgres)),
            'missing_count': len(missing_in_postgres),
            'extra_count': len(extra_in_postgres)
        }
        
        # Print summary
        print(f"  MongoDB {mongo_collection}: {len(mongo_ids)} records")
        print(f"  PostgreSQL {entity_name}: {len(postgres_ids)} records")
        print(f"  Missing in PostgreSQL: {len(missing_in_postgres)} records")
        print(f"  Extra in PostgreSQL: {len(extra_in_postgres)} records")
        
        return result
    
    def run_comparison(self) -> Dict:
        """Run comparison for all entities"""
        print("=" * 60)
        print("DATABASE COMPARISON REPORT")
        print("=" * 60)
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        self.connect_databases()
        
        results = {}
        
        # Sort tables by export_order for consistent processing
        sorted_tables = sorted(TABLE_SCHEMAS.items(), key=lambda x: x[1].export_order)
        
        for entity_name, schema in sorted_tables:
            results[entity_name] = self.compare_entity(entity_name, schema)
        
        self.results = results
        return results
    
    def print_missing_ids_report(self):
        """Print detailed report of missing IDs in PostgreSQL"""
        print("\n" + "=" * 60)
        print("MISSING IDs IN POSTGRESQL - DETAILED REPORT")
        print("=" * 60)
        
        total_missing = 0
        
        for entity_name, result in self.results.items():
            missing_ids = result['missing_in_postgres']
            
            if missing_ids:
                total_missing += len(missing_ids)
                print(f"\n📋 {entity_name.upper()}")
                print(f"   Collection: {result['mongo_collection']}")
                print(f"   Missing {len(missing_ids)} IDs in PostgreSQL:")
                
                # Print IDs in chunks of 10 for readability
                for i in range(0, len(missing_ids), 10):
                    chunk = missing_ids[i:i+10]
                    print(f"   {', '.join(chunk)}")
            else:
                print(f"\n✅ {entity_name.upper()}: No missing IDs")
        
        print(f"\n📊 SUMMARY: {total_missing} total IDs missing in PostgreSQL across all entities")
    
    def print_summary(self):
        """Print summary statistics"""
        print("\n" + "=" * 60)
        print("COMPARISON SUMMARY")
        print("=" * 60)
        
        print(f"{'Entity':<20} {'MongoDB':<10} {'PostgreSQL':<12} {'Missing':<8} {'Extra':<8} {'Status'}")
        print("-" * 70)
        
        for entity_name, result in self.results.items():
            status = "✅ OK" if result['missing_count'] == 0 and result['extra_count'] == 0 else "⚠️  DIFF"
            print(f"{entity_name:<20} {result['mongo_count']:<10} {result['postgres_count']:<12} "
                  f"{result['missing_count']:<8} {result['extra_count']:<8} {status}")
    
    def close_connections(self):
        """Close database connections"""
        if self.postgres_conn:
            self.postgres_conn.close()
        if self.mongo_db is not None and hasattr(self.mongo_db.client, 'close'):
            self.mongo_db.client.close()


def main():
    """Main function to run the database comparison"""
    comparator = DatabaseComparator()
    
    try:
        # Run the comparison
        comparator.run_comparison()
        
        # Print reports
        comparator.print_missing_ids_report()
        comparator.print_summary()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Operation cancelled by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        comparator.close_connections()
        print("\n🔚 Database comparison completed")


if __name__ == "__main__":
    main()