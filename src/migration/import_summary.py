from collections import defaultdict


class ImportSummary:
    """Handles import statistics tracking and reporting"""
    
    def __init__(self):
        self.stats = defaultdict(lambda: {'good': 0, 'bad': defaultdict(int), 'skipped': 0, 'failed_records': []})
        self.max_failed_records = 10  # Limit to prevent memory issues
    
    def record_success(self, entity, count=1):
        """Record successful import(s) for an entity"""
        self.stats[entity]['good'] += count
    
    def record_error(self, entity, reason, failed_record=None):
        """Record an error for an entity with a specific reason"""
        self.stats[entity]['bad'][reason] += 1
        
        # Store failed record details for debugging (limit to prevent memory issues)
        if failed_record and len(self.stats[entity]['failed_records']) < self.max_failed_records:
            self.stats[entity]['failed_records'].append({
                'reason': reason,
                'record_id': failed_record.get('id', 'unknown'),
                'details': str(failed_record)[:200] + '...' if len(str(failed_record)) > 200 else str(failed_record)
            })
    
    def record_skipped(self, entity, count=1):
        """Record skipped import(s) for an entity (e.g., due to conflicts)"""
        self.stats[entity]['skipped'] += count
    
    def print_summary(self, entities=None):
        """Print summary for specific entities or all entities
        
        Args:
            entities: String or list of entity names to show. If None, shows all.
        """
        print("\n" + "="*60)
        print("IMPORT SUMMARY")
        print("="*60)
        
        entities_to_show = entities if entities else self.stats.keys()
        if isinstance(entities_to_show, str):
            entities_to_show = [entities_to_show]
        
        for entity in entities_to_show:
            if entity in self.stats:
                stats = self.stats[entity]
                print(f"\n📊 {entity.upper()}:")
                print(f"  ✅ Good imports: {stats['good']}")
                
                if stats['skipped'] > 0:
                    print(f"  ⏭️  Skipped imports: {stats['skipped']} (conflicts/duplicates)")
                
                if stats['bad']:
                    print(f"  ❌ Bad imports: {sum(stats['bad'].values())}")
                    print("  Reasons:")
                    for reason, count in stats['bad'].items():
                        print(f"    - {reason}: {count}")
                    
                    # Show failed record details if available
                    if stats['failed_records']:
                        print("  Failed record examples:")
                        for i, failed in enumerate(stats['failed_records'][:5], 1):  # Show max 5 examples
                            print(f"    {i}. ID: {failed['record_id']} | Reason: {failed['reason']}")
                            print(f"       Details: {failed['details']}")
                        
                        if len(stats['failed_records']) > 5:
                            print(f"    ... and {len(stats['failed_records']) - 5} more failed records")
                else:
                    print(f"  ❌ Bad imports: 0")
        
        if not entities:  # Show totals only when showing all entities
            total_good = sum(stats['good'] for stats in self.stats.values())
            total_bad = sum(sum(stats['bad'].values()) for stats in self.stats.values())
            total_skipped = sum(stats['skipped'] for stats in self.stats.values())
            print(f"\n🎯 TOTALS: {total_good} good, {total_bad} bad, {total_skipped} skipped")