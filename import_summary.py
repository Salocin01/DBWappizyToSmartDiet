from collections import defaultdict


class ImportSummary:
    """Handles import statistics tracking and reporting"""
    
    def __init__(self):
        self.stats = defaultdict(lambda: {'good': 0, 'bad': defaultdict(int)})
    
    def record_success(self, entity):
        """Record a successful import for an entity"""
        self.stats[entity]['good'] += 1
    
    def record_error(self, entity, reason):
        """Record an error for an entity with a specific reason"""
        self.stats[entity]['bad'][reason] += 1
    
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
                
                if stats['bad']:
                    print(f"  ❌ Bad imports: {sum(stats['bad'].values())}")
                    print("  Reasons:")
                    for reason, count in stats['bad'].items():
                        print(f"    - {reason}: {count}")
                else:
                    print(f"  ❌ Bad imports: 0")
        
        if not entities:  # Show totals only when showing all entities
            total_good = sum(stats['good'] for stats in self.stats.values())
            total_bad = sum(sum(stats['bad'].values()) for stats in self.stats.values())
            print(f"\n🎯 TOTALS: {total_good} good, {total_bad} bad")