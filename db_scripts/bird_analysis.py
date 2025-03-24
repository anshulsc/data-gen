import json
import re
import os
from collections import Counter, defaultdict

def load_bird_data(file_path):
    """Load BIRD dataset from JSON file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        bird_data = json.load(f)
    return bird_data

def extract_join_info(sql_query):
    """Extract JOIN information from SQL query"""
    # Check if JOIN exists in the query
    has_join = bool(re.search(r'\bjoin\b', sql_query, re.IGNORECASE))
    
    # Extract tables being joined
    tables = re.findall(r'FROM\s+([^\s,]+)(?:\s+AS\s+([^\s,]+))?|JOIN\s+([^\s,]+)(?:\s+AS\s+([^\s,]+))?', 
                        sql_query, re.IGNORECASE)
    
    # Process table names
    table_names = []
    for t in tables:
        # Extract actual table name (either direct or AS alias)
        if t[0]:  # FROM clause
            table_names.append(t[0])
        elif t[2]:  # JOIN clause
            table_names.append(t[2])
    
    # Extract join conditions
    join_conditions = re.findall(r'ON\s+([^WHERE]+)', sql_query, re.IGNORECASE)
    
    # Count the number of tables involved
    table_count = len(set(table_names))
    
    return {
        'has_join': has_join,
        'table_names': table_names,
        'join_conditions': join_conditions,
        'table_count': table_count
    }

def analyze_bird_dataset(data):
    """Analyze BIRD dataset to find multi-table databases"""
    # Initialize data structures
    db_join_counter = Counter()
    db_table_counter = defaultdict(set)
    db_join_details = defaultdict(list)
    db_table_frequency = defaultdict(lambda: Counter())
    db_table_pairs = defaultdict(lambda: Counter())
    
    # Analyze each query
    for entry in data:
        db_id = entry.get('db_id')
        sql = entry.get('SQL', '')
        question = entry.get('question', '')
        
        if not db_id or not sql:
            continue
        
        # Extract join information
        join_info = extract_join_info(sql)
        
        # Track all tables mentioned in all queries
        table_names = join_info['table_names']
        for table in table_names:
            db_table_counter[db_id].add(table)
            db_table_frequency[db_id][table] += 1
        
        # Track table pairs in joins
        if join_info['has_join'] and len(table_names) > 1:
            db_join_counter[db_id] += 1
            
            # Store join details
            db_join_details[db_id].append({
                'sql': sql,
                'question': question,
                'tables': table_names,
                'join_conditions': join_info['join_conditions'],
                'table_count': join_info['table_count']
            })
            
            # Track pairs of tables that are joined
            for i in range(len(table_names)):
                for j in range(i+1, len(table_names)):
                    table_pair = tuple(sorted([table_names[i], table_names[j]]))
                    db_table_pairs[db_id][table_pair] += 1
    
    return {
        'db_join_counter': db_join_counter,
        'db_table_counter': {k: len(v) for k, v in db_table_counter.items()},
        'db_unique_tables': {k: list(v) for k, v in db_table_counter.items()},
        'db_table_frequency': {k: dict(v) for k, v in db_table_frequency.items()},
        'db_table_pairs': {db_id: {str(pair): count for pair, count in pairs.items()} 
                          for db_id, pairs in db_table_pairs.items()},
        'db_join_details': db_join_details
    }

def rank_databases(analysis_results):
    """Rank databases by join complexity and frequency"""
    join_counter = analysis_results['db_join_counter']
    table_counter = analysis_results['db_table_counter']
    join_details = analysis_results['db_join_details']
    table_frequency = analysis_results['db_table_frequency']
    table_pairs = analysis_results['db_table_pairs']
    
    # Calculate a score based on join frequency and table count
    db_scores = {}
    for db_id in join_counter:
        join_freq = join_counter[db_id]
        unique_tables = table_counter.get(db_id, 0)
        
        # Calculate average tables per join
        total_tables = sum(detail['table_count'] for detail in join_details[db_id])
        avg_tables = total_tables / len(join_details[db_id]) if join_details[db_id] else 0
        
        # Get most frequently used tables
        most_common_tables = sorted(
            table_frequency[db_id].items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:5]  # Top 5 tables
        
        # Get most frequently joined table pairs
        most_common_pairs = []
        if db_id in table_pairs:
            most_common_pairs = sorted(
                [(eval(k), v) for k, v in table_pairs[db_id].items()],
                key=lambda x: x[1],
                reverse=True
            )[:5]  # Top 5 pairs
        
        # Score = join frequency * average tables per join
        db_scores[db_id] = {
            'score': join_freq * avg_tables,
            'join_count': join_freq,
            'unique_tables': unique_tables,
            'avg_tables_per_join': avg_tables,
            'most_common_tables': most_common_tables,
            'most_common_pairs': [{"tables": list(pair), "count": count} for pair, count in most_common_pairs]
        }
    
    # Sort databases by score
    ranked_dbs = sorted(db_scores.items(), key=lambda x: x[1]['score'], reverse=True)
    
    return ranked_dbs

def export_results_to_json(analysis_results, ranked_dbs, output_file):
    """Export all analysis results to a comprehensive JSON file"""
    export_data = {
        # Overall statistics
        'analysis_summary': {
            'total_databases_with_joins': len(analysis_results['db_join_counter']),
            'total_join_queries': sum(analysis_results['db_join_counter'].values()),
        },
        
        # Ranked databases
        'ranked_databases': [{
            'db_id': db_id,
            'score': stats['score'],
            'join_count': stats['join_count'],
            'unique_tables': stats['unique_tables'],
            'avg_tables_per_join': stats['avg_tables_per_join'],
            'most_common_tables': [{"table": table, "count": count} for table, count in stats['most_common_tables']],
            'most_common_joined_pairs': stats['most_common_pairs']
        } for db_id, stats in ranked_dbs],
        
        # Detailed database information 
        'database_details': {
            db_id: {
                'unique_tables': analysis_results['db_unique_tables'].get(db_id, []),
                'table_frequency': analysis_results['db_table_frequency'].get(db_id, {}),
                'table_pairs': analysis_results['db_table_pairs'].get(db_id, {}),
                'join_examples': [
                    {
                        'question': detail['question'],
                        'sql': detail['sql'],
                        'tables_involved': detail['tables'],
                        'join_conditions': detail['join_conditions']
                    }
                    for detail in analysis_results['db_join_details'].get(db_id, [])[:10]  # Limit to 10 examples
                ]
            }
            for db_id in analysis_results['db_join_counter']
        }
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(export_data, f, indent=2)
    
    return export_data

def main():
    # File paths
    input_file = "/Users/anshulsingh/Hamburg/experiments/benchmarks/data_gen/raw_data/BIRD/train/train.json"  # Replace with your file path
    output_dir = "bird_analysis"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Load data
    print(f"Loading BIRD data from {input_file}...")
    bird_data = load_bird_data(input_file)
    print(f"Loaded {len(bird_data)} queries.")
    
    # Analyze data
    print("Analyzing queries for JOIN operations and table usage...")
    analysis_results = analyze_bird_dataset(bird_data)
    
    # Rank databases
    print("Ranking databases by multi-table complexity...")
    ranked_dbs = rank_databases(analysis_results)
    
    # Export all results to JSON
    output_json = os.path.join(output_dir, "bird_multitable_analysis.json")
    print(f"Exporting comprehensive analysis to {output_json}...")
    export_results_to_json(analysis_results, ranked_dbs, output_json)
    
    # Output summary
    join_dbs = len(analysis_results['db_join_counter'])
    print(f"\nFound {join_dbs} databases with JOIN operations.")
    if ranked_dbs:
        top_db, top_stats = ranked_dbs[0]
        print(f"Top multi-table database: '{top_db}' with {top_stats['join_count']} JOIN queries")
        print(f"Most frequently used tables in {top_db}:")
        for table, count in top_stats['most_common_tables']:
            print(f"  - {table}: {count} occurrences")
    
    print(f"\nAll analysis results have been exported to {output_json}")
    print("You can use this JSON file to identify the best databases for creating multi-table QA datasets.")
    print("\nAnalysis complete!")

if __name__ == "__main__":
    main()