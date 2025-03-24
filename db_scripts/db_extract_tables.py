import sqlite3
import json
import os
import argparse
from collections import defaultdict, deque
from termcolor import cprint
import random
import traceback

def get_foreign_key_relationships(cursor):
    """Get all foreign key relationships in the database"""
    relationships = []
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    for table_tuple in tables:
        table_name = table_tuple[0]
        try:
            cursor.execute(f"PRAGMA foreign_key_list({table_name});")
            foreign_keys = cursor.fetchall()
            
            for fk in foreign_keys:
                if len(fk) >= 8:  # Ensure we have enough elements in the tuple
                    relationships.append({
                        'table': table_name,
                        'id': fk[0],
                        'seq': fk[1],
                        'ref_table': fk[2],
                        'from_col': fk[3],
                        'to_col': fk[4],
                        'on_update': fk[5],
                        'on_delete': fk[6],
                        'match': fk[7]
                    })
        except sqlite3.Error as e:
            cprint(f"Error getting foreign keys for table {table_name}: {e}", "red")
    
    return relationships

def get_primary_keys(cursor, table_name):
    """Get primary key columns for a table"""
    try:
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        primary_keys = [col[1] for col in columns if col[5] == 1]  # Column is PK if the 6th item is 1
        
        # If no primary key is defined, use the first column as a surrogate key
        if not primary_keys and columns:
            primary_keys = [columns[0][1]]
            
        return primary_keys
    except sqlite3.Error as e:
        cprint(f"Error getting primary keys for table {table_name}: {e}", "red")
        return []

def get_table_columns(cursor, table_name):
    """Get all column names for a table"""
    try:
        cursor.execute(f"PRAGMA table_info({table_name});")
        return [col[1] for col in cursor.fetchall()]
    except sqlite3.Error as e:
        cprint(f"Error getting columns for table {table_name}: {e}", "red")
        return []

def get_column_index(columns, column_name):
    """Get the index of a column in a list of columns"""
    try:
        return columns.index(column_name)
    except ValueError:
        return -1

def build_bidirectional_relationship_map(relationships):
    """
    Build a comprehensive map of all relationships between tables,
    including both directions (parent->child and child->parent)
    """
    rel_map = defaultdict(list)
    
    for rel in relationships:
        source_table = rel['table']
        target_table = rel['ref_table']
        source_col = rel['from_col']
        target_col = rel['to_col']
        
        # Only add valid relationships
        if source_table and target_table and source_col and target_col:
            # Add forward relationship (child -> parent)
            rel_map[source_table].append({
                'related_table': target_table,
                'local_column': source_col,
                'remote_column': target_col,
                'direction': 'outgoing'  # This table references the other
            })
            
            # Add reverse relationship (parent -> child)
            rel_map[target_table].append({
                'related_table': source_table,
                'local_column': target_col,
                'remote_column': source_col,
                'direction': 'incoming'  # This table is referenced by the other
            })
    
    return rel_map

def extract_related_samples(conn, relationships, max_rows=500):
    """Extract connected samples across tables maintaining referential integrity"""
    cursor = conn.cursor()
    tables_data = {}
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    all_tables = [table[0] for table in cursor.fetchall() if not table[0].startswith('sqlite_')]
    
    # Build relationship map
    relationship_map = build_bidirectional_relationship_map(relationships)
    
    # Find a good starting table (one with most relationships if possible)
    if relationship_map:
        start_table = max(relationship_map.keys(), key=lambda t: len(relationship_map[t]))
        cprint(f"Starting with table '{start_table}' which has {len(relationship_map[start_table])} relationships", "cyan")
    else:
        if all_tables:
            start_table = all_tables[0]
            cprint(f"No valid relationships found, starting with first table: {start_table}", "yellow")
        else:
            cprint("No tables found in the database", "red")
            return {}
    
    # Dictionary to track processed tables and their sampling status
    processed = {}
    for table in all_tables:
        processed[table] = {
            'sampled': False,
            'method': None,
            'related_to': []
        }
    
    # Dictionary to store column information and key values for each table
    table_info = {}
    for table in all_tables:
        columns = get_table_columns(cursor, table)
        primary_keys = get_primary_keys(cursor, table)
        
        table_info[table] = {
            'columns': columns,
            'primary_keys': primary_keys,
            'selected_values': defaultdict(set),  # To track values we've already selected
            'column_indices': {col: idx for idx, col in enumerate(columns)}
        }
    
    # Start with sampling from our starting table
    cursor.execute(f"SELECT COUNT(*) FROM {start_table};")
    start_table_count = cursor.fetchone()[0]
    
    cprint(f"Sampling from start table '{start_table}' (total rows: {start_table_count})", "blue")
    
    if start_table_count > max_rows:
        cursor.execute(f"SELECT * FROM {start_table} ORDER BY RANDOM() LIMIT {max_rows};")
    else:
        cursor.execute(f"SELECT * FROM {start_table};")
    
    start_rows = cursor.fetchall()
    
    # Store the sampled data
    tables_data[start_table] = {
        "columns": table_info[start_table]['columns'],
        "data": start_rows,
        "row_count": len(start_rows),
        "total_rows": start_table_count,
        "sampled": start_table_count > max_rows,
        "sampling_method": "initial_random"
    }
    
    # Mark as processed
    processed[start_table] = {
        'sampled': True,
        'method': 'initial_random',
        'related_to': []
    }
    
    # Extract primary key values from the sample
    for row in start_rows:
        for pk in table_info[start_table]['primary_keys']:
            col_idx = table_info[start_table]['column_indices'].get(pk, -1)
            if col_idx >= 0 and col_idx < len(row):
                table_info[start_table]['selected_values'][pk].add(row[col_idx])
    
    # Process relationships using BFS
    queue = deque([start_table])
    seen_edges = set()  # Track processed relationships to avoid cycles
    
    while queue:
        current_table = queue.popleft()
        
        # Get relationships for current table
        relationships_for_table = relationship_map.get(current_table, [])
        
        cprint(f"Processing relationships for '{current_table}' ({len(relationships_for_table)} relationships)", "cyan")
        
        for rel in relationships_for_table:
            related_table = rel['related_table']
            edge_key = f"{current_table}:{rel['local_column']}:{related_table}:{rel['remote_column']}"
            
            # Skip if we've already processed this exact relationship
            if edge_key in seen_edges:
                continue
                
            seen_edges.add(edge_key)
            
            # Don't bother with sqlite internal tables
            if related_table.startswith('sqlite_'):
                continue
            
            # Check if we already have data for the current table
            if current_table not in tables_data:
                cprint(f"Missing data for table '{current_table}', skipping relationship", "yellow")
                continue
                
            current_rows = tables_data[current_table]['data']
            
            try:
                local_col = rel['local_column']
                remote_col = rel['remote_column']
                direction = rel['direction']
                
                local_col_idx = table_info[current_table]['column_indices'].get(local_col, -1)
                
                if local_col_idx == -1:
                    cprint(f"Column '{local_col}' not found in '{current_table}'", "yellow")
                    continue
                
                # Extract values from the current table that we'll use to find related rows
                join_values = set()
                for row in current_rows:
                    if local_col_idx < len(row) and row[local_col_idx] is not None:
                        join_values.add(row[local_col_idx])
                
                if not join_values:
                    cprint(f"No join values found for {current_table}.{local_col} -> {related_table}.{remote_col}", "yellow")
                    continue
                
                cprint(f"Found {len(join_values)} join values for {current_table}.{local_col} -> {related_table}.{remote_col}", "blue")
                
                # Get already selected rows for the related table
                already_selected_rows = []
                if related_table in tables_data:
                    already_selected_rows = tables_data[related_table]['data']
                
                # Count total rows in the related table
                cursor.execute(f"SELECT COUNT(*) FROM {related_table};")
                related_table_count = cursor.fetchone()[0]
                
                # If the related table has already been fully sampled, just skip
                if processed.get(related_table, {}).get('sampled', False):
                    cprint(f"Table '{related_table}' already sampled, updating related_to", "yellow")
                    processed[related_table]['related_to'].append(current_table)
                    continue
                
                # Prepare query to get related rows
                join_values_list = list(join_values)
                placeholders = ','.join(['?' for _ in join_values_list])
                
                # Get rows that match our join values
                try:
                    query = f"SELECT * FROM {related_table} WHERE {remote_col} IN ({placeholders})"
                    cursor.execute(query, join_values_list)
                    related_rows = cursor.fetchall()
                    
                    cprint(f"Found {len(related_rows)} related rows in '{related_table}'", "green")
                    
                    # If we have too many related rows, take a random sample
                    if len(related_rows) > max_rows:
                        related_rows = random.sample(related_rows, max_rows)
                        sampling_method = "related_random_sample"
                    elif len(related_rows) == 0:
                        # If no related rows found through FK relationship, take random sample
                        cursor.execute(f"SELECT * FROM {related_table} ORDER BY RANDOM() LIMIT {max_rows};")
                        related_rows = cursor.fetchall()
                        sampling_method = "fallback_random"
                    elif len(related_rows) < max_rows:
                        # We need more rows to meet the target - get additional random rows
                        # But first exclude the IDs we already have
                        additional_needed = max_rows - len(related_rows)
                        
                        # Get the primary keys for this table
                        primary_keys = table_info[related_table]['primary_keys']
                        
                        if primary_keys:
                            pk = primary_keys[0]  # Use the first PK for simplicity
                            pk_idx = table_info[related_table]['column_indices'].get(pk, -1)
                            
                            if pk_idx >= 0:
                                # Extract the PKs we already have
                                existing_pks = [row[pk_idx] for row in related_rows if pk_idx < len(row) and row[pk_idx] is not None]
                                
                                if existing_pks:
                                    # Get additional rows excluding ones we already have
                                    exclusion_placeholders = ','.join(['?' for _ in existing_pks])
                                    additional_query = f"""
                                        SELECT * FROM {related_table} 
                                        WHERE {pk} NOT IN ({exclusion_placeholders})
                                        ORDER BY RANDOM() LIMIT {additional_needed}
                                    """
                                    
                                    cursor.execute(additional_query, existing_pks)
                                    additional_rows = cursor.fetchall()
                                    related_rows.extend(additional_rows)
                                    
                                    cprint(f"Added {len(additional_rows)} additional random rows to '{related_table}'", "blue")
                                else:
                                    # Just get random rows if we couldn't extract PKs
                                    cursor.execute(f"SELECT * FROM {related_table} ORDER BY RANDOM() LIMIT {additional_needed};")
                                    additional_rows = cursor.fetchall()
                                    related_rows.extend(additional_rows)
                            else:
                                # Just get random rows if we couldn't find PK column
                                cursor.execute(f"SELECT * FROM {related_table} ORDER BY RANDOM() LIMIT {additional_needed};")
                                additional_rows = cursor.fetchall()
                                related_rows.extend(additional_rows)
                        else:
                            # Just get random rows if no PK defined
                            cursor.execute(f"SELECT * FROM {related_table} ORDER BY RANDOM() LIMIT {additional_needed};")
                            additional_rows = cursor.fetchall()
                            related_rows.extend(additional_rows)
                        
                        sampling_method = "related_with_random_supplement"
                    else:
                        sampling_method = "related_exact"
                    
                    # Store the sampled data
                    tables_data[related_table] = {
                        "columns": table_info[related_table]['columns'],
                        "data": related_rows,
                        "row_count": len(related_rows),
                        "total_rows": related_table_count,
                        "sampled": related_table_count > len(related_rows),
                        "sampling_method": sampling_method
                    }
                    
                    # Mark as processed
                    processed[related_table] = {
                        'sampled': True,
                        'method': sampling_method,
                        'related_to': [current_table]
                    }
                    
                    # Add to queue for further processing if not already in queue
                    if related_table not in queue:
                        queue.append(related_table)
                    
                    # Extract primary key values from the sample for future reference
                    for row in related_rows:
                        for pk in table_info[related_table]['primary_keys']:
                            col_idx = table_info[related_table]['column_indices'].get(pk, -1)
                            if col_idx >= 0 and col_idx < len(row) and row[col_idx] is not None:
                                table_info[related_table]['selected_values'][pk].add(row[col_idx])
                
                except sqlite3.Error as e:
                    cprint(f"Error querying related rows for {related_table}: {e}", "red")
                    traceback.print_exc()
            
            except Exception as e:
                cprint(f"Unexpected error processing relationship: {e}", "red")
                traceback.print_exc()
    
    # Process any remaining tables that weren't reached through relationships
    for table in all_tables:
        if not processed.get(table, {}).get('sampled', False):
            try:
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table};")
                row_count = cursor.fetchone()[0]
                
                # Get random sample
                sample_size = min(max_rows, row_count)
                cursor.execute(f"SELECT * FROM {table} ORDER BY RANDOM() LIMIT {sample_size};")
                rows = cursor.fetchall()
                
                tables_data[table] = {
                    "columns": table_info[table]['columns'],
                    "data": rows,
                    "row_count": len(rows),
                    "total_rows": row_count,
                    "sampled": row_count > sample_size,
                    "sampling_method": "random_unrelated"
                }
                
                processed[table] = {
                    'sampled': True,
                    'method': 'random_unrelated',
                    'related_to': []
                }
                
                cprint(f"Added random sample for table '{table}' ({len(rows)} rows)", "yellow")
            except Exception as e:
                cprint(f"Error processing table {table}: {e}", "red")
    
    # Add processing summary to the result
    sampling_summary = {table: info for table, info in processed.items()}
    relationship_summary = {table: [rel['related_table'] for rel in rels] 
                           for table, rels in relationship_map.items()}
    
    tables_data['__meta__'] = {
        "sampling_summary": sampling_summary,
        "table_relationships": relationship_summary
    }
    
    return tables_data

def sqlite_to_connected_json_files(db_path, output_root, max_rows=500):
    base_name = os.path.basename(db_path)
    
    output_dir = os.path.join(output_root, os.path.splitext(base_name)[0])
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        cprint(f"Created directory: {output_dir}", "green")
    
    try:
        conn = sqlite3.connect(db_path)
        conn.text_factory = lambda x: x.decode("utf-8", "replace")
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in cursor.fetchall()]
        
        # Get foreign key relationships
        relationships = get_foreign_key_relationships(cursor)
        
        if not relationships:
            cprint(f"No foreign key relationships found in {db_path}. Using direct extraction.", "yellow")
            
            # Fall back to simple random sampling for each table
            for table_name in tables:
                try:
                    cursor.execute(f"PRAGMA table_info({table_name});")
                    columns_info = cursor.fetchall()
                    columns = [col[1] for col in columns_info]
                    
                    # Get row count
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                    row_count = cursor.fetchone()[0]
                    
                    # Extract rows
                    row_limit = min(max_rows, row_count)
                    
                    cursor.execute(f"SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT {row_limit};")
                    rows = cursor.fetchall()
                    
                    table_json = {
                        "columns": columns,
                        "data": rows,
                        "total_rows": row_count,
                        "sampled": row_count > row_limit,
                        "sampling_method": "random"
                    }
                    
                    output_file = os.path.join(output_dir, f"{table_name}.json")
                    with open(output_file, "w", encoding="utf-8") as f:
                        json.dump(table_json, f, indent=4)
                    
                    cprint(f"Exported table '{table_name}' with {len(rows)} rows to {output_file}", "blue")
                except Exception as e:
                    cprint(f"Error processing table {table_name}: {e}", "red")
        else:
            # Use the enhanced sampling to maintain relationships
            cprint(f"Found {len(relationships)} foreign key relationships. Extracting related samples...", "green")
            
            try:
                # Get related samples
                tables_data = extract_related_samples(conn, relationships, max_rows)
                
                # Export each table's data
                for table_name, table_info in tables_data.items():
                    # Skip meta information which is handled separately
                    if table_name == '__meta__':
                        continue
                        
                    output_file = os.path.join(output_dir, f"{table_name}.json")
                    with open(output_file, "w", encoding="utf-8") as f:
                        json.dump(table_info, f, indent=4)
                    
                    sampling_method = table_info.get("sampling_method", "unknown")
                    cprint(f"Exported table '{table_name}' with {table_info['row_count']} rows (method: {sampling_method}) to {output_file}", "blue")
                
                # Export relationships and metadata
                if '__meta__' in tables_data:
                    metadata = {
                        "relationships": relationships,
                        "extraction_info": tables_data['__meta__'],
                        "max_rows_per_table": max_rows
                    }
                else:
                    metadata = {
                        "relationships": relationships,
                        "max_rows_per_table": max_rows
                    }
                
                metadata_file = os.path.join(output_dir, "metadata.json")
                with open(metadata_file, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, indent=4)
                
                cprint(f"Exported relationship metadata to {metadata_file}", "green")
                
            except Exception as e:
                cprint(f"Error during related extraction: {e}", "red")
                traceback.print_exc()
                cprint("Falling back to basic extraction...", "yellow")
                
                # Fall back to basic extraction
                for table_name in tables:
                    try:
                        cursor.execute(f"PRAGMA table_info({table_name});")
                        columns_info = cursor.fetchall()
                        columns = [col[1] for col in columns_info]
                        
                        # Get row count
                        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                        row_count = cursor.fetchone()[0]
                        
                        # Get random sample of rows
                        cursor.execute(f"SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT {max_rows};")
                        rows = cursor.fetchall()
                        
                        table_json = {
                            "columns": columns,
                            "data": rows,
                            "total_rows": row_count,
                            "sampled": row_count > max_rows,
                            "sampling_method": "fallback_random"
                        }
                        
                        output_file = os.path.join(output_dir, f"{table_name}.json")
                        with open(output_file, "w", encoding="utf-8") as f:
                            json.dump(table_json, f, indent=4)
                        
                        cprint(f"Exported table '{table_name}' with {len(rows)} rows to {output_file}", "blue")
                    except Exception as table_error:
                        cprint(f"Failed to process table {table_name}: {table_error}", "red")
    
    except Exception as e:
        cprint(f"Failed to process database {db_path}: {e}", "red")
        traceback.print_exc()
    
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def process_dataset_folder(dataset_folder, output_folder, db_id=None, max_rows=500):
    if not os.path.isdir(dataset_folder):
        cprint(f"Error: {dataset_folder} is not a valid directory.", "red")
        return

    if db_id:
        found = False
        for subdir in os.listdir(dataset_folder):
            subdir_path = os.path.join(dataset_folder, subdir)
            if os.path.isdir(subdir_path):
                db_file = f"{db_id}.sqlite"
                db_path = os.path.join(subdir_path, db_file)
                
                if os.path.exists(db_path):
                    cprint(f"Found and processing database: {db_path}", "magenta")
                    sqlite_to_connected_json_files(db_path, output_folder, max_rows)
                    found = True
                    break
        
        if not found:
            cprint(f"Database with ID '{db_id}' not found in any subdirectory.", "red")
    else:
        for subdir in os.listdir(dataset_folder):
            subdir_path = os.path.join(dataset_folder, subdir)
            if os.path.isdir(subdir_path):
                sqlite_files = [f for f in os.listdir(subdir_path) if f.endswith('.sqlite')]
                if not sqlite_files:
                    cprint(f"No SQLite files found in {subdir_path}", "yellow")
                    continue
                for sqlite_file in sqlite_files:
                    db_path = os.path.join(subdir_path, sqlite_file)
                    cprint(f"Processing database: {db_path}", "magenta")
                    sqlite_to_connected_json_files(db_path, output_folder, max_rows)
            else:
                cprint(f"Skipping non-directory: {subdir_path}", "yellow")

def main():
    parser = argparse.ArgumentParser(
        description="Extract related samples from SQLite databases to JSON files while preserving relationships."
    )
    parser.add_argument(
        "--dataset_folder",
        "-d",
        required=True,
        help="Path to the dataset folder containing database subfolders."
    )
    parser.add_argument(
        "--output_folder",
        "-o",
        required=True,
        help="Path to the output folder where JSON files will be stored."
    )
    parser.add_argument(
        "--db_id",
        type=str,
        help="Optional: Specific database ID to process (e.g., 'app_store'). If not provided, all databases will be processed."
    )
    parser.add_argument(
        "--max_rows",
        type=int,
        default=500,
        help="Maximum number of rows to extract per table. Default is 500."
    )
    args = parser.parse_args()
    
    process_dataset_folder(args.dataset_folder, args.output_folder, args.db_id, args.max_rows)

if __name__ == '__main__':
    main()

"""
# Process all databases with 500 row limit:
python db_extract_tables.py \
--dataset_folder /path/to/databases \
--output_folder /path/to/output \
--max_rows 500

# Process only a specific database:
python db_extract_tables.py \
--dataset_folder /path/to/databases \
--output_folder /path/to/output \
--db_id talkingdata \
--max_rows 500
"""