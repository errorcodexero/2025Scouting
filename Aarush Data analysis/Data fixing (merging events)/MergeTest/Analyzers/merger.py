import sqlite3
import pandas as pd
import argparse
import os
import sys

def get_primary_keys(conn, table_name):
    """Returns a list of primary key columns for a given table."""
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cursor.fetchall() if row[5] == 1]  # row[5] == 1 means it's a PK

def merge_tables_by_key(df_priority, df_fallback, primary_keys):
    """Merge two DataFrames using primary keys, prioritizing non-null values from df_priority."""
    df_priority = df_priority.set_index(primary_keys)
    df_fallback = df_fallback.set_index(primary_keys)

    # Outer join on index, then prioritize non-null values from priority
    df_merged = df_priority.combine_first(df_fallback).reset_index()
    return df_merged

def merge_databases(priority_db_path, fallback_db_path, output_db_path):
    # Validate paths
    for path in [priority_db_path, fallback_db_path]:
        if not os.path.exists(path):
            print(f"❌ File not found: {path}")
            sys.exit(1)

    # Connect to databases
    conn_priority = sqlite3.connect(priority_db_path)
    conn_fallback = sqlite3.connect(fallback_db_path)
    conn_output = sqlite3.connect(output_db_path)

    # Get table names from priority DB
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn_priority)
    table_names = tables['name'].tolist()

    for table in table_names:
        print(f"\n🔄 Merging table: {table}")

        try:
            df_priority = pd.read_sql(f"SELECT * FROM {table}", conn_priority)
            df_fallback = pd.read_sql(f"SELECT * FROM {table}", conn_fallback)

            # Check schema match
            if set(df_priority.columns) != set(df_fallback.columns):
                print(f"⚠️ Skipping table '{table}' due to schema mismatch.")
                continue

            # Get primary keys
            primary_keys = get_primary_keys(conn_priority, table)
            if not primary_keys:
                if table.lower() == "matches":
                    primary_keys = ["key", "team_key"]
                    print(f"⚠️ No primary key found for '{table}', using fallback keys: {primary_keys}")
                else:
                    print(f"⚠️ Skipping table '{table}' — no primary key found.")
                    continue


            # Merge by primary key
            df_merged = merge_tables_by_key(df_priority, df_fallback, primary_keys)

            # Write to output DB
            df_merged.to_sql(table, conn_output, if_exists='replace', index=False)
            print(f"✅ Merged {len(df_merged)} rows into '{table}'")

        except Exception as e:
            print(f"❌ Error merging table '{table}': {e}")

    # Close connections
    conn_priority.close()
    conn_fallback.close()
    conn_output.close()
    print(f"\n🎉 Merge complete. Output saved to: {output_db_path}")

def main():
    parser = argparse.ArgumentParser(description="Merge two SQLite databases with priority override.")
    parser.add_argument("--priority", required=True, help="Path to the priority database file")
    parser.add_argument("--fallback", required=True, help="Path to the fallback database file")
    parser.add_argument("--output", required=True, help="Path to save the merged database file")

    args = parser.parse_args()
    merge_databases(args.priority, args.fallback, args.output)

if __name__ == "__main__":
    main()