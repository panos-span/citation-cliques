import sqlite3
import sys
import os


def list_tables(db_path):
    if not os.path.exists(db_path):
        print(f"Error: Database file '{db_path}' not found.")
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Get list of tables
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
        )
        tables = cursor.fetchall()

        print(f"Database: {db_path}")
        print(f"Found {len(tables)} tables:")
        print("-" * 60)
        print(f"{'Table Name':<40} | {'Row Count':<15}")
        print("-" * 60)

        for table in tables:
            table_name = table[0]
            try:
                # Use double quotes for table name to handle special characters or keywords
                cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                count = cursor.fetchone()[0]
                print(f"{table_name:<40} | {count:<15,}")
            except sqlite3.OperationalError as e:
                print(f"{table_name:<40} | Error: {e}")

        print("-" * 60)
        conn.close()

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    else:
        db_path = "rolap.db"  # Default

    list_tables(db_path)
