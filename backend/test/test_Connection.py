import sys
from pathlib import Path

# Add the parent directory to the path so we can import from backend
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from backend.database.Connection import Connection
import time

def run_tests():
    print("--- Starting Database Connection Test ---")

    # 1. Instantiate the Connection class (Connection attempt happens here)
    db_connector = Connection()
    
    time.sleep(1) # Give a moment for connection attempt and printout

    if db_connector.status == 'active':
        print("\nConnection is ACTIVE. Proceeding with tests.")
        
        # --- TEST 1: Create Table ---
        print("\n--- Test 1: Creating 'stock_metrics' Table ---")
        result_create = db_connector.query_create_table("stock_metrics")
        print(f"Table Creation Result: {result_create}")
        
        # --- TEST 2: Insert Data (CREATE) ---
        print("\n--- Test 2: Inserting a New Record ---")
        # Ensure your keyword arguments match the columns in query_create_table
        insert_data = {
            'ticker': 'TSLA',
            'metric': 'open',
            'mean': 200.50,
            'median': 201.00,
            'std': 5.5,
            'low': 195.00,
            'max': 210.00,
            'count': 100
        }
        result_insert = db_connector.query_submit(table_name="stock_metrics", **insert_data)
        print(f"Insertion Status Code: {result_insert}")

        # --- TEST 3: Extract Data (READ) ---
        print("\n--- Test 3: Extracting All Data ---")
        all_data = db_connector.get_table_data("stock_metrics")
        print(f"Extracted Records ({len(all_data)} total):")
        for record in all_data:
            print(f"  > {record}")
            
        # --- TEST 4: Show Tables ---
        print("\n--- Test 4: Show Tables ---")
        tables = db_connector.show_tables()
        print(f"Tables in '{db_connector.database}': {tables}")


    else:
        print("\nConnection is INACTIVE. Cannot proceed. Check MySQL server/credentials.")

    # 3. Close the connection
    db_connector.close()
    print("\n--- Database Test Complete ---")


if __name__ == "__main__":
    run_tests()