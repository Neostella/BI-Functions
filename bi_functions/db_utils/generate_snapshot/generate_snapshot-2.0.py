import os
import subprocess
from settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, OUTPUT_DIR, DB_SCHEMA


def clear_output_directory(directory):
    """
    Clear all files in the output directory.
    """
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                print('Old files removed')
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")

def ensure_output_directory():
    """
    Ensure the output directory exists and is empty.
    """
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    else:
        # Clear the directory
        for filename in os.listdir(OUTPUT_DIR):
            file_path = os.path.join(OUTPUT_DIR, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)

def dump_view(view_name):
    """
    Dump the definition of a specific view using pg_dump and save it to a file.
    """
    output_file = os.path.join(OUTPUT_DIR, f"{view_name}.sql")
    command = [
        "pg_dump",
        '-h', DB_HOST,
        '-p', str(DB_PORT),
        '-U', DB_USER,
        '-d', DB_NAME,
        '-s',  # Schema only
        '-n', DB_SCHEMA,  # Specify the schema
        '-t', f"{DB_SCHEMA}.{view_name}",  # Include schema in the table name
        '-f', output_file
    ]

    os.environ['PGPASSWORD'] = DB_PASSWORD
    
    try:
        subprocess.run(command, check=True)
        
    except subprocess.CalledProcessError as e:
        print(f"Error dumping view {view_name}: {e}")

def get_views():
    """
    Fetch all view names from the PostgreSQL database using psql.
    """
    os.environ['PGPASSWORD'] = DB_PASSWORD
    command = [
        "psql",
        f"-h{DB_HOST}",
        f"-p{DB_PORT}",
        f"-d{DB_NAME}",
        f"-U{DB_USER}",
        "-t",  # Only print the tuples
        "-A",  # Unaligned output mode
        "-c",
        f"SELECT table_name FROM information_schema.views WHERE table_schema IN ('{DB_SCHEMA}');"
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        views = result.stdout.strip().split('\n')
        return views
    except subprocess.CalledProcessError as e:
        print(f"Error fetching views: {e}")
        return []

def main():


    ensure_output_directory()
    print(f"Cleaning folder {OUTPUT_DIR}")
    # Clear the output directory
    clear_output_directory(OUTPUT_DIR)

    views = get_views()
    if not views:
        print("No views found in the database.")
        return
    
    print(f"Dumping snapshots into: {OUTPUT_DIR}")
    for view in views:
        dump_view(view)
    print(f"Snapshots dump end")

if __name__ == "__main__":
    main()
