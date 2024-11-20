import os
import subprocess
import datetime
from git import Repo
import argparse

# Constants
DB_NAME= "fvdw_shOrgId_d226"
ROOT_DIR = 'C:/Users/juan.garcia/bi-analytics-blg/BI-Analytics-BLG'
REPO_PATH = os.path.join(ROOT_DIR)  # Repo path
CHANGESET_DIR = os.path.join(ROOT_DIR, f'changelogs/{DB_NAME}/changesets')
SQL_FILES_PATH = f'db/{DB_NAME}/views'
GIT_USERNAME = "github-actions[bot]"
GIT_EMAIL = "github-actions[bot]@users.noreply.github.com"
VIEW_OWNER = "neo-dw-support"
ENV = 'current'

def run_command(command):
    """Utility to run shell commands."""
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running command: {command}\n{result.stderr}")
        exit(1)
    return result.stdout.strip()

def get_modified_sql_files(branch_to_compare):
    """Fetch the list of modified SQL files compared to the 'main' branch."""
    run_command(f'cd {ROOT_DIR} && git checkout main')
    run_command(f'cd {ROOT_DIR} && git pull origin main:main')
    run_command(f'cd {ROOT_DIR} && git fetch --all')
    run_command(f'cd {ROOT_DIR} && git checkout {branch_to_compare}')
    run_command(f'cd {ROOT_DIR} && git fetch origin')
    run_command(f'cd {ROOT_DIR} && git rebase origin/main')
    diff_files = run_command(f'cd {ROOT_DIR} && git diff --name-only origin/main {branch_to_compare} -- {SQL_FILES_PATH}*.sql')
    return diff_files.splitlines()

def create_changeset_file(sql_file, pr_author, changeset_id, timestamp):
    """Generate a Liquibase changeset for the given SQL file."""
    filename = os.path.basename(sql_file)
    basename = os.path.splitext(filename)[0]
    timestamp_suffix = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    
    changeset_name = f"{CHANGESET_DIR}/{timestamp_suffix}_{basename}.sql"
    base_name = os.path.basename(sql_file)  # Returns 'example_view.sql'

    # Remove the extension
    view_name = os.path.splitext(base_name)[0] 

    os.makedirs(CHANGESET_DIR, exist_ok=True)
    with open(changeset_name, 'w') as f:
        f.write("--liquibase formatted sql\n")
        f.write(f"--changeset {pr_author}:{changeset_id}\n")
        f.write(f"--comment Auto-generated changeset from {sql_file}\n")
        f.write(f"--timestamp {timestamp}\n\n")
        f.write(f"-- From: {sql_file}\n")
        #f.write(f"DROP VIEW IF EXISTS {ENV}.\"{view_name}\";\n\n")
        #f.write(f"CREATE VIEW {ENV}.\"{view_name}\" AS \n\n")


        sql_file_path = os.path.join(ROOT_DIR,sql_file)
        with open(sql_file_path, 'r') as original_file:
            f.write(original_file.read())

        #f.write(f"\n\n")
        #f.write(f"ALTER VIEW {ENV}.\"{view_name}\" OWNER TO \"{VIEW_OWNER}\"; \n\n")
        # f.write(f"""
        #     GRANT ALL ON TABLE {ENV}.\"{view_name}\" TO "neo-bi-support-group";
        #     GRANT SELECT ON TABLE {ENV}.\"{view_name}\" TO "corey.maxedon";
        #     GRANT ALL ON TABLE {ENV}.\"{view_name}\" TO "neo-dw-support";
        #     GRANT SELECT ON TABLE {ENV}.\"{view_name}\" TO "data_analyst";
        #     GRANT SELECT ON TABLE {ENV}.\"{view_name}\" TO "neo-guillermogallo";
        #     GRANT ALL ON TABLE {ENV}.\"{view_name}\" TO "neo-ml-support";
        #     GRANT SELECT ON TABLE {ENV}.\"{view_name}\" TO "smriti.dewangan";
        #     GRANT SELECT ON TABLE {ENV}.\"{view_name}\" TO "sonali.mehere";
        #     """)

    # Replace SQL patterns in the generated changeset
    # run_command(
    #     f"sed -i -e 's/DROP VIEW IF EXISTS current./DROP VIEW IF EXISTS test./g' "
    #     f"-e 's/DROP VIEW current./DROP VIEW IF EXISTS test./g' "
    #     f"-e 's/CREATE VIEW current./CREATE VIEW test./g' "
    #     f"-e 's/ALTER VIEW current./ALTER VIEW test./g' "
    #     f"-e 's/GRANT ALL ON TABLE current./GRANT ALL ON TABLE test./g' {changeset_name}"
    # )

def commit_and_push_changes():
    """Commit and push the generated changesets to the current branch."""
    run_command(f'git config --global user.name "{GIT_USERNAME}"')
    run_command(f'git config --global user.email "{GIT_EMAIL}"')
    run_command(f'git add {CHANGESET_DIR}/*.sql')
    run_command(f'git commit -m "view deployed Auto-generate changesets for SQL deployment"')
    run_command('git push origin HEAD')

def main(branch):
    repo = Repo(REPO_PATH)
    current_branch = repo.active_branch.name

    modified_sql_files = get_modified_sql_files(branch)

    if not modified_sql_files:
        print("No modified SQL files found.")
        return

    pr_author = os.getenv('GITHUB_ACTOR', 'unknown')
    changeset_id = os.getenv('GITHUB_RUN_ID', 'manual')
    timestamp = datetime.datetime.now().isoformat()

    for sql_file in modified_sql_files:
        create_changeset_file(sql_file, pr_author, changeset_id, timestamp)

    diff_files = run_command(f'cd {ROOT_DIR} && git diff --name-only origin/main {branch} -- {SQL_FILES_PATH}*.sql')
    print(f'git commit -m "view deployed({diff_files}) Auto-generate changesets for SQL deployment"')
    # commit_and_push_changes()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate SQL changesets.")
    parser.add_argument(
        '--branch', 
        type=str, 
        required=True, 
        help='The branch to compare against the main branch.'
    )
    args = parser.parse_args()
    main(args.branch)
