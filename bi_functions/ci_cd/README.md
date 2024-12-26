# README

## Prerequisites

Before using the `generate_changeset.py` script, ensure you have the following prerequisites installed:

1. **Python**: Make sure Python 3.x is installed on your system.
2. **Git**: Ensure Git is installed and configured.
3. **Liquibase**: Install Liquibase for database version control.
4. **Required Python Packages**: Install the required Python packages using `pip`.

## Setup

1. **Clone the Repository**:
    ```sh
    git clone <repository-url>
    cd <repository-directory>
    ```

2. **Install Python Dependencies**:
    ```sh
    pip install -r requirements.txt
    ```

3. **Set Environment Variables**:
    Ensure the following environment variables are set:
    - `ROOT_DIR`: The root directory of the repository.
    - `SQL_FILES_PATH`: The path to the SQL files.
    - `CHANGESET_DIR`: The directory where changesets will be stored.
    - `GIT_USERNAME`: Your Git username.
    - `GIT_EMAIL`: Your Git email.

## Usage

### Generate SQL Changesets

To generate SQL changesets, use the `generate_changeset.py` script with the `--branch` argument to specify the branch to compare against the `main` branch.

    ```sh
    python generate_changeset.py --branch <branch-name>
    ```

### Deploy Changesets

To deploy the generated changesets, use the `--deploy` argument.

    ```sh
    python generate_changeset.py --deploy
    ```

### Test Changesets

To test the changesets, use the `--test` argument.

    ```sh
    python generate_changeset.py --test
    ```

### Commit and Push Changes

To commit and push the generated changesets, use the `--commit` argument with a commit message.

    ```sh
    python generate_changeset.py --commit "Your commit message"
    ```

## Example

Here is an example of generating changesets for a branch named `feature-branch`:

    ```sh
    python generate_changeset.py --branch feature-branch
    ```

This will:
1. Fetch the list of modified SQL files compared to the `main` branch.
2. Generate Liquibase changesets for the modified SQL files.
3. Commit and push the changesets to the current branch.

## Troubleshooting

If you encounter any issues, ensure that:
- All prerequisites are installed.
- Environment variables are correctly set.
- You have the necessary permissions to run Git and Liquibase commands.

For further assistance, refer to the documentation or contact the repository maintainer.

This README provides a comprehensive guide to using the `generate_changeset.py` script, including prerequisites, setup, usage, and troubleshooting steps.