```markdown
# BI Functions Documentation

Welcome to the BI Functions documentation.

## Database Utilities

### get_all_views

This function retrieves all views from a PostgreSQL database and creates a file for each view with its definition.

### Usage

```python
from bi_functions.db_utils.get_all_views import get_all_views

get_all_views()