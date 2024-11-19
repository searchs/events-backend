import sqlite3


"""Test conf file"""


def setup_test_db():
    conn = sqlite3.connect("test.db")
    cursor = conn.cursor()

    # Create test events table
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY,
        package_name TEXT,
        timestamp DATETIME,
        event_type TEXT
    )
    """
    )

    conn.commit()
    conn.close()


# Run setup  before tests
def setup_module(module):
    setup_test_db()
