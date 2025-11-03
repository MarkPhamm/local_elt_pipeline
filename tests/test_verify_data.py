"""
Tests to verify data was loaded into DuckDB correctly.

These tests check that:
- The database file exists
- The table has the expected structure
- Data was loaded with the expected columns
- Sample records can be queried
"""

import os

import duckdb
import pytest


@pytest.fixture
def db_path():
    """Path to the DuckDB database file."""
    return "database/cfpb_complaints.duckdb"


@pytest.fixture
def db_connection(db_path):
    """Create a connection to the DuckDB database."""
    # Check if database file exists
    if not os.path.exists(db_path):
        pytest.skip(f"Database file not found: {db_path}. Run the pipeline first.")

    conn = duckdb.connect(db_path)
    yield conn
    conn.close()


@pytest.fixture
def schema_name():
    """Schema name where data is stored."""
    return "raw"


@pytest.fixture
def table_name():
    """Table name for complaints data."""
    return "cfpb_complaints"


class TestDatabaseExists:
    """Test that the database file exists."""

    def test_database_file_exists(self, db_path):
        """Test that the database file exists."""
        assert os.path.exists(db_path), f"Database file should exist at {db_path}"
        assert os.path.getsize(db_path) > 0, "Database file should not be empty"


class TestTableStructure:
    """Test the structure of the loaded table."""

    def test_table_exists(self, db_connection, schema_name, table_name):
        """Test that the table exists in the database."""
        result = db_connection.execute(
            f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
            """
        ).fetchone()[0]

        assert result == 1, f"Table {schema_name}.{table_name} should exist"

    def test_table_has_columns(self, db_connection, schema_name, table_name):
        """Test that the table has columns."""
        columns = db_connection.execute(
            f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
            ORDER BY ordinal_position
            """
        ).fetchall()

        assert len(columns) > 0, f"Table {schema_name}.{table_name} should have columns"
        assert len(columns) >= 10, f"Table should have at least 10 columns, got {len(columns)}"

    def test_table_has_required_columns(self, db_connection, schema_name, table_name):
        """Test that the table has required key columns."""
        columns = db_connection.execute(
            f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
            """
        ).fetchall()

        column_names = [col[0] for col in columns]
        required_columns = ["complaint_id", "date_received", "company", "product"]

        for required_col in required_columns:
            assert required_col in column_names, f"Table should have column: {required_col}"


class TestDataContent:
    """Test the content of the loaded data."""

    def test_table_has_records(self, db_connection, schema_name, table_name):
        """Test that the table contains records."""
        count = db_connection.execute(
            f"SELECT COUNT(*) FROM {schema_name}.{table_name}"
        ).fetchone()[0]

        assert count > 0, f"Table {schema_name}.{table_name} should have records"
        print(f"\n✓ Total records in {schema_name}.{table_name}: {count:,}")

    def test_complaint_id_is_unique(self, db_connection, schema_name, table_name):
        """Test that complaint_id values are unique (primary key constraint)."""
        result = db_connection.execute(
            f"""
            SELECT COUNT(DISTINCT complaint_id) as unique_count, COUNT(*) as total_count
            FROM {schema_name}.{table_name}
            """
        ).fetchone()

        unique_count, total_count = result

        assert unique_count == total_count, (
            f"complaint_id should be unique. "
            f"Found {unique_count} unique values out of {total_count} total records"
        )

    def test_sample_records_have_required_fields(self, db_connection, schema_name, table_name):
        """Test that sample records have the required fields populated."""
        samples = db_connection.execute(
            f"""
            SELECT complaint_id, date_received, company, product
            FROM {schema_name}.{table_name}
            WHERE complaint_id IS NOT NULL
            LIMIT 5
            """
        ).fetchall()

        assert len(samples) > 0, "Should be able to fetch sample records"

        for sample in samples:
            complaint_id, date_received, company, product = sample

            assert complaint_id is not None, "complaint_id should not be None"
            assert complaint_id != "", "complaint_id should not be empty"
            assert date_received is not None, "date_received should not be None"
            assert company is not None, "company should not be None"
            assert product is not None, "product should not be None"

    def test_date_received_format(self, db_connection, schema_name, table_name):
        """Test that date_received is in the expected format."""
        # Check that date_received can be parsed as a date
        result = db_connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {schema_name}.{table_name}
            WHERE date_received IS NOT NULL
            """
        ).fetchone()[0]

        total = db_connection.execute(
            f"SELECT COUNT(*) FROM {schema_name}.{table_name}"
        ).fetchone()[0]

        # At least 90% of records should have valid dates
        assert (result / total) >= 0.9, (
            f"At least 90% of records should have valid dates. "
            f"Got {result}/{total} ({result / total * 100:.1f}%)"
        )


class TestDataQuality:
    """Test data quality metrics."""

    def test_no_completely_null_records(self, db_connection, schema_name, table_name):
        """Test that records have at least some data (not all null)."""
        # Count records where all key fields are null
        null_count = db_connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {schema_name}.{table_name}
            WHERE complaint_id IS NULL
            AND date_received IS NULL
            AND company IS NULL
            """
        ).fetchone()[0]

        total = db_connection.execute(
            f"SELECT COUNT(*) FROM {schema_name}.{table_name}"
        ).fetchone()[0]

        # Should have very few completely null records
        assert null_count == 0, (
            f"Should not have completely null records. Found {null_count} out of {total}"
        )
