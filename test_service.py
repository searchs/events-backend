from contextlib import contextmanager
import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timezone, timedelta
import sqlite3
from unittest.mock import Mock, patch
import httpx

from main import app, init_db
import asyncio

pytestmark = pytest.mark.asyncio


# Test client setup
@pytest.fixture
def client():
    """Create a test client for the FastAPI application."""
    return TestClient(app)


# Database fixtures
@pytest.fixture
def test_db():
    """Create a test database in memory."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # Initialize schema
    init_db()

    yield conn
    conn.close()


@pytest.fixture
def mock_db(monkeypatch, test_db):
    """Replace the production database with test database."""

    @contextmanager
    def mock_get_db():
        yield test_db

    monkeypatch.setattr("main.get_db", mock_get_db)
    return test_db


# Mock PyPI response since it's an external service/dependency
@pytest.fixture
def mock_pypi_response():
    """Mock PyPI API response data."""
    return {
        "info": {
            "version": "1.0.0",
            "author": "Test Author",
            "summary": "Test Description",
            "home_page": "https://test.com",
            "license": "MIT",
        },
        "releases": {},
    }


@pytest.fixture
async def mock_http_client(mock_pypi_response):
    """Mock httpx client for PyPI API calls."""
    async with httpx.AsyncClient() as client:
        with patch.object(client, "get") as mock_get:
            mock_get.return_value = Mock(
                status_code=200,
                raise_for_status=Mock(),
                json=Mock(return_value=mock_pypi_response),
            )
            yield mock_get


# Test cases


@pytest.mark.asyncio
async def test_record_install_event(client, mock_db, mock_http_client):
    """Test recording a package installation event."""
    event_data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "package": "test-package",
        "type": "install",
    }

    response = client.post("/event", json=event_data)

    assert response.status_code == 201
    assert response.json()["status"] == "success"

    # Verify database record
    cursor = mock_db.cursor()
    cursor.execute("SELECT * FROM events WHERE package_name = ?", ("test-package",))
    record = cursor.fetchone()

    assert record is not None
    assert record["event_type"] == "install"
    assert record["package_name"] == "test-package"


@pytest.mark.asyncio
async def test_record_event_invalid_timestamp(client):
    """Test recording an event with future timestamp."""
    future_time = datetime.now(timezone.utc) + timedelta(days=1)
    event_data = {
        "timestamp": future_time.isoformat(),
        "package": "test-package",
        "type": "install",
    }

    response = client.post("/event", json=event_data)
    assert response.status_code == 422  # Validation error


@pytest.mark.asyncio
async def test_record_event_empty_package(client):
    """Test recording an event with empty package name."""
    event_data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "package": "",
        "type": "install",
    }

    response = client.post("/event", json=event_data)
    assert response.status_code == 422  # Validation error


def test_get_package_install_total(client, mock_db):
    """Test getting total installations for a package."""
    # Insert test data
    cursor = mock_db.cursor()
    timestamp = datetime.now(timezone.utc)
    cursor.execute(
        "INSERT INTO events (timestamp, package_name, event_type) VALUES (?, ?, ?)",
        (timestamp, "test-package", "install"),
    )
    mock_db.commit()

    response = client.get("/package/test-package/event/install/total")
    assert response.status_code == 200
    assert response.json() == 1


def test_get_package_install_last(client, mock_db):
    """Test getting last installation timestamp for a package."""
    # Insert test data
    cursor = mock_db.cursor()
    timestamp = datetime.now(timezone.utc)
    cursor.execute(
        "INSERT INTO events (timestamp, package_name, event_type) VALUES (?, ?, ?)",
        (timestamp, "test-package", "install"),
    )
    mock_db.commit()

    response = client.get("/package/test-package/event/install/last")
    assert response.status_code == 200
    assert response.json() is not None


@pytest.mark.asyncio
async def test_get_metrics(client, mock_db):
    """Test getting overall metrics."""
    # Insert test data
    cursor = mock_db.cursor()
    timestamp = datetime.now(timezone.utc)

    # Add install events
    cursor.execute(
        "INSERT INTO events (timestamp, package_name, event_type) VALUES (?, ?, ?)",
        (timestamp, "package1", "install"),
    )
    cursor.execute(
        "INSERT INTO events (timestamp, package_name, event_type) VALUES (?, ?, ?)",
        (timestamp, "package2", "install"),
    )

    # Add uninstall event
    cursor.execute(
        "INSERT INTO events (timestamp, package_name, event_type) VALUES (?, ?, ?)",
        (timestamp, "package1", "uninstall"),
    )

    # Add package pair
    cursor.execute(
        "INSERT INTO package_pairs (package1, package2, timestamp) VALUES (?, ?, ?)",
        ("package1", "package2", timestamp),
    )

    mock_db.commit()

    response = client.get("/metrics")
    assert response.status_code == 200
    data = response.json()

    assert data["total_installs"] == 2
    assert data["total_uninstalls"] == 1
    assert len(data["most_installed"]) > 0
    assert len(data["install_trend"]) > 0


@pytest.mark.asyncio
async def test_get_package_stats(client, mock_db):
    """Test getting detailed statistics for a specific package."""
    # Insert test data
    cursor = mock_db.cursor()
    timestamp = datetime.now(timezone.utc)

    cursor.execute(
        """
        INSERT INTO events
        (timestamp, package_name, event_type, package_version, author, description)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            timestamp,
            "test-package",
            "install",
            "1.0.0",
            "Test Author",
            "Test Description",
        ),
    )
    mock_db.commit()

    response = client.get("/packages/test-package")
    assert response.status_code == 200
    data = response.json()

    assert data["package_name"] == "test-package"
    assert data["total_installs"] == 1
    assert data["metadata"]["version"] == "1.0.0"
    assert data["metadata"]["author"] == "Test Author"


def test_health_check(client):
    """Test the health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


# Error handling tests


@pytest.mark.asyncio
async def test_record_event_pypi_error(client, mock_db):
    """Test handling PyPI API errors when recording an event."""
    with patch("httpx.AsyncClient.get") as mock_get:
        mock_get.side_effect = httpx.HTTPError("PyPI API error")

        event_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "package": "test-package",
            "type": "install",
        }

        response = client.post("/event", json=event_data)
        assert response.status_code == 201  # Should still succeed with default values

        # Verify database record has null metadata
        cursor = mock_db.cursor()
        cursor.execute("SELECT * FROM events WHERE package_name = ?", ("test-package",))
        record = cursor.fetchone()

        assert record["package_version"] is None
        assert record["author"] is None
        assert record["description"] is None


@pytest.mark.asyncio
async def test_get_metrics_with_hours_filter(client, mock_db):
    """Test getting metrics with hours filter."""
    # Insert test data
    cursor = mock_db.cursor()
    now = datetime.now(timezone.utc)
    old_timestamp = now - timedelta(hours=25)
    recent_timestamp = now - timedelta(hours=1)

    # Add old install event
    cursor.execute(
        "INSERT INTO events (timestamp, package_name, event_type) VALUES (?, ?, ?)",
        (old_timestamp, "package1", "install"),
    )

    # Add recent install event
    cursor.execute(
        "INSERT INTO events (timestamp, package_name, event_type) VALUES (?, ?, ?)",
        (recent_timestamp, "package2", "install"),
    )

    mock_db.commit()

    # Get metrics for last 24 hours
    response = client.get("/metrics?hours=24")
    assert response.status_code == 200
    data = response.json()

    assert data["total_installs"] == 1  # Should only count recent install


@pytest.mark.asyncio
async def test_database_connection_error(client):
    """Test handling database connection errors."""
    with patch("main.get_db") as mock_get_db:
        mock_get_db.side_effect = sqlite3.Error("Database error")

        response = client.get("/health")
        assert response.status_code == 503
        assert "unhealthy" in response.json()["detail"].lower()
