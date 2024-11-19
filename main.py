from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import sqlite3
from typing import List, Optional
from enum import Enum
import asyncio

from fastapi import status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import validator
from datetime import datetime, timedelta, timezone
from typing import Dict
from contextlib import contextmanager
from loguru import logger


class EventType(str, Enum):
    """Enumeration of supported package event types.

    Attributes:
        INSTALL: Represents a package installation event.
        UNINSTALL: Represents a package uninstallation event.
    """

    INSTALL = "install"
    UNINSTALL = "uninstall"


class PackageEvent(BaseModel):
    """Model representing a package installation or uninstallation event.

    Attributes:
        timestamp: The UTC datetime when the event occurred.
        package: The name of the Python package.
        type: The type of event (install/uninstall).
    """

    timestamp: datetime
    package: str
    type: EventType

    @validator("package")
    def validate_package_name(cls, v):
        """Validates the package name is not empty.

        Args:
            v: The package name to validate.

        Returns:
            str: The validated and stripped package name.

        Raises:
            ValueError: If the package name is empty or only whitespace.
        """
        if not v or not v.strip():
            raise ValueError("Package name cannot be empty")
        return v.strip()

    @validator("timestamp")
    def validate_timestamp(cls, v):
        """Validates the event timestamp is not too far in the future.

        Args:
            v: The timestamp to validate.

        Returns:
            datetime: The validated timestamp.

        Raises:
            ValueError: If the timestamp is more than 5 minutes in the future.
        """
        if v > datetime.now(tz=timezone.utc) + timedelta(minutes=5):
            raise ValueError("Timestamp cannot be in the future")
        return v


class PackageResponseOut(BaseModel):
    """Model for package metadata response.

    Attributes:
        version: Package version string.
        author: Package author name.
        description: Package description text.
        homepage: Package homepage URL.
        license: Package license identifier.
    """

    version: str
    author: str
    description: str
    homepage: str
    license: str


class InstallMetrics(BaseModel):
    """Model for installation metrics.

    Attributes:
        count: Number of installations.
        last: Timestamp of most recent installation.
    """

    count: int = 0
    last: datetime


class EventMetrics(BaseModel):
    """Model containing both install and uninstall metrics.

    Attributes:
        install: Installation metrics.
        uninstall: Uninstallation metrics.
    """

    install: InstallMetrics
    uninstall: InstallMetrics


class PackageMetrics(BaseModel):
    """Model for aggregated package metrics.

    Attributes:
        total_installs: Total number of package installations.
        total_uninstalls: Total number of package uninstallations.
        most_installed: List of tuples (package_name, install_count) for most installed packages.
        most_uninstalled: List of tuples (package_name, uninstall_count) for most uninstalled packages.
        install_trend: Dictionary mapping dates to daily install counts.
        popular_pairs: List of tuples (package1, package2, count) for commonly co-installed packages.
    """

    total_installs: int
    total_uninstalls: int
    most_installed: List[tuple[str, int]]
    most_uninstalled: List[tuple[str, int]]
    install_trend: Dict[str, int]
    popular_pairs: List[tuple[str, str, int]]


app = FastAPI(title="Python Package Events Tracker")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@contextmanager
def get_db():
    """Context manager for database connections.

    Yields:
        sqlite3.Connection: Database connection with Row factory enabled.
    """
    conn = sqlite3.connect("package_events.db")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    """Initialize the database schema.

    Creates the necessary tables and indexes if they don't exist:
    - events table for storing package events
    - package_pairs table for tracking co-installations
    - Relevant indexes for performance optimization
    """
    with get_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                package_name TEXT NOT NULL,
                event_type TEXT NOT NULL,
                package_version TEXT,
                author TEXT,
                description TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_package_timestamp
            ON events(package_name, timestamp);

            CREATE INDEX IF NOT EXISTS idx_event_type
            ON events(event_type);

            CREATE TABLE IF NOT EXISTS package_pairs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                package1 TEXT NOT NULL,
                package2 TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                UNIQUE(package1, package2, timestamp)
            );
        """
        )


def fetch_package_install_total(package_name: str) -> int:
    """Get the total number of installations for a package.

    Args:
        package_name: Name of the package.

    Returns:
        int: Total number of installations.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*) FROM events
            WHERE package_name = ? AND event_type = 'install'
        """,
            (package_name,),
        )
        return cursor.fetchone()[0]


def fetch_package_install_last(package_name: str) -> datetime | None:
    """Get the timestamp of the most recent installation for a package.

    Args:
        package_name: Name of the package.

    Returns:
        datetime | None: Timestamp of most recent installation, or None if never installed.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT MAX(timestamp) FROM events
            WHERE package_name = ? AND event_type = 'install'
        """,
            (package_name,),
        )
        return cursor.fetchone()[0] if cursor.fetchone() else None


async def fetch_package_metadata(package_name: str) -> dict:
    """Fetch package metadata from PyPI with error handling and retries.

    Args:
        package_name: Name of the package to fetch metadata for.

    Returns:
        dict: Package metadata including version, author, description, etc.
            Returns default None values if metadata fetch fails.
    """
    async with httpx.AsyncClient() as client:
        for attempt in range(3):  # 3 retry attempts
            try:
                response = await client.get(
                    f"https://pypi.org/pypi/{package_name}/json", timeout=10.0
                )
                response.raise_for_status()
                data = response.json()
                return {
                    "version": data["info"]["version"],
                    "author": data["info"]["author"],
                    "description": data["info"]["summary"],
                    "releases": data["releases"],
                    "homepage": data["info"]["home_page"],
                    "license": data["info"]["license"],
                }
            except httpx.HTTPError as e:
                if attempt == 2:  # Last attempt
                    logger.error(
                        f"Failed to fetch PyPI metadata for {package_name}: {str(e)}"
                    )
                    return {
                        "version": None,
                        "author": None,
                        "description": None,
                        "releases": None,
                        "homepage": None,
                        "license": None,
                    }
                await asyncio.sleep(1)  # Wait before retry


@app.on_event("startup")
async def startup_event():
    """Initialize database on application startup."""
    init_db()
    logger.info("Database initialized successfully")


@app.post("/event", status_code=status.HTTP_201_CREATED)
async def record_event(event: PackageEvent) -> dict:
    """Record a package install/uninstall event.

    Args:
        event: Package event details including timestamp, package name, and event type.

    Returns:
        dict: Success message.

    Raises:
        HTTPException: If event recording fails.
    """
    try:
        metadata = await fetch_package_metadata(event.package)

        with get_db() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT INTO events (
                    timestamp, package_name, event_type,
                    package_version, author, description
                ) VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    event.timestamp,
                    event.package,
                    event.type,
                    metadata["version"],
                    metadata["author"],
                    metadata["description"],
                ),
            )

            if event.type == "install":
                cursor.execute(
                    """
                    SELECT DISTINCT package_name
                    FROM events
                    WHERE event_type = 'install'
                    AND timestamp >= datetime(?, '-1 hour')
                    AND package_name != ?
                """,
                    (event.timestamp, event.package),
                )

                recent_packages = cursor.fetchall()
                for recent in recent_packages:
                    try:
                        cursor.execute(
                            """
                            INSERT INTO package_pairs (package1, package2, timestamp)
                            VALUES (?, ?, ?)
                        """,
                            (event.package, recent[0], event.timestamp),
                        )
                    except sqlite3.IntegrityError:
                        pass

            conn.commit()

        return {"status": "success", "message": "Event recorded successfully"}

    except Exception as e:
        logger.error(f"Error recording event: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to record event: {str(e)}",
        )


@app.get("/package/{package:str}/event/install/total")
async def get_package_install_total(package: str):
    """Get total number of installations for a package.

    Args:
        package: Name of the package.

    Returns:
        int: Total installation count.
    """
    return fetch_package_install_total(package)


@app.get("/package/{package:str}/event/install/last")
async def get_package_install_last(package: str):
    """Get timestamp of most recent installation for a package.

    Args:
        package: Name of the package.

    Returns:
        datetime | None: Timestamp of most recent installation.
    """
    return fetch_package_install_last(package)


@app.get("/metrics")
async def get_metrics(hours: Optional[int] = None):
    """Get comprehensive package installation metrics.

    Args:
        hours: Optional number of hours to limit the metrics timeframe.

    Returns:
        PackageMetrics: Aggregated metrics including install counts, trends, and popular pairs.
    """
    with get_db() as conn:
        cursor = conn.cursor()

        time_condition = ""
        if hours:
            time_condition = f"WHERE timestamp >= datetime('now', '-{hours} hours')"

        cursor.execute(
            f"""
            SELECT event_type, COUNT(*)
            FROM events
            {time_condition}
            GROUP BY event_type
        """
        )
        event_counts = dict(cursor.fetchall())

        cursor.execute(
            f"""
            SELECT package_name, COUNT(*) as count
            FROM events
            WHERE event_type = 'install'
            {time_condition.replace('WHERE', 'AND') if time_condition else ''}
            GROUP BY package_name
            ORDER BY count DESC
            LIMIT 10
        """
        )
        most_installed = cursor.fetchall()

        cursor.execute(
            f"""
            SELECT package_name, COUNT(*) as count
            FROM events
            WHERE event_type = 'uninstall'
            {time_condition.replace('WHERE', 'AND') if time_condition else ''}
            GROUP BY package_name
            ORDER BY count DESC
            LIMIT 5
        """
        )
        most_uninstalled = cursor.fetchall()

        cursor.execute(
            """
            SELECT date(timestamp) as date, COUNT(*) as count
            FROM events
            WHERE event_type = 'install'
            GROUP BY date(timestamp)
            ORDER BY date DESC
            LIMIT 30
        """
        )
        install_trend = dict(cursor.fetchall())

        cursor.execute(
            """
            SELECT package1, package2, COUNT(*) as count
            FROM package_pairs
            GROUP BY package1, package2
            ORDER BY count DESC
            LIMIT 10
        """
        )
        popular_pairs = cursor.fetchall()

        return PackageMetrics(
            total_installs=event_counts.get("install", 0),
            total_uninstalls=event_counts.get("uninstall", 0),
            most_installed=most_installed,
            most_uninstalled=most_uninstalled,
            install_trend=install_trend,
            popular_pairs=popular_pairs,
        )


@app.get("/packages/{package_name}")
async def get_package_stats(package_name: str) -> dict:
    """Get detailed statistics for a specific package.

    Args:
        package_name: Name of the package to get statistics for.

    Returns:
        dict: Detailed package statistics including metadata, install counts,
            daily trends, and hourly distribution.
    """
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT package_version, author, description
            FROM events
            WHERE package_name = ?
            ORDER BY timestamp DESC
            LIMIT 1
        """,
            (package_name,),
        )
        metadata = cursor.fetchone()

        cursor.execute(
            """
            SELECT event_type, COUNT(*)
            FROM events
            WHERE package_name = ?
            GROUP BY event_type
        """,
            (package_name,),
        )
        event_counts = dict(cursor.fetchall())

        cursor.execute(
            """
            SELECT date(timestamp) as date, COUNT(*) as count
            FROM events
            WHERE package_name = ? AND event_type = 'install'
            GROUP BY date(timestamp)
            ORDER BY date DESC
            LIMIT 30
        """,
            (package_name,),
        )
        daily_installs = dict(cursor.fetchall())
        logger.info(f"DAILY INSTALLS: {daily_installs}")

        cursor.execute(
            """
            SELECT strftime('%H', timestamp) as hour, COUNT(*) as count
            FROM events
            WHERE package_name = ? AND event_type = 'install'
            GROUP BY hour
            ORDER BY hour
        """,
            (package_name,),
        )
        hours_distribution = dict(cursor.fetchall())

        return {
            "package_name": package_name,
            "metadata": {
                "version": metadata[0] if metadata else None,
                "author": metadata[1] if metadata else None,
                "description": metadata[2] if metadata else None,
            },
            "total_installs": event_counts.get("install", 0),
            "total_uninstalls": event_counts.get("uninstall", 0),
        }
