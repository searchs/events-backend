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

# Setup logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


class EventType(str, Enum):
    INSTALL = "install"
    UNINSTALL = "uninstall"


class PackageEvent(BaseModel):
    timestamp: datetime
    package: str
    type: EventType

    @validator("package")
    def validate_package_name(cls, v):
        if not v or not v.strip():
            raise ValueError("Package name cannot be empty")
        return v.strip()

    @validator("timestamp")
    def validate_timestamp(cls, v):
        if v > datetime.now(tz=timezone.utc) + timedelta(minutes=5):
            raise ValueError("Timestamp cannot be in the future")
        return v


class PackageResponseOut(BaseModel):
    version: str
    author: str
    description: str
    homepage: str
    license: str


class InstallMetrics(BaseModel):
    count: int = 0
    last: datetime


class EventMetrics(BaseModel):
    install: InstallMetrics
    uninstall: InstallMetrics


class PackageMetrics(BaseModel):
    total_installs: int
    total_uninstalls: int
    most_installed: List[tuple[str, int]]
    most_uninstalled: List[tuple[str, int]]
    install_trend: Dict[str, int]  # Daily install counts
    popular_pairs: List[tuple[str, str, int]]  # Packages often installed together


class DetailedPackageStats(BaseModel):
    package_name: str
    metadata: dict
    events: EventMetrics.model_dump
    total_installs: int
    total_uninstalls: int
    daily_installs: Dict[str, int]
    install_hours_distribution: Dict[str, int]
    # common_co_installations: List[tuple[str, int]]


app = FastAPI(title="Python Package Events Tracker")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Database connection management
@contextmanager
def get_db():
    conn = sqlite3.connect("package_events.db")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db():
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


# GET / package / {package: str} / event / install / total
def fetch_package_install_total(package_name: str) -> int:
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
    """Fetch package metadata from PyPI with error handling and retries"""
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
    init_db()
    logger.info("Database initialized successfully")


@app.post("/event", status_code=status.HTTP_201_CREATED)
async def record_event(event: PackageEvent) -> dict:
    """Record a package install/uninstall event with enhanced error handling"""
    try:
        metadata = await fetch_package_metadata(event.package)

        with get_db() as conn:
            cursor = conn.cursor()

            # Record the event
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

            # Record package pairs for co-installation analysis
            if event.type == "install":
                # Get recent installations (within 1 hour)
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
                        pass  # Ignore duplicate pairs

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
    return fetch_package_install_total(package)


@app.get("/package/{package:str}/event/install/last")
async def get_package_install_last(package: str):
    return fetch_package_install_last(package)


@app.get("/metrics")
async def get_metrics(hours: Optional[int] = None):
    """Get comprehensive package installation metrics"""
    with get_db() as conn:
        cursor = conn.cursor()

        time_condition = ""
        if hours:
            time_condition = f"WHERE timestamp >= datetime('now', '-{hours} hours')"

        # Basic metrics
        cursor.execute(
            f"""
            SELECT event_type, COUNT(*)
            FROM events
            {time_condition}
            GROUP BY event_type
        """
        )
        event_counts = dict(cursor.fetchall())

        # Most installed packages
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

        # Most uninstalled packages
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

        # Daily install trend
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

        # Popular package pairs
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
    Retrieve events info from DB and other info from PyPI.
    """
    with get_db() as conn:
        cursor = conn.cursor()

        # Get latest metadata
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

        # Get install/uninstall counts
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

        # Daily install counts
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
        # TODO: Remove this, it's just for testing Ola
        logger.info(f"DAILY INSTALLS: {daily_installs}")

        # Hour of day distribution
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

        # Common co-installations
        cursor.execute(
            """
            SELECT
                CASE
                    WHEN package1 = ? THEN package2
                    ELSE package1
                END as other_package,
                COUNT(*) as count
            FROM package_pairs
            WHERE package1 = ? OR package2 = ?
            GROUP BY other_package
            ORDER BY count DESC
            LIMIT 5
        """,
            (package_name, package_name, package_name),
        )
        # co_installations = cursor.fetchall()

        return {
            "package_name": package_name,
            "metadata": {
                "version": metadata[0] if metadata else None,
                "author": metadata[1] if metadata else None,
                "description": metadata[2] if metadata else None,
            },
            "total_installs": event_counts.get("install", 0),
            "total_uninstalls": event_counts.get("uninstall", 0),
            "daily_installs": daily_installs,
            "install_hours_distribution": hours_distribution,
        }

        # return DetailedPackageStats(
        #     package_name=package_name,
        #     metadata={
        #         "version": metadata[0] if metadata else None,
        #         "author": metadata[1] if metadata else None,
        #         "description": metadata[2] if metadata else None,
        #     },
        #     total_installs=event_counts.get("install", 0),
        #     total_uninstalls=event_counts.get("uninstall", 0),
        #     daily_installs=daily_installs,
        #     install_hours_distribution=hours_distribution,
        #     common_co_installations=co_installations,
        # )


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Service unhealthy: {str(e)}",
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
