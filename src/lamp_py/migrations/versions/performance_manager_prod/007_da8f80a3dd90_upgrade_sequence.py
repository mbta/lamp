"""upgrade sequence

Revision ID: da8f80a3dd90
Revises: 36e7a7aee148
Create Date: 2025-04-11 09:43:50.433896

This change re-indexes all PROD table indexes in an attempt to resolve DB query degradation.

Details
* upgrade -> REINDEX all indexes on PRDO

* downgrade -> None

"""

from alembic import op
import sqlalchemy as sa

from lamp_py.runtime_utils.process_logger import ProcessLogger

# revision identifiers, used by Alembic.
revision = "da8f80a3dd90"
down_revision = "36e7a7aee148"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # REINDEX all tables
    tables = [
        "vehicle_events",
        "vehicle_trips",
        "static_feed_info",
        "static_trips",
        "static_routes",
        "static_stops",
        "static_stop_times",
        "static_calendar",
        "static_calendar_dates",
        "static_directions",
        "static_route_patterns",
    ]
    for table in tables:
        try:
            log = ProcessLogger(f"reindex_{table}")
            log.log_start()
            op.execute(sa.text(f"REINDEX TABLE {table};"))
            log.log_complete()
        except Exception as e:
            log.log_failure(e)


def downgrade() -> None:
    # No downgrade
    pass
