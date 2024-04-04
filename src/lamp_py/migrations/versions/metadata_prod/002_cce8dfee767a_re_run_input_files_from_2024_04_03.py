"""re-run input files from 2024-04-03

Revision ID: cce8dfee767a
Revises: 07903947aabe
Create Date: 2024-04-04 11:50:55.161259

Details
* upgrade -> update metdata table to re-process failed parquet files from April 3, 2024

* downgrade -> Nothing

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "cce8dfee767a"
down_revision = "07903947aabe"
branch_labels = None
depends_on = None


def upgrade() -> None:
    update_query = """
    UPDATE 
        public.metadata_log 
    SET 
        rail_pm_process_fail = false 
        , rail_pm_processed = false 
    WHERE 
        created_on > '2024-04-03 09:00:00'
        and created_on < '2024-04-03 15:00:00'
        and (
            path LIKE '%RT_TRIP_UPDATES%'
            or path LIKE '%RT_VEHICLE_POSITION%'
        )
    ;
    """
    op.execute(update_query)


def downgrade() -> None:
    pass
