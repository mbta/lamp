"""extend service_id_by_date_and_route

Revision ID: ae6c6e4b2df5
Revises: 1b53fd278b10
Create Date: 2023-12-17 06:56:17.330783

Details
* upgrade -> extend service_id_by_date_and_route VIEW to generate values past current date

* downgrade -> Nothing
"""
from alembic import op

from lamp_py.migrations.versions.performance_manager_prod.sql_strings.strings_003 import (
    view_service_id_by_date_and_route,
)


# revision identifiers, used by Alembic.
revision = "ae6c6e4b2df5"
down_revision = "1b53fd278b10"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP VIEW IF EXISTS service_id_by_date_and_route;")
    op.execute(view_service_id_by_date_and_route)


def downgrade() -> None:
    pass
