from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

MetadataSqlBase: Any = declarative_base(name="Metadata")


class MetadataLog(MetadataSqlBase):  # pylint: disable=too-few-public-methods
    """Table for keeping track of parquet files in S3"""

    __tablename__ = "metadata_log"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    rail_pm_processed = sa.Column(sa.Boolean, default=sa.false())
    rail_pm_process_fail = sa.Column(sa.Boolean, default=sa.false())
    path = sa.Column(sa.String(256), nullable=False, unique=True)
    created_on = sa.Column(
        sa.DateTime(timezone=True), server_default=sa.func.now()
    )


sa.Index(
    "ix_metadata_log_not_processed",
    MetadataLog.path,
    postgresql_where=(MetadataLog.rail_pm_processed == sa.false()),
)
