"""feed_info init_date trigger

Revision ID: 43153d536c2a
Revises: 98aa70293578
Create Date: 2023-04-25 06:53:09.672206

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "43153d536c2a"
down_revision = "98aa70293578"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "static_feed_info",
        sa.Column("version_pub_date", sa.Integer(), nullable=True),
    )

    op.create_index(
        op.f("ix_static_feed_info_version_pub_date"),
        "static_feed_info",
        ["version_pub_date"],
        unique=False,
    )

    create_feed_info_insert_function = """
        CREATE OR REPLACE FUNCTION insert_feed_info() RETURNS TRIGGER AS $$ 
        BEGIN 
            IF NEW.feed_version ~ '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}' THEN
                NEW.version_pub_date := replace((substring(NEW.feed_version from '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}')::timestamptz at time zone 'US/Eastern')::date::text,'-','')::integer;
            ELSE
                NEW.version_pub_date := NEW.feed_start_date;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """
    op.execute(create_feed_info_insert_function)

    create_trigger = """
        CREATE TRIGGER insert_into_feed_info BEFORE INSERT ON static_feed_info 
        FOR EACH ROW EXECUTE PROCEDURE insert_feed_info();
    """
    op.execute(create_trigger)


def downgrade() -> None:
    drop_trigger = (
        "DROP TRIGGER IF EXISTS insert_into_feed_info ON static_feed_info;"
    )
    op.execute(drop_trigger)

    drop_function = "DROP function IF EXISTS public.insert_feed_info();"
    op.execute(drop_function)

    op.drop_index(
        op.f("ix_static_feed_info_version_pub_date"),
        table_name="static_feed_info",
    )

    op.drop_column("static_feed_info", "version_pub_date")
