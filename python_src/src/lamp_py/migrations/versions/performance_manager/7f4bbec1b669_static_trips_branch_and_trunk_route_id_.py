"""static_trips branch and trunk route_id trigger

Revision ID: 7f4bbec1b669
Revises: 43153d536c2a
Create Date: 2023-04-25 10:29:30.479958

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "7f4bbec1b669"
down_revision = "43153d536c2a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "static_trips",
        sa.Column("branch_route_id", sa.String(60), nullable=True),
    )
    op.add_column(
        "static_trips",
        sa.Column("trunk_route_id", sa.String(60), nullable=True),
    )
    create_red_braintree_branch = """
        CREATE OR REPLACE FUNCTION red_is_braintree_branch(p_trip_id varchar, p_fk_ts int) RETURNS boolean AS $$ 
        DECLARE
            is_braintree boolean;
        BEGIN 
            SELECT
                count(*) > 0
            INTO
                is_braintree
            FROM 
                static_stop_times sst
            WHERE 
                sst.trip_id = p_trip_id
                AND sst."timestamp" = p_fk_ts
                AND sst.stop_id IN ('70097', '70098', '70099', '70100', '70101', '70102', '70103', '70104', '70105')
            ;
 
            RETURN is_braintree;
        END;
        $$ LANGUAGE plpgsql;
    """
    op.execute(create_red_braintree_branch)

    create_red_ashmont_branch = """
        CREATE OR REPLACE FUNCTION red_is_ashmont_branch(p_trip_id varchar, p_fk_ts int) RETURNS boolean AS $$ 
        DECLARE
            is_ashmont boolean;
        BEGIN 
            SELECT
                count(*) > 0
            INTO
                is_ashmont
            FROM 
                static_stop_times sst
            WHERE 
                sst.trip_id = p_trip_id
                AND sst."timestamp" = p_fk_ts
                AND sst.stop_id IN ('70087', '70088', '70089', '70090', '70091', '70092', '70093', '70094')
            ;
 
            RETURN is_ashmont;
        END;
        $$ LANGUAGE plpgsql;
    """
    op.execute(create_red_ashmont_branch)

    create_branch_trunk_function = """
        CREATE OR REPLACE FUNCTION insert_static_trips_branch_trunk() RETURNS TRIGGER AS $$ 
        BEGIN 
            IF NEW.route_id ~ 'Green-' THEN
                NEW.branch_route_id := NEW.route_id;
                NEW.trunk_route_id := 'Green';
            ELSEIF NEW.route_id = 'Red' THEN
                IF red_is_braintree_branch(NEW.trip_id, NEW."timestamp") THEN
                    NEW.branch_route_id := 'Red-Braintree';
                    NEW.trunk_route_id := NEW.route_id;
                ELSEIF red_is_ashmont_branch(NEW.trip_id, NEW."timestamp") THEN
                    NEW.branch_route_id := 'Red-Ashmont';
                    NEW.trunk_route_id := NEW.route_id;
                ELSE
                    NEW.branch_route_id := null;
                    NEW.trunk_route_id := NEW.route_id;
                END IF;
            ELSE
                NEW.branch_route_id := null;
                NEW.trunk_route_id := NEW.route_id;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """
    op.execute(create_branch_trunk_function)

    create_trigger = """
        CREATE TRIGGER static_trips_create_branch_trunk BEFORE INSERT ON static_trips 
        FOR EACH ROW EXECUTE PROCEDURE insert_static_trips_branch_trunk();
    """
    op.execute(create_trigger)


def downgrade() -> None:
    drop_trigger = "DROP TRIGGER IF EXISTS static_trips_create_branch_trunk ON static_trips;"
    op.execute(drop_trigger)

    drop_function = "DROP function IF EXISTS public.red_is_braintree_branch();"
    op.execute(drop_function)

    drop_function = "DROP function IF EXISTS public.red_is_ashmont_branch();"
    op.execute(drop_function)

    drop_function = (
        "DROP function IF EXISTS public.insert_static_trips_branch_trunk();"
    )
    op.execute(drop_function)

    op.drop_column("static_trips", "trunk_route_id")
    op.drop_column("static_trips", "branch_route_id")
