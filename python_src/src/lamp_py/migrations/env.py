from logging.config import fileConfig

from alembic import context

from lamp_py.postgres.postgres_utils import get_local_engine
from lamp_py.runtime_utils.import_env import load_environment

# load local environment, if running alembic locally
load_environment()

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# gate to make sure alembic is run using -n flag
if config.config_ini_section == "alembic":
    raise Exception("Run alembic with -n flag to specifiy Database name.")

# get database name from -n flag when alembic is run from cmd line 
db_name = config.config_ini_section

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
from lamp_py.postgres.postgres_schema import SqlBase

# using dictionary for target_metadata to support migrating multiple dbs
# each dictionary name should have a section defined in alembic.ini that
# matches the keys used in the target_metadata dictionary
target_metadata = {
    "performance_manager": SqlBase.metadata,
}

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    raise Exception("Alembic offline migration not implemented.")
    # url = config.get_main_option("sqlalchemy.url")
    # context.configure(
    #     url=url,
    #     target_metadata=target_metadata,
    #     literal_binds=True,
    #     dialect_opts={"paramstyle": "named"},
    # )

    # with context.begin_transaction():
    #     context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # support for multiple engines to manage multiple dbs 
    engines = {
        "performance_manager": get_local_engine(),
    }

    connectable = engines[db_name]

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata[db_name]
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
