import sqlalchemy as sa
from lamp_py.mssql.mssql_utils import MSSQLManager


def start() -> None:
    """
    Test MSSQL DB Connection
    """
    db = MSSQLManager(verbose=True)
    select_query = sa.text(
        "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE';"
    )
    for record in db.select_as_list(select_query):
        print(record)


if __name__ == "__main__":
    start()
