import duckdb

def authenticate_s3() -> bool:
    duckdb.sql("INSTALL aws")
    duckdb.sql("LOAD aws")
    auth = duckdb.sql("""CREATE OR REPLACE SECRET secret (
        TYPE s3,
        PROVIDER credential_chain
    )""")

    return auth.pl()[0][0]
