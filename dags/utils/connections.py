def get_duckdb_connection(db_schemas: list[str], attach_type: str = 'postgres'):
    """
    Attaching multiple database schemas
    :param attach_type: Type of attaching database (currently supports only postgres)
    :param db_schemas: List of schema names
    :return: duckdb connection instance
    """
    import duckdb
    from utils.get_env import (
        POSTGRES_DWH_HOST,
        POSTGRES_DWH_PORT,
        POSTGRES_DWH_DB,
        POSTGRES_DWH_USER,
        POSTGRES_DWH_PASSWORD
    )
    con = duckdb.connect(database=':memory')

    con.execute('LOAD postgres;')

    for schema in db_schemas:
        con.execute(f'''ATTACH
            'host={POSTGRES_DWH_HOST} 
            port={POSTGRES_DWH_PORT}
            dbname={POSTGRES_DWH_DB} 
            user={POSTGRES_DWH_USER} 
            password={POSTGRES_DWH_PASSWORD}'
            AS {schema}(TYPE {attach_type}, SCHEMA {schema})''')
        print(f'Successfully connected to a {attach_type} {schema} table')

    return con
