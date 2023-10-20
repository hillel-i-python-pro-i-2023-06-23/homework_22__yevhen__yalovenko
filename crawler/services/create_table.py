from crawler.services.db_connection import DBConnection


def create_table():
    with DBConnection() as connection:
        with connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS urls (url TEXT PRIMARY KEY)
            """
            )
