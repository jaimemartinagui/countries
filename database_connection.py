"""
    Modulo para la conexion con la base de datos PostgreSQL.
"""

import psycopg2

from config.config import DATABASE_CONN

class DatabaseConnection:
    """Clase para la conexion con la base de datos PostgreSQL."""

    def __init__(self, autocommit=True):

        self.autocommit    = autocommit
        self.database_vars = DATABASE_CONN
        self.conn          = self._open_connection()

    def _open_connection(self):
        """Metodo para abrir la conexion con PostgreSQL."""

        try:
            conn = psycopg2.connect(
                host=self.database_vars['postgres_host'],
                port=self.database_vars['postgres_port'],
                database=self.database_vars['postgres_database'],
                user=self.database_vars['postgres_user'],
                password=self.database_vars['postgres_password']
            )
            conn.autocommit = self.autocommit
        except:
            raise Exception('La ejecucion se ha detenido. Compruebe logs.\n')

        return conn

    def close_connection(self):
        """Metodo para cerrar la conexion con PostgreSQL."""

        self.conn.close()

    def get_cursor(self):
        """Metodo que devuelve el cursor de la conexion."""

        return self.conn.cursor()
