import psycopg2
from psycopg2 import pool, OperationalError
from dotenv import load_dotenv
import os
from typing import Optional

load_dotenv()

class DatabaseManager:
    _connection_pool = None

    @classmethod
    def initialize_pool(cls):
        try:
            cls._connection_pool = pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD")
            )
            print("✅ Pool de conexões inicializado")
        except OperationalError as e:
            print(f"❌ Falha ao criar pool: {e}")

    @classmethod
    def get_connection(cls) -> Optional[psycopg2.extensions.connection]:
        try:
            if cls._connection_pool:
                conn = cls._connection_pool.getconn()
                print("🔌 Conexão obtida do pool")
                return conn
            return None
        except Exception as e:
            print(f"❌ Erro ao obter conexão: {e}")
            return None

    @classmethod
    def return_connection(cls, conn: psycopg2.extensions.connection):
        if cls._connection_pool and conn:
            try:
                cls._connection_pool.putconn(conn)
                print("🔄 Conexão devolvida ao pool")
            except Exception as e:
                print(f"⚠️ Erro ao devolver conexão: {e}")

    @classmethod
    def test_connection(cls):
        print("Iniciando teste de conexão...")
        conn = cls.get_connection()
        
        if conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT version();")
                    print(f"📌 PostgreSQL: {cursor.fetchone()[0]}")
                print("✅ Teste de conexão bem-sucedido")
            except psycopg2.Error as e:
                print(f"❌ Erro no teste: {e}")
            finally:
                cls.return_connection(conn)
        else:
            print("❌ Falha ao obter conexão para teste")

DatabaseManager.initialize_pool()

if __name__ == "__main__":
    DatabaseManager.test_connection()