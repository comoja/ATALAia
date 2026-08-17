import sys
import os
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from middleware.database.dbConnection import getConnection, DBConnectionPool

def test_sqlalchemy_pool():
    print("Testing SQLAlchemy with DBConnectionPool creator...")
    engine = create_engine(
        "mysql+mysqlconnector://",
        creator=getConnection,
        poolclass=NullPool
    )
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        row = result.fetchone()
        print("SQLAlchemy query result:", row[0])
    
    print("Test successful!")

if __name__ == "__main__":
    test_sqlalchemy_pool()
