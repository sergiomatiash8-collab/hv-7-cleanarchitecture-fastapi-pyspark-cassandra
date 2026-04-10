import os
from cassandra.cluster import Session
from src.infrastructure.cassandra.database import cassandra_db

class CassandraInitializer:
    """
    Class responsible for creating keyspaces and tables in Cassandra.
    """
    
    def __init__(self, session: Session):
        """
        Initialize with an active Cassandra session.
        """
        self.session = session

    def execute_schema(self, schema_path: str):
        """
        Reads a .cql file and executes its commands one by one.
        """
        if not os.path.exists(schema_path):
            print(f"❌ Schema file not found at: {schema_path}")
            return

        print(f"📖 Reading schema from {schema_path}...")
        
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema_content = f.read()

        # Cassandra queries are separated by semicolons
        # We split them to execute each command individually
        queries = schema_content.split(';')

        for query in queries:
            clean_query = query.strip()
            if clean_query:
                try:
                    self.session.execute(clean_query)
                    # We log only the first line of the query for brevity
                    print(f"✅ Executed: {clean_query.splitlines()[0]}...")
                except Exception as e:
                    print(f"⚠️ Error executing query: {e}")

if __name__ == "__main__":
    # Internal testing logic to run initialization
    # Get active session from our database helper
    session = cassandra_db.connect()
    
    initializer = CassandraInitializer(session)
    
    # Path to our schema file
    schema_file = os.path.join("src", "infrastructure", "cassandra", "schema.cql")
    
    initializer.execute_schema(schema_file)
    
    cassandra_db.close()