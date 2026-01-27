import sys
import os

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.cassandra.schema import CassandraSchema

try:
    print("Testing CassandraSchema.get_all_cql_statements()...")
    stmts = CassandraSchema.get_all_cql_statements()
    print(f"Successfully retrieved {len(stmts)} statements.")
    
    for i, stmt in enumerate(stmts):
        print(f"Statement {i+1} preview: {stmt.strip()[:50]}...")
        
    assert len(stmts) > 0
    assert isinstance(stmts, list)
    print("Verification passed!")
except Exception as e:
    print(f"Verification failed: {e}")
    sys.exit(1)
