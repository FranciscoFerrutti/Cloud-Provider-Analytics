import sys
import os

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.serving.astradb_setup import AstraDBSetup

try:
    print("Testing AstraDBSetup config...")
    
    # Test client creation
    try:
        client = AstraDBSetup.get_client()
        print(f"DataAPIClient created: {client}")
    except Exception as e:
        print(f"Failed to create DataAPIClient (expected if no token): {e}")

    # Test Endpoint construction
    try:
        endpoint = AstraDBSetup.get_api_endpoint()
        print(f"API Endpoint constructed: {endpoint}")
    except Exception as e:
        print(f"Failed to construct endpoint (expected if keys missing): {e}")
        
    print("Collections defined:")
    for col in AstraDBSetup.COLLECTIONS:
        print(f"- {col}")
        
    assert len(AstraDBSetup.COLLECTIONS) > 0
    print("Verification passed!")

except Exception as e:
    print(f"Verification failed: {e}")
    sys.exit(1)
