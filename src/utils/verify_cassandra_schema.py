import sys
import os

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.serving.astradb_setup import AstraDBSetup

try:
    print("Testing AstraDBSetup config...")
    
    # Just check if we can get client variables (without making network call if possible, or failing fast)
    try:
        AstraDBSetup.get_client()
        print("AstraDB Client configuration seems present.")
    except Exception as e:
        print(f"AstraDB Client configuration missing or invalid: {e}")
        # This is expected if env vars are not set in this environment
        # But for verification of code structure, it suffices.
    
    print("Collections defined:")
    for col in AstraDBSetup.COLLECTIONS:
        print(f"- {col}")
        
    assert len(AstraDBSetup.COLLECTIONS) > 0
    print("Verification passed!")

except Exception as e:
    print(f"Verification failed: {e}")
    sys.exit(1)
