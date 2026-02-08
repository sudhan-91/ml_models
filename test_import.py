import sys
import os

# Add the current directory to sys.path so we can import APP
sys.path.append(os.getcwd())

try:
    from APP.main import app
    print("Successfully imported APP.main.app")
except Exception as e:
    print(f"Failed to import APP.main: {e}")
    import traceback
    traceback.print_exc()
