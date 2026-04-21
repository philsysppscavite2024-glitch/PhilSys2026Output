import sys
from pathlib import Path

# Ensure the app directory is in the Python path
sys.path.insert(0, str(Path(__file__).parent))

from app import create_app

app = create_app()

if __name__ == "__main__":
    app.run()
