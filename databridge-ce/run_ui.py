import sys
import os
from ui.server import app

# Add the src directory to the Python path to ensure imports work correctly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

if __name__ == '__main__':
    # Running in debug mode is helpful for development.
    # It provides auto-reloading when files change.
    app.run(debug=True, port=5050)
