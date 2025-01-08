from flask import Flask, jsonify
from database import init_app, db
from routes.locations import locations_bp
from routes.movies import movies_bp
from routes.filming_locations import filming_locations_bp
from routes.restaurants import restaurants_bp
from routes.itinaries import itineraries_bp
import json

app = Flask(__name__)

# Initialize database
init_app(app)

# Register blueprints
app.register_blueprint(movies_bp, url_prefix="/api/v1")
app.register_blueprint(locations_bp, url_prefix="/api/v1")
app.register_blueprint(filming_locations_bp, url_prefix="/api/v1")
app.register_blueprint(restaurants_bp, url_prefix="/api/v1")
app.register_blueprint(itineraries_bp, url_prefix="/api/v1")

# Load metadata from JSON file
METADATA_FILE = 'api_metadata.json'

try:
    with open(METADATA_FILE, 'r', encoding='utf-8') as file:
        api_metadata = json.load(file)
except FileNotFoundError:
    api_metadata = {"error": "Metadata file not found. Please ensure 'api_metadata.json' exists."}

# Route for API metadata
@app.route('/api/v1/metadata', methods=['GET'])
def get_metadata():
    """
    Endpoint to retrieve API metadata.
    """
    return jsonify(api_metadata), 200

@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)