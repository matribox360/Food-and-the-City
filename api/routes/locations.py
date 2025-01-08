from flask import Blueprint, jsonify, request
from sqlalchemy.orm import joinedload
from database import db
from models import Location
from schemas import LocationSchema

locations_bp = Blueprint("locations", __name__)

# Define schema
locations_schema = LocationSchema(many=True)
location_schema = LocationSchema()

@locations_bp.route("/locations", methods=["GET"])
def get_locations():
    """
    Handle GET requests to fetch all or filtered locations.
    """
    # Extract query parameters
    city = request.args.get("city")
    suburb = request.args.get("suburb")
    country_code = request.args.get("country_code")

    # Build the query dynamically
    query = db.session.query(Location)

    if city:
        query = query.filter(Location.loc_city.ilike(f"%{city}%"))
    if suburb:
        query = query.filter(Location.loc_suburb.ilike(f"%{suburb}%"))
    if country_code:
        query = query.filter(Location.loc_country_code.ilike(f"%{country_code}%"))

    locations = query.all()

    # Serialize the result
    return jsonify(locations_schema.dump(locations))
