from flask import Blueprint, jsonify, request
from sqlalchemy.sql import text
from database import db
from models import Restaurant, Location
from schemas import RestaurantSchema

restaurants_bp = Blueprint("restaurants", __name__)

# Define schema
restaurants_schema = RestaurantSchema(many=True)
@restaurants_bp.route("/restaurants", methods=["GET"])
def get_restaurants():
    """
    Handle GET requests to fetch restaurants based on various filters.
    """
    # Extract query parameters
    filming_location_id = request.args.get("nearby_filming_location", type=int)
    seating_interest = request.args.get("seating_interest")
    latitude = request.args.get("latitude", type=float)
    longitude = request.args.get("longitude", type=float)
    distance = request.args.get("distance", type=float, default=500)  # Default to 500 meters

    if filming_location_id:
        sql_query = text("""
            SELECT r.res_id, 
                   r.res_name, 
                   r.res_doing_business_as_dba, 
                   r.res_seating_interest_sidewalk, 
                   l_rest.loc_id AS l_loc_id, 
                   l_rest.loc_name AS l_loc_name, 
                   l_rest.loc_city AS l_loc_city, 
                   l_rest.loc_country AS l_loc_country, 
                   l_rest.loc_country_code AS l_loc_country_code, 
                   l_rest.loc_county AS l_loc_county, 
                   l_rest.loc_display_name AS l_loc_display_name, 
                   l_rest.loc_geography AS l_loc_geography, 
                   l_rest.loc_house_number AS l_loc_house_number, 
                   l_rest.loc_iso3166_2_lvl4 AS l_loc_iso3166_2_lvl4, 
                   l_rest.loc_latitude AS l_loc_latitude, 
                   l_rest.loc_longitude AS l_loc_longitude, 
                   l_rest.loc_neighborhood AS l_loc_neighborhood, 
                   l_rest.loc_postcode AS l_loc_postcode, 
                   l_rest.loc_road AS l_loc_road, 
                   l_rest.loc_state AS l_loc_state, 
                   l_rest.loc_suburb AS l_loc_suburb, 
                   ST_Distance(l.loc_geography, l_rest.loc_geography) AS distance
            FROM fc_restaurants r
            JOIN fc_locations l_rest ON r.res_location_id = l_rest.loc_id
            JOIN fc_locations l ON l.loc_id = :filming_location_id
            WHERE (r.res_seating_interest_sidewalk = :seating_interest OR :seating_interest IS NULL)
            ORDER BY distance ASC
            LIMIT 10;
        """)
        params = {"filming_location_id": filming_location_id, "seating_interest": seating_interest}
    elif latitude and longitude:
        sql_query = text("""
            SELECT r.res_id, 
                   r.res_name, 
                   r.res_doing_business_as_dba, 
                   r.res_seating_interest_sidewalk, 
                   l.loc_id AS l_loc_id, 
                   l.loc_name AS l_loc_name, 
                   l.loc_city AS l_loc_city, 
                   l.loc_country AS l_loc_country, 
                   l.loc_country_code AS l_loc_country_code, 
                   l.loc_county AS l_loc_county, 
                   l.loc_display_name AS l_loc_display_name, 
                   l.loc_geography AS l_loc_geography, 
                   l.loc_house_number AS l_loc_house_number, 
                   l.loc_iso3166_2_lvl4 AS l_loc_iso3166_2_lvl4, 
                   l.loc_latitude AS l_loc_latitude, 
                   l.loc_longitude AS l_loc_longitude, 
                   l.loc_neighborhood AS l_loc_neighborhood, 
                   l.loc_postcode AS l_loc_postcode, 
                   l.loc_road AS l_loc_road, 
                   l.loc_state AS l_loc_state, 
                   l.loc_suburb AS l_loc_suburb, 
                   ST_Distance(ST_MakePoint(:longitude, :latitude)::geography, l.loc_geography) AS distance
            FROM fc_restaurants r
            JOIN fc_locations l ON r.res_location_id = l.loc_id
            WHERE ST_DWithin(l.loc_geography, ST_MakePoint(:longitude, :latitude)::geography, :distance)
            ORDER BY distance ASC
LIMIT 10;
        """)
        params = {"latitude": latitude, "longitude": longitude, "distance": distance}
    else:
        return jsonify({"error": "You must provide either 'nearby_filming_location' or 'latitude' and 'longitude'."}), 400

    # Execute the query
    result = db.session.execute(sql_query, params)

    # Map results to Restaurant and Location models
    restaurants = []
    for row in result.mappings():
        # Extract Restaurant fields
        restaurant_data = {
            "res_id": row["res_id"],
            "res_name": row["res_name"],
            "res_doing_business_as_dba": row["res_doing_business_as_dba"],
            "res_seating_interest_sidewalk": row["res_seating_interest_sidewalk"],
        }

        # Extract Location fields
        location_data = {
            "loc_id": row["l_loc_id"],
            "loc_name": row["l_loc_name"],
            "loc_city": row["l_loc_city"],
            "loc_country": row["l_loc_country"],
            "loc_country_code": row["l_loc_country_code"],
            "loc_county": row["l_loc_county"],
            "loc_display_name": row["l_loc_display_name"],
            "loc_geography": row["l_loc_geography"],
            "loc_house_number": row["l_loc_house_number"],
            "loc_iso3166_2_lvl4": row["l_loc_iso3166_2_lvl4"],
            "loc_latitude": row["l_loc_latitude"],
            "loc_longitude": row["l_loc_longitude"],
            "loc_neighborhood": row["l_loc_neighborhood"],
            "loc_postcode": row["l_loc_postcode"],
            "loc_road": row["l_loc_road"],
            "loc_state": row["l_loc_state"],
            "loc_suburb": row["l_loc_suburb"],
        }

        # Add location and distance to restaurant_data
        restaurant_data["location"] = location_data
        restaurant_data["distance"] = row["distance"]

        # Append to the restaurants list
        restaurants.append(restaurant_data)

    # Serialize the results
    return jsonify(restaurants_schema.dump(restaurants))