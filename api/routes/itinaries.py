from flask import Blueprint, jsonify, request
from sqlalchemy.sql import text
from database import db

# Create Blueprint
itineraries_bp = Blueprint("itineraries", __name__)

@itineraries_bp.route("/itineraries", methods=["GET"])
def get_itineraries():
    """
    Generate a tourism itinerary based on selected movies and nearby restaurants.
    """
    # Extract query parameters
    imdb_ids = request.args.get("imdb_ids", "")
    distance = request.args.get("distance", type=int, default=400)  # Default to 400 meters

    if not imdb_ids:
        return jsonify({"error": "You must provide a list of IMDb IDs in the 'imdb_ids' parameter."}), 400

    # Convert comma-separated IMDb IDs into a list
    imdb_ids = imdb_ids.split(",")

    # Define SQL query
    sql_query = text("""
        WITH restaurant_locations AS (
            SELECT
                r.res_id,
                r.res_name,
                r.res_doing_business_as_dba,
                r.res_seating_interest_sidewalk,
                l.loc_id AS restaurant_loc_id,
                l.loc_name AS restaurant_loc_name,
                l.loc_city AS restaurant_loc_city,
                l.loc_country AS restaurant_loc_country,
                l.loc_country_code AS restaurant_loc_country_code,
                l.loc_house_number AS restaurant_loc_house_number,
                l.loc_neighborhood AS restaurant_loc_neighborhood,
                l.loc_postcode AS restaurant_loc_postcode,
                l.loc_road AS restaurant_loc_road,
                l.loc_state AS restaurant_loc_state,
                l.loc_suburb AS restaurant_loc_suburb,
                l.loc_geography AS restaurant_geography
            FROM fc_restaurants r
            JOIN fc_locations l ON r.res_location_id = l.loc_id
        )
        SELECT
            m.mov_imdb_id,
            m.mov_title,
            fl.fl_location_id,
            l1.loc_id AS filming_loc_id,
            l1.loc_name AS filming_loc_name,
            l1.loc_display_name AS filming_loc_display_name,
            l1.loc_address_type AS filming_loc_address_type,
            l1.loc_city AS filming_loc_city,
            l1.loc_country AS filming_loc_country,
            rl.res_id AS restaurant_id,
            rl.res_name AS restaurant_name,
            rl.res_doing_business_as_dba,
            rl.res_seating_interest_sidewalk,
            rl.restaurant_loc_id,
            rl.restaurant_loc_city,
            rl.restaurant_loc_country,
            rl.restaurant_loc_country_code,
            rl.restaurant_loc_house_number,
            rl.restaurant_loc_neighborhood,
            rl.restaurant_loc_postcode,
            rl.restaurant_loc_road,
            rl.restaurant_loc_state,
            rl.restaurant_loc_suburb,
            ST_Distance(l1.loc_geography, rl.restaurant_geography) AS distance
        FROM fc_movies m
        JOIN fc_filming_locations fl ON fl.fl_imdb_id = m.mov_imdb_id
        JOIN fc_locations l1 ON fl.fl_location_id = l1.loc_id
        LEFT JOIN restaurant_locations rl ON ST_DWithin(l1.loc_geography, rl.restaurant_geography, :distance)
        WHERE m.mov_imdb_id = ANY(:imdb_ids)
        ORDER BY m.mov_title, l1.loc_name, distance ASC;
    """)

    # Execute query with parameters
    result = db.session.execute(sql_query, {"imdb_ids": imdb_ids, "distance": distance})

    # Process results
    itineraries = {}
    for row in result.mappings():
        mov_imdb_id = row["mov_imdb_id"]
        mov_title = row["mov_title"]
        filming_location_id = row["filming_loc_id"]
        restaurant_id = row["restaurant_id"]

        # Initialize movie in response if not already present
        if mov_imdb_id not in itineraries:
            itineraries[mov_imdb_id] = {
                "mov_imdb_id": mov_imdb_id,
                "mov_title": mov_title,
                "filming_locations": []
            }

        # Find or create the filming location in the response
        filming_location = next(
            (loc for loc in itineraries[mov_imdb_id]["filming_locations"] if loc["id"] == filming_location_id),
            None
        )
        if not filming_location:
            filming_location = {
                "id": filming_location_id,
                "display_name": row["filming_loc_display_name"],
                "address_type": row["filming_loc_address_type"],
                "city": row["filming_loc_city"],
                "country": row["filming_loc_country"],
                "restaurants_nearby": []
            }
            itineraries[mov_imdb_id]["filming_locations"].append(filming_location)

        # Add restaurant to the filming location if available
        if restaurant_id:
            filming_location["restaurants_nearby"].append({
                "id": restaurant_id,
                "name": row["restaurant_name"],
                "doing_business_as": row["res_doing_business_as_dba"],
                "seating_interest": row["res_seating_interest_sidewalk"],
                "location": {
                    "loc_city": row["restaurant_loc_city"],
                    "loc_country": row["restaurant_loc_country"],
                    "loc_country_code": row["restaurant_loc_country_code"],
                    "loc_house_number": row["restaurant_loc_house_number"],
                    "loc_neighborhood": row["restaurant_loc_neighborhood"],
                    "loc_postcode": row["restaurant_loc_postcode"],
                    "loc_road": row["restaurant_loc_road"],
                    "loc_state": row["restaurant_loc_state"],
                    "loc_suburb": row["restaurant_loc_suburb"]
                },
                "distance": row["distance"]
            })

    # Convert the itineraries dictionary into a list for JSON serialization
    return jsonify(list(itineraries.values()))
