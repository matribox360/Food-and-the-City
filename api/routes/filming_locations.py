from flask import Blueprint, jsonify, request
from sqlalchemy.sql import text
from database import db
from models import FilmingLocation, Movie, Genre, Actor
from schemas import FilmingLocationSchema

filming_locations_bp = Blueprint("filming_locations", __name__)

# Define schema
filming_locations_schema = FilmingLocationSchema(many=True)

@filming_locations_bp.route("/filming-locations", methods=["GET"])
def get_filming_locations():
    """
    Handle GET requests to fetch all or filtered filming locations.
    """
    # Extract query parameters
    movie_name = request.args.get("movie_name")
    genre = request.args.get("genre")
    actor = request.args.get("actor")
    imdb_id = request.args.get("imdb_id")

    # Build the query dynamically
    query = db.session.query(FilmingLocation).join(FilmingLocation.movie).join(FilmingLocation.location)

    if movie_name:
        query = query.filter(Movie.mov_title.ilike(f"%{movie_name}%"))
    if genre:
        query = query.join(Movie.genres).filter(Genre.gen_name.ilike(f"%{genre}%"))
    if actor:
        query = query.join(Movie.actors).filter(Actor.act_name.ilike(f"%{actor}%"))
    if imdb_id:
        query = query.filter(Movie.mov_imdb_id == imdb_id)

    # Execute the query
    filming_locations = query.all()

    # Serialize the results
    return jsonify(filming_locations_schema.dump(filming_locations))
