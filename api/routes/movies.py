from sqlalchemy.orm import joinedload

from models import Movie, Genre, Actor
from flask import Blueprint, jsonify, request
from sqlalchemy.sql import text
from schemas import MovieSchema
from database import db

movies_bp = Blueprint("movies", __name__)
movie_schema = MovieSchema()
movies_schema = MovieSchema(many=True)

@movies_bp.route("/movies", methods=["GET"])
def get_movies():
    # Extract query parameters
    name = request.args.get("name")
    year = request.args.get("year", type=int)
    director = request.args.get("director")
    has_filming_location = request.args.get("has_filming_location", type=lambda v: v.lower() == 'true')
    genre_name = request.args.get("genre")
    actor_name = request.args.get("actor")

    # Build the query dynamically
    query = db.session.query(Movie).options(
        joinedload(Movie.genres),
        joinedload(Movie.actors)
    )

    if name:
        query = query.filter(Movie.mov_title.ilike(f"%{name}%"))
    if year:
        query = query.filter(Movie.mov_year == year)
    if director:
        query = query.filter(Movie.mov_director.ilike(f"%{director}%"))
    if has_filming_location is not None:
        if has_filming_location:
            query = query.filter(Movie.filming_locations.any())
        else:
            query = query.filter(~Movie.filming_locations.any())
    if genre_name:
        query = query.filter(Movie.genres.any(Genre.gen_name.ilike(f"%{genre_name}%")))
    if actor_name:
        query = query.filter(Movie.actors.any(Actor.act_name.ilike(f"%{actor_name}%")))

    movies = query.all()

    # Use movies_schema to serialize the response
    return jsonify(movies_schema.dump(movies))