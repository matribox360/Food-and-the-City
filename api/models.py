from sqlalchemy import Table, Column, ForeignKey, Integer, String, Float, Text
from sqlalchemy.orm import relationship
from database import db
from geoalchemy2 import Geography

# Association table for movies and genres
fc_genres_movies = Table(
    "fc_genres_movies",
    db.Model.metadata,
    Column("gm_genre_id", ForeignKey("fc_genres.gen_id"), primary_key=True),
    Column("gm_imdb_id", ForeignKey("fc_movies.mov_imdb_id"), primary_key=True)
)

# Association table for movies and actors
fc_actors_movies = Table(
    "fc_actors_movies",
    db.Model.metadata,
    Column("am_actor_id", ForeignKey("fc_actors.act_id"), primary_key=True),
    Column("am_imdb_id", ForeignKey("fc_movies.mov_imdb_id"), primary_key=True)
)

class Location(db.Model):
    __tablename__ = "fc_locations"

    loc_id = Column(Integer, primary_key=True)
    loc_source_type = Column(String(255))
    loc_status = Column(String(50))
    loc_failure_reason = Column(Text)
    loc_address_type = Column(String(255))
    loc_name = Column(Text, index=True)
    loc_display_name = Column(Text)
    loc_latitude = Column(Float)
    loc_longitude = Column(Float)
    loc_geography = Column(Geography("POINT", srid=4326))
    loc_house_number = Column(String(50))
    loc_road = Column(String(255))
    loc_neighborhood = Column(String(255))
    loc_suburb = Column(String(255))
    loc_county = Column(String(255))
    loc_city = Column(String(255))
    loc_state = Column(String(255))
    loc_iso3166_2_lvl4 = Column(String(10))
    loc_postcode = Column(String(20))
    loc_country = Column(String(255))
    loc_country_code = Column(String(10))

class Restaurant(db.Model):
    __tablename__ = "fc_restaurants"

    res_id = Column(Integer, primary_key=True)
    res_name = Column(String(255))
    res_legal_business_name = Column(String(255))
    res_doing_business_as_dba = Column(String(255))
    res_seating_interest_sidewalk = Column(String(15))  # Allowed values: 'sidewalk', 'both', 'openstreets', 'roadway'
    res_landmarkdistrict_terms = Column(Integer)  # Boolean values (1/0)
    res_location_id = Column(Integer, ForeignKey("fc_locations.loc_id"))
    loc_geography = Column(Geography("POINT"))  # For geospatial queries

    location = db.relationship("Location", backref="restaurants")

class Movie(db.Model):
    __tablename__ = "fc_movies"

    mov_imdb_id = db.Column(db.String, primary_key=True)
    mov_title = db.Column(db.String, index=True)
    mov_year = db.Column(db.Integer)
    mov_rating = db.Column(db.Float)
    mov_director = db.Column(db.String)
    mov_writer = db.Column(db.String)
    mov_overview = db.Column(db.String)
    mov_nb_users_ratings = db.Column(db.String)

    genres = relationship("Genre", secondary="fc_genres_movies", back_populates="movies")
    actors = relationship("Actor", secondary="fc_actors_movies", back_populates="movies")

    # Add a relationship for filming locations
    filming_locations = relationship(
        "FilmingLocation",
        primaryjoin="Movie.mov_imdb_id == FilmingLocation.fl_imdb_id",
        back_populates="movie"
    )

class Genre(db.Model):
    __tablename__ = "fc_genres"

    gen_id = db.Column(db.Integer, primary_key=True)
    gen_name = db.Column(db.String)

    movies = relationship("Movie", secondary="fc_genres_movies", back_populates="genres")


class Actor(db.Model):
    __tablename__ = "fc_actors"

    act_id = db.Column(db.Integer, primary_key=True)
    act_name = db.Column(db.String)

    movies = relationship("Movie", secondary="fc_actors_movies", back_populates="actors")

class FilmingLocation(db.Model):
    __tablename__ = "fc_filming_locations"

    fl_location_id = Column(Integer, ForeignKey("fc_locations.loc_id"), primary_key=True)
    fl_imdb_id = Column(String, ForeignKey("fc_movies.mov_imdb_id"), primary_key=True)

    location = db.relationship("Location", backref="filming_locations")
    movie = db.relationship("Movie")
