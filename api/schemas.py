from geoalchemy2.shape import to_shape
from marshmallow import post_dump, fields
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field
from marshmallow_sqlalchemy.fields import Nested
from models import Location, Movie, Restaurant, Genre, Actor, FilmingLocation


class LocationSchema(SQLAlchemySchema):
    class Meta:
        model = Location
        load_instance = True

    loc_id = auto_field()
    loc_source_type = auto_field()
    loc_status = auto_field()
    loc_failure_reason = auto_field()
    loc_address_type = auto_field()
    loc_name = auto_field()
    loc_display_name = auto_field()
    loc_latitude = auto_field()
    loc_longitude = auto_field()
    loc_house_number = auto_field()
    loc_road = auto_field()
    loc_neighborhood = auto_field()
    loc_suburb = auto_field()
    loc_county = auto_field()
    loc_city = auto_field()
    loc_state = auto_field()
    loc_iso3166_2_lvl4 = auto_field()
    loc_postcode = auto_field()
    loc_country = auto_field()
    loc_country_code = auto_field()

class SimplifiedLocationSchema(SQLAlchemySchema):
    class Meta:
        model = Location
        load_instance = True

    loc_name = auto_field()  # Name of the location
    loc_display_name = auto_field()  # Full display name
    loc_house_number = auto_field()  # House number
    loc_road = auto_field()  # Road name
    loc_neighborhood = auto_field()  # Neighborhood
    loc_suburb = auto_field()  # Suburb
    loc_city = auto_field()  # City
    loc_county = auto_field()  # County
    loc_state = auto_field()  # State
    loc_postcode = auto_field()  # Postcode/ZIP code
    loc_country = auto_field()  # Country
    loc_country_code = auto_field()  # Country code (ISO)
    loc_iso3166_2_lvl4 = auto_field()  # State or province code
    loc_latitude = auto_field()  # Latitude for geolocation
    loc_longitude = auto_field()  # Longitude for geolocation

class GenreSchema(SQLAlchemySchema):
    class Meta:
        model = Genre
        load_instance = True

    gen_name = auto_field()

class ActorSchema(SQLAlchemySchema):
    class Meta:
        model = Actor
        load_instance = True

    act_name = auto_field()


class MovieSchema(SQLAlchemySchema):
    class Meta:
        model = Movie
        load_instance = True

    mov_imdb_id = auto_field()
    mov_title = auto_field()
    mov_year = auto_field()
    mov_rating = auto_field()
    mov_director = auto_field()
    mov_writer = auto_field()
    mov_overview = auto_field()
    mov_nb_users_ratings = auto_field()

    # Nested relationships for genres and actors
    genres = Nested(GenreSchema, many=True, dump_only=True)
    actors = Nested(ActorSchema, many=True, dump_only=True)

    @post_dump
    def simplify_relationships(self, data, **kwargs):
        # Convert nested genres and actors to simple lists of strings
        data["genres"] = [genre["gen_name"] for genre in data.get("genres", [])]
        data["actors"] = [actor["act_name"] for actor in data.get("actors", [])]
        return data

class SimplifiedMovieSchema(SQLAlchemySchema):
    class Meta:
        model = Movie
        load_instance = True

    mov_imdb_id = auto_field()
    mov_title = auto_field()
    mov_year = auto_field()
    mov_rating = auto_field()
    mov_director = auto_field()
    mov_writer = auto_field()
    mov_overview = auto_field()
    mov_nb_users_ratings = auto_field()

class RestaurantSchema(SQLAlchemySchema):
    class Meta:
        model = Restaurant
        load_instance = True

    res_id = auto_field()
    res_name = auto_field()
    res_doing_business_as_dba = auto_field()
    res_seating_interest_sidewalk = auto_field()
    location = Nested(SimplifiedLocationSchema, many=False)
    distance = fields.Float()  # Add the distance field

class FilmingLocationSchema(SQLAlchemySchema):
    class Meta:
        model = FilmingLocation
        load_instance = True

    fl_location_id = auto_field()
    fl_imdb_id = auto_field()
    location = Nested(SimplifiedLocationSchema, many=False)
    movie = Nested(SimplifiedMovieSchema, many=False)

