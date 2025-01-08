-- Recreate the schema
-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- fc_locations Table
CREATE TABLE fc_locations (
    loc_id SERIAL PRIMARY KEY,
    loc_source_type VARCHAR(255),
    loc_status VARCHAR(50),
    loc_failure_reason TEXT,
    loc_address_type VARCHAR(255),
    loc_name TEXT,
    loc_display_name TEXT,
    loc_latitude DOUBLE PRECISION,
    loc_longitude DOUBLE PRECISION,
    loc_geography GEOGRAPHY(Point, 4326),
    loc_house_number VARCHAR(50),
    loc_road VARCHAR(255),
    loc_neighborhood VARCHAR(255),
    loc_suburb VARCHAR(255),
    loc_county VARCHAR(255),
    loc_city VARCHAR(255),
    loc_state VARCHAR(255),
    loc_ISO3166_2_lvl4 VARCHAR(10),
    loc_postcode VARCHAR(20),
    loc_country VARCHAR(255),
    loc_country_code VARCHAR(10)
);

-- fc_filming_locations Table
CREATE TABLE fc_filming_locations (
    fl_location_id INT REFERENCES fc_locations(loc_id),
    fl_imdb_id VARCHAR(20)
);

-- fc_restaurants Table
CREATE TABLE fc_restaurants (
    res_id SERIAL PRIMARY KEY,
    res_name VARCHAR(255),
    res_legal_business_name VARCHAR(255),
    res_doing_business_as_dba VARCHAR(255),
    res_seating_interest_sidewalk VARCHAR(15) CHECK (
        res_seating_interest_sidewalk IN ('sidewalk', 'both', 'openstreets', 'roadway')
    ),
    res_landmarkdistrict_terms BOOLEAN,
    res_location_id INT REFERENCES fc_locations(loc_id)
);

-- fc_movies Table
CREATE TABLE fc_movies (
    mov_imdb_id VARCHAR(20) PRIMARY KEY,
    mov_title VARCHAR(255),
    mov_year INT,
    mov_director VARCHAR(255),
    mov_writer TEXT,
    mov_overview TEXT,
    mov_rating FLOAT,
    mov_nb_users_ratings VARCHAR(10)
);

-- fc_genres Table
CREATE TABLE fc_genres (
    gen_id SERIAL PRIMARY KEY,
    gen_name VARCHAR(100)
);

-- fc_genres_movies Table
CREATE TABLE fc_genres_movies (
    gm_genre_id INT REFERENCES fc_genres(gen_id),
    gm_imdb_id VARCHAR(20) REFERENCES fc_movies(mov_imdb_id),
    PRIMARY KEY (gm_genre_id, gm_imdb_id)
);

-- fc_actors Table
CREATE TABLE fc_actors (
    act_id SERIAL PRIMARY KEY,
    act_name VARCHAR(255)
);

-- fc_actors_movies Table
CREATE TABLE fc_actors_movies (
    am_actor_id INT REFERENCES fc_actors(act_id),
    am_imdb_id VARCHAR(20) REFERENCES fc_movies(mov_imdb_id),
    PRIMARY KEY (am_actor_id, am_imdb_id)
);