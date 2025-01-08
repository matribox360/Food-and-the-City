-- Drop all tables in order of dependency
DROP TABLE IF EXISTS fc_filming_locations CASCADE;

DROP TABLE IF EXISTS fc_restaurants CASCADE;

DROP TABLE IF EXISTS fc_genres_movies CASCADE;

DROP TABLE IF EXISTS fc_actors_movies CASCADE;

DROP TABLE IF EXISTS fc_genres CASCADE;

DROP TABLE IF EXISTS fc_actors CASCADE;

DROP TABLE IF EXISTS fc_movies CASCADE;

DROP TABLE IF EXISTS fc_locations CASCADE;