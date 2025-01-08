UPDATE
    fc_locations
SET
    loc_geography = ST_SetSRID(ST_MakePoint(loc_longitude, loc_latitude), 4326) :: geography
WHERE
    loc_latitude IS NOT NULL
    AND loc_longitude IS NOT NULL;

SELECT
    r. *,
    l. *,
    ST_Distance(
        l.loc_geography,
        ST_MakePoint(-73.98014, 40.575552) :: geography
    ) AS distance
FROM
    fc_restaurants r
    JOIN fc_locations l ON r.res_location_id = l.loc_id
WHERE
    l.loc_geography IS NOT NULL
ORDER BY
    distance ASC
LIMIT
    5;


/*
    Filming Locations Endpoint
    Endpoint: /filming-locations

    Purpose: Handles all queries related to filming locations.

    Filters (as query parameters):
    movie_name: Filter by movie name (e.g., /filming-locations?movie_name=Inception).
    genre: Filter by genre (e.g., /filming-locations?genre=Action).
    actor: Filter by actor (e.g., /filming-locations?actor=Leonardo DiCaprio).
    imdb_id: Filter by IMDb ID of the movie (e.g., /filming-locations?imdb_id=tt1375666).
*/
SELECT fl.*, m.*
FROM fc_filming_locations fl
JOIN fc_movies m ON fl.fl_imdb_id = m.mov_imdb_id
LEFT JOIN fc_genres_movies gm ON gm.gm_imdb_id = m.mov_imdb_id
LEFT JOIN fc_genres g ON g.gen_id = gm.gm_genre_id
LEFT JOIN fc_actors_movies am ON am.am_imdb_id = m.mov_imdb_id
LEFT JOIN fc_actors a ON a.act_id = am.am_actor_id
WHERE (m.mov_title ILIKE '%' || :movie_name || '%' OR :movie_name IS NULL)
  AND (g.gen_name ILIKE '%' || :genre || '%' OR :genre IS NULL)
  AND (a.act_name ILIKE '%' || :actor || '%' OR :actor IS NULL)
  AND (m.mov_imdb_id = :imdb_id OR :imdb_id IS NULL);

/*
2. Restaurants Endpoint
Endpoint: /restaurants

Purpose: Handles all queries related to restaurants.

Filters (as query parameters):

nearby_filming_location: Provide a filming_location_id to get nearby restaurants (e.g., /restaurants?nearby_filming_location=123).
seating_interest: Filter by seating interest (e.g., /restaurants?seating_interest=sidewalk).
latitude and longitude: Get restaurants near a specific location (e.g., /restaurants?latitude=40.7128&longitude=-74.0060).
distance: Set a distance radius for nearby queries (default 500 meters, e.g., /restaurants?latitude=40.7128&longitude=-74.0060&distance=1000).
*/
-- For nearby filming location
SELECT r.*, ST_Distance(l.loc_geography, r.loc_geography) AS distance
FROM fc_restaurants r
JOIN fc_locations l ON r.res_location_id = l.loc_id
WHERE l.loc_id = :filming_location_id
  AND (r.res_seating_interest_sidewalk = :seating_interest OR :seating_interest IS NULL)
ORDER BY distance ASC
LIMIT 10;

-- For latitude/longitude
SELECT r.*, ST_Distance(ST_MakePoint(:longitude, :latitude)::geography, r.loc_geography) AS distance
FROM fc_restaurants r
WHERE ST_DWithin(r.loc_geography, ST_MakePoint(:longitude, :latitude)::geography, :distance)
ORDER BY distance ASC
LIMIT 10;

/*
3. Movies Endpoint
Endpoint: /movies

Purpose: Handles all queries related to movies.

Filters (as query parameters):

name: Filter by movie name (e.g., /movies?name=Inception).
year: Filter by release year (e.g., /movies?year=2010).
director: Filter by director (e.g., /movies?director=Christopher Nolan).
has_filming_location: Boolean to filter movies with filming locations (e.g., /movies?has_filming_location=true).
*/

SELECT m.*
FROM fc_movies m
LEFT JOIN fc_filming_locations fl ON fl.fl_imdb_id = m.mov_imdb_id
WHERE (m.mov_title ILIKE '%' || :name || '%' OR :name IS NULL)
  AND (m.mov_year = :year OR :year IS NULL)
  AND (m.mov_director ILIKE '%' || :director || '%' OR :director IS NULL)
  AND (:has_filming_location IS NULL OR (:has_filming_location = TRUE AND fl.fl_location_id IS NOT NULL));


/*
4. Tourism Itinerary Endpoint
Endpoint: /itinerary

Purpose: Allows users to generate a tourism itinerary based on their selected movies.

Filters (as query parameters):

movies: List of IMDb IDs for the movies (e.g., /itinerary?movies=tt1375666,tt0816692).
distance: Set a distance radius for nearby restaurants (default 500 meters).
*/
SELECT m.mov_title, fl.*, r.*, ST_Distance(l1.loc_geography, r.loc_geography) AS distance
FROM fc_movies m
JOIN fc_filming_locations fl ON fl.fl_imdb_id = m.mov_imdb_id
JOIN fc_locations l1 ON fl.fl_location_id = l1.loc_id
JOIN fc_restaurants r ON ST_DWithin(l1.loc_geography, r.loc_geography, :distance)
WHERE m.mov_imdb_id = ANY(:movies)
ORDER BY m.mov_title, distance ASC;
