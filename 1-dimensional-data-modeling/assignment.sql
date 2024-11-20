-- 1. DDL for actors table:

-- CREATE TYPE films_struct AS (
--     film TEXT,
--     votes INTEGER,
--     rating FLOAT,
--     filmid TEXT
--                             );
--
-- CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');
--
-- CREATE TABLE actors (
--     actor TEXT,
--     actor_id TEXT,
--     films films_struct[],
--     quality_class quality_class,
--     is_active BOOLEAN,
--     PRIMARY KEY (actor_id)
-- )

-- INSERT INTO actors
-- WITH film_data AS (
--     SELECT
--         actor AS actor_name,
--         actorid,
--         ARRAY_AGG(
--             ROW(film, votes, rating, filmid)::films_struct
--         ) AS films
--     FROM actor_films
--     GROUP BY actor_name, actorid
-- ),
-- quality_data AS (
--     SELECT
--         actorid,
--         CASE
--             WHEN AVG(rating) > 8 THEN 'star'
--             WHEN AVG(rating) > 7 THEN 'good'
--             WHEN AVG(rating) > 6 THEN 'average'
--             ELSE 'bad'
--         END::quality_class AS quality_class
--     FROM actor_films
--     GROUP BY actorid
-- ),
-- active_data AS (
--     SELECT
--         actorid,
--         MAX(year) = 2021 AS is_active
--     FROM actor_films
--     GROUP BY actorid
-- ),
-- combined_data AS (
--     SELECT
--         f.actor_name AS actor,
--         f.actorid,
--         f.films,
--         q.quality_class,
--         a.is_active
--     FROM film_data f
--     JOIN quality_data q ON f.actorid = q.actorid
--     JOIN active_data a ON f.actorid = a.actorid
-- )
-- SELECT *
-- FROM combined_data;

-- 2. Cumulative table generation query: Write a query that populates the actors table one year at a time.

-- DROP TABLE actors

-- CREATE TABLE actors (
--     actor TEXT,
--     actor_id TEXT,
--     films films_struct[],
--     quality_class quality_class,
--     is_active BOOLEAN,
--     PRIMARY KEY (actor_id)
-- )

INSERT INTO actors
WITH actor_start_years AS (
    SELECT af.actorid, MIN(af.year) AS first_year
    FROM actor_films af
    GROUP BY af.actorid
),
years AS (
    SELECT asy.actorid, generate_series(asy.first_year, 2021) AS year
    FROM actor_start_years asy
),
film_data AS (
    SELECT
        af.actor AS actor,
        af.actorid AS actorid,
        ARRAY_AGG(
            ROW(af.film, af.votes, af.rating, af.filmid)::films_struct
        ) AS films
    FROM actor_films af
    JOIN years y ON af.actorid = y.actorid AND af.year = y.year
    GROUP BY af.actor, af.actorid, y.year
),
quality_data AS (
    SELECT
        af.actorid AS actorid,
        CASE
            WHEN AVG(af.rating) > 8 THEN 'star'
            WHEN AVG(af.rating) > 7 THEN 'good'
            WHEN AVG(af.rating) > 6 THEN 'average'
            ELSE 'bad'
        END::quality_class AS quality_class
    FROM actor_films af
    JOIN years y ON af.actorid = y.actorid AND af.year = y.year
    GROUP BY af.actorid, y.year
),
active_data AS (
    SELECT
        af.actorid AS actorid,
        MAX(af.year) = 2021 AS is_active
    FROM actor_films af
    GROUP BY af.actorid
),
combined_data AS (
    SELECT
        f.actor AS actor,
        f.actorid AS actorid,
        f.films,
        q.quality_class,
        a.is_active
    FROM film_data f
    JOIN quality_data q ON f.actorid = q.actorid
    JOIN active_data a ON f.actorid = a.actorid
)
SELECT *
FROM combined_data;
