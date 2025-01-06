DROP TABLE IF EXISTS actors;

CREATE TABLE actors (
    actorid TEXT PRIMARY KEY, -- Use TEXT instead of INTEGER
    actor VARCHAR(255) NOT NULL,
    films JSONB NOT NULL,
    quality_class VARCHAR(10) NOT NULL CHECK (quality_class IN ('star', 'good', 'average', 'bad')),
    is_active BOOLEAN NOT NULL
);


WITH actor_films_grouped AS (
    SELECT
        actorid, -- actorid remains TEXT
        actor,
        JSONB_AGG(
            JSONB_BUILD_OBJECT(
                'film', film,
                'votes', votes,
                'rating', rating,
                'filmid', filmid
            )
        ) AS films,
        AVG(rating) FILTER (WHERE year = 2023) AS avg_rating_recent_year, -- Calculate average rating for the given year
        MAX(year) = 2023 AS is_active -- Determine if actor is active for the year
    FROM actor_films
    WHERE year <= 2023 -- Include films released up to the specified year
    GROUP BY actorid, actor
)
INSERT INTO actors (actorid, actor, films, quality_class, is_active)
SELECT
    actorid,
    actor,
    films,
    CASE
        WHEN avg_rating_recent_year > 8 THEN 'star'
        WHEN avg_rating_recent_year > 7 THEN 'good'
        WHEN avg_rating_recent_year > 6 THEN 'average'
        ELSE 'bad'
    END AS quality_class, -- Assign quality class
    is_active
FROM actor_films_grouped
ON CONFLICT (actorid) DO UPDATE
SET films = EXCLUDED.films, -- Update films
    quality_class = EXCLUDED.quality_class, -- Update quality class
    is_active = EXCLUDED.is_active; -- Update active status



CREATE TABLE actors_history_scd (
    actorid TEXT NOT NULL, -- Matches the type in the actors table
    actor VARCHAR(255) NOT NULL,
    films JSONB NOT NULL, -- Array of JSON objects for films
    quality_class VARCHAR(10) NOT NULL CHECK (quality_class IN ('star', 'good', 'average', 'bad')),
    is_active BOOLEAN NOT NULL,
    start_date DATE NOT NULL, -- Start date for the record
    end_date DATE, -- End date for the record; NULL for active records
    PRIMARY KEY (actorid, start_date)
);


INSERT INTO actors_history_scd (actorid, actor, films, quality_class, is_active, start_date, end_date)
SELECT
    actorid,
    actor,
    films,
    quality_class,
    is_active,
    CURRENT_DATE AS start_date,
    NULL AS end_date -- Active records have no end date
FROM actors;


WITH latest_data AS (
    SELECT
        actorid,
        actor,
        films,
        quality_class,
        is_active
    FROM actors
),
expired_rows AS (
    UPDATE actors_history_scd
    SET end_date = CURRENT_DATE - INTERVAL '1 day'
    WHERE end_date IS NULL -- Only update active records
    AND (
        quality_class <> (SELECT quality_class FROM latest_data WHERE latest_data.actorid = actors_history_scd.actorid)
        OR is_active <> (SELECT is_active FROM latest_data WHERE latest_data.actorid = actors_history_scd.actorid)
    )
    RETURNING actorid -- Return affected actorid for inserting new records
)
INSERT INTO actors_history_scd (actorid, actor, films, quality_class, is_active, start_date, end_date)
SELECT
    ld.actorid,
    ld.actor,
    ld.films,
    ld.quality_class,
    ld.is_active,
    CURRENT_DATE AS start_date,
    NULL AS end_date
FROM latest_data ld
LEFT JOIN expired_rows er
ON ld.actorid = er.actorid; -- Only insert new records for updated actors
