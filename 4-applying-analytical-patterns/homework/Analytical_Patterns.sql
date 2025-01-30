-- Query 1: Track Player State Changes
WITH player_state_changes AS (
    SELECT
        ps.player_name,
        ps.start_season,
        ps.end_season,
        ps.is_active,
        LAG(ps.is_active) OVER (PARTITION BY ps.player_name ORDER BY ps.start_season) AS prev_is_active,
        LAG(ps.end_season) OVER (PARTITION BY ps.player_name ORDER BY ps.start_season) AS prev_end_season
    FROM players_scd ps
)
SELECT
    player_name,
    start_season,
    CASE
        WHEN prev_is_active IS NULL AND is_active THEN 'New'
        WHEN prev_is_active = TRUE AND is_active = FALSE THEN 'Retired'
        WHEN prev_is_active = TRUE AND is_active = TRUE THEN 'Continued Playing'
        WHEN prev_is_active = FALSE AND is_active = TRUE AND (prev_end_season IS NULL OR prev_end_season < start_season) THEN 'Returned from Retirement'
        WHEN prev_is_active = FALSE AND is_active = FALSE THEN 'Stayed Retired'
        ELSE 'Unknown'
    END AS player_state
FROM player_state_changes
WHERE
    (prev_is_active IS DISTINCT FROM is_active) OR
    prev_is_active IS NULL;



WITH player_changes AS (
    SELECT
        player_name,
        is_active,
        LAG(is_active) OVER (PARTITION BY player_name ORDER BY current_season) AS prev_is_active,
        start_season,
        end_season,
        current_season
    FROM players_scd
)
SELECT
    player_name,
    CASE
        WHEN prev_is_active IS NULL AND is_active = TRUE THEN 'New'
        WHEN prev_is_active = TRUE AND is_active = FALSE THEN 'Retired'
        WHEN prev_is_active = FALSE AND is_active = TRUE THEN 'Returned from Retirement'
        WHEN prev_is_active = FALSE AND is_active = FALSE THEN 'Stayed Retired'
        ELSE 'Continued Playing'
    END AS state_change,
    current_season
FROM player_changes;


-- ### Query 2: Use of GROUPING SETS for Aggregations
WITH game_details_with_season AS (
    SELECT
        gd.game_id,
        gd.player_name,
        gd.team_abbreviation,
        ps.season,
        gd.pts,
        gd.plus_minus
    FROM game_details gd
    JOIN player_seasons ps ON gd.player_name = ps.player_name
),
player_season_points AS (
    SELECT
        player_name,
        season,
        SUM(pts) AS total_points
    FROM game_details_with_season
    GROUP BY player_name, season
),
player_team_points AS (
    SELECT
        player_name,
        team_abbreviation,
        SUM(pts) AS total_points
    FROM game_details_with_season
    GROUP BY player_name, team_abbreviation
),
team_wins AS (
    SELECT
        team_abbreviation,
        COUNT(*) AS games_played,
        SUM(CASE WHEN plus_minus > 0 THEN 1 ELSE 0 END) AS wins
    FROM game_details_with_season
    GROUP BY team_abbreviation
)
SELECT
    CASE
        WHEN GROUPING(player_name) = 1 AND GROUPING(team_abbreviation) = 0 AND GROUPING(season) = 1 THEN 'Team-Wins'
        WHEN GROUPING(player_name) = 0 AND GROUPING(season) = 1 AND GROUPING(team_abbreviation) = 0 THEN 'Player-Season'
        WHEN GROUPING(player_name) = 0 AND GROUPING(team_abbreviation) = 1 AND GROUPING(season) = 0 THEN 'Player-Team'
        ELSE 'Unknown'
    END AS aggregation_type,
    player_name,
    COALESCE(team_abbreviation, season::text) AS detail,
    total_points
FROM (
    -- Player and Season aggregation (Total points by Player-Season)
    SELECT player_name, NULL AS team_abbreviation, season::text AS detail, total_points
    FROM player_season_points

    UNION ALL

    -- Player and Team aggregation (Total points by Player-Team)
    SELECT player_name, team_abbreviation, NULL AS detail, total_points
    FROM player_team_points

    UNION ALL

    -- Team aggregation (Wins for each team)
    SELECT NULL AS player_name, team_abbreviation, NULL AS detail, wins::numeric AS total_points
    FROM team_wins
) AS combined
GROUP BY GROUPING SETS (
    (player_name, team_abbreviation, season),  -- Player, Team, and Season
    (player_name, team_abbreviation),         -- Player and Team
    (player_name, season),                    -- Player and Season
    (team_abbreviation)                       -- Team only
)
ORDER BY total_points DESC;







-- ### Query 3: Most Points for a Team
WITH game_details_with_season AS (
    SELECT
        gd.game_id,
        gd.player_name,
        gd.team_abbreviation,
        ps.season,
        gd.pts
    FROM game_details gd
    JOIN player_seasons ps ON gd.player_name = ps.player_name
),
player_team_points AS (
    SELECT
        player_name,
        team_abbreviation,
        SUM(pts) AS total_points
    FROM game_details_with_season
    GROUP BY player_name, team_abbreviation
)
SELECT
    team_abbreviation,
    player_name,
    total_points
FROM player_team_points
WHERE total_points = (
    SELECT MAX(total_points)
    FROM player_team_points pt
    WHERE pt.team_abbreviation = player_team_points.team_abbreviation
)
ORDER BY team_abbreviation, total_points DESC;


-- ### Query 4: Most Points for a Season
WITH game_details_with_season AS (
    SELECT
        gd.game_id,
        gd.player_name,
        gd.team_abbreviation,
        ps.season,
        gd.pts
    FROM game_details gd
    JOIN player_seasons ps ON gd.player_name = ps.player_name
),
player_season_points AS (
    SELECT
        player_name,
        CAST(season AS TEXT) AS season,
        SUM(pts) AS total_points
    FROM game_details_with_season
    GROUP BY player_name, season
)
SELECT
    season,
    player_name,
    total_points
FROM player_season_points
WHERE total_points = (
    SELECT MAX(total_points)
    FROM player_season_points psp
    WHERE psp.season = player_season_points.season
)
ORDER BY season, total_points DESC;

-- ### Query 5: Most Total Wins
SELECT
    team_abbreviation,
    COUNT(*) AS games_played,
    SUM(CASE WHEN plus_minus > 0 THEN 1 ELSE 0 END) AS wins
FROM game_details
GROUP BY team_abbreviation
ORDER BY wins DESC
LIMIT 1;



-- ### Query 6: Most Games Won in a 90-game Stretch
WITH team_game_wins AS (
    SELECT
        team_abbreviation,
        game_id,
        pts,
        ROW_NUMBER() OVER (PARTITION BY team_abbreviation ORDER BY game_id) AS row_num,
        CASE WHEN plus_minus > 0 THEN 1 ELSE 0 END AS is_win
    FROM game_details
),
rolling_wins AS (
    SELECT
        team_abbreviation,
        row_num,
        SUM(is_win) OVER (PARTITION BY team_abbreviation ORDER BY row_num ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS wins_in_stretch
    FROM team_game_wins
)
SELECT
    team_abbreviation,
    MAX(wins_in_stretch) AS max_wins_in_90_games
FROM rolling_wins
GROUP BY team_abbreviation
ORDER BY max_wins_in_90_games DESC
LIMIT 1;


-- ### Query 7: Longest Streak Over 10 Points for LeBron James
WITH lebron_games AS (
    SELECT
        player_name,
        game_id,
        pts,
        ROW_NUMBER() OVER (PARTITION BY player_name ORDER BY game_id) AS rn,
        CASE WHEN pts > 10 THEN 1 ELSE 0 END AS over_10
    FROM game_details
    WHERE player_name = 'LeBron James'
),
streaks AS (
    SELECT
        player_name,
        game_id,
        over_10,
        SUM(over_10) OVER (PARTITION BY player_name ORDER BY rn) - over_10 AS grp
    FROM lebron_games
)
SELECT
    player_name,
    MAX(streak_length) AS longest_streak
FROM (
    SELECT
        player_name,
        grp,
        COUNT(*) AS streak_length
    FROM streaks
    WHERE over_10 = 1
    GROUP BY player_name, grp
) AS streak_lengths
GROUP BY player_name
ORDER BY longest_streak DESC
LIMIT 1;