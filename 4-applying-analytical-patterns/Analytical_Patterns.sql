SELECT
    column_name,
    data_type
FROM
    information_schema.columns
WHERE
    table_schema = 'public' -- usually 'public'
    AND table_name = 'game_details';


-- ### Query 1: Track Player State Changes