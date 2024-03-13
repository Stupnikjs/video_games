CREATE OR REPLACE TABLE `datalake.video_games` 
(
    game_id INT64 NOT NULL,
    avg_note FLOAT64, 
    user_note INT64, 
    oldest_note DATE,
    latest_note DATE, 

)