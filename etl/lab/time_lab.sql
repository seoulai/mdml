CREATE EXTENSION hstore;

with labevents_64 AS
(
    SELECT st1.subject_id AS subject_id
           , st1.hadm_id AS hadm_id
           , CASE WHEN st1.hadm_id IS NULL THEN'off'
                  ELSE 'on'
             END AS is_hadm
           , st1.charttime AS time_id
           , CAST( st1.charttime AS date) AS date_id
           , st1.itemid AS item_id
           , CASE WHEN st1.valuenum IS NULL THEN st1.value
                  ELSE CAST( st1.valuenum AS text)
              END AS value
           , st1.valueuom AS unit
           , CASE WHEN st1.flag IS NULL THEN 'normal'
                  ELSE st1.flag
              END AS flag
           , st2.label AS item_name
    FROM labevents st1
    LEFT OUTER JOIN d_labitems st2
                 ON ( st1.itemid = st2.itemid)
    WHERE subject_id = '64'
)
SELECT subject_id
       , hadm_id
       , MAX( is_hadm) AS is_hadm
       , time_id
       , MAX( date_id) AS date_id
       , COUNT(1) AS lab_cnt
       , hstore( ARRAY_AGG( item_name), ARRAY_AGG( flag)) AS item_info
FROM labevents_64
--WHERE item_name = 'Platelet Count'
--WHERE item_name = 'Red Blood Cells'
--WHERE item_name = 'White Blood Cells'
--WHERE flag = 'delta'
GROUP BY time_id
         , hadm_id
         , subject_id
ORDER BY time_id
;
