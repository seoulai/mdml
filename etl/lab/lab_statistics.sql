CREATE EXTENSION hstore;

with labevents_41976 AS
(
    SELECT st1.subject_id AS subject_id
           , st1.hadm_id AS hadm_id
           , CASE WHEN st1.hadm_id IS NULL THEN'off'
                  ELSE 'on'
             END AS is_hadm
           , CASE WHEN st1.charttime BETWEEN st3.intime AND st3.outtime THEN st3.icustay_id ELSE NULL END AS icustay_id
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
    LEFT OUTER JOIN icustays st3
                 ON ( st1.subject_id = st3.subject_id
                      AND st1.hadm_id = st3.hadm_id)
    WHERE st1.subject_id = '41976'
), lab_statistics_41976 AS
(
SELECT subject_id
       , hadm_id
       , MAX( is_hadm) AS is_hadm
       , MAX( icustay_id) AS icustay_id
       , time_id
       , MAX( date_id) AS date_id
       , COUNT(1) AS lab_cnt
       , SUM( CASE WHEN flag = 'normal' THEN 1 ELSE 0 END) AS normal_cnt
       , SUM( CASE WHEN flag = 'abnormal' THEN 1 ELSE 0 END) AS abnormal_cnt
       , SUM( CASE WHEN flag = 'delta' THEN 1 ELSE 0 END) AS delta_cnt
       --, hstore( ARRAY_AGG( item_name), ARRAY_AGG( flag)) AS item_info
FROM labevents_41976
--WHERE item_name = 'Vancomycin'
--WHERE item_name = 'PT'
--WHERE flag = 'abnormal'
--WHERE flag = 'delta'
GROUP BY time_id
         , hadm_id
         , subject_id
ORDER BY time_id
)
SELECT *
       , ROUND( abnormal_cnt / CAST( lab_cnt AS NUMERIC) * 100, 2) AS abnormal_per
FROM lab_statistics_41976
;
