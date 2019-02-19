with labevents_64 AS
(
    SELECT st1.subject_id AS subject_id
           , st1.hadm_id AS hadm_id
           , CASE WHEN st1.hadm_id IS NULL THEN'in'
                  ELSE 'off'
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
       , item_id
       , MAX( item_name) AS item_name
       , COUNT(1) AS item_lab_cnt
       , ARRAY_AGG( CONCAT( date_id::text, ':', '[', flag, ', ', is_hadm, ']') order by time_id) AS item_lab_arr
FROM labevents_64
--WHERE flag IN( 'abnormal', 'delta')
GROUP BY item_id
         , subject_id
;
