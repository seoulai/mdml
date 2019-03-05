CREATE EXTENSION hstore;

SELECT st0.*
       , st1.*

-- base table
FROM
(
SELECT subject_id
       , gender
       , dob
FROM patients
WHERE subject_id = '64'
) st0
LEFT OUTER JOIN
(SELECT tmp1.subject_id
       , tmp1.hadm_id
       , tmp1.admittime AS time_id
       , tmp2.icd9_code_info AS log_info
       , 'admissons'::text AS log_type_code
FROM
    (
        SELECT *
          FROM admissions
         WHERE subject_id = '64'
    ) tmp1  -- sepsis, seq_num = 3
    JOIN (
          SELECT subject_id
                 , hadm_id
                 , hstore( ARRAY_AGG(icd9_code), ARRAY_AGG(seq_num::text)) AS icd9_code_info
            FROM diagnoses_icd st2
           WHERE subject_id = '64'
            GROUP BY hadm_id
                     , subject_id
         ) tmp2
      ON ( tmp1.subject_id = tmp2.subject_id
           AND tmp1.hadm_id = tmp2.hadm_id
         )

UNION ALL

SELECT tmp1.subject_id AS subject_id
       , tmp1.hadm_id AS hadm_id
       --, (CASE WHEN tmp1.icustay_id IS NULL THEN 0 ELSE 1 END) AS is_icustay
       , tmp1.charttime AS time_id
       , hstore( ARRAY_AGG(tmp2.label), ARRAY_AGG(tmp1.value::text)) AS log_info
       --, tmp2.label AS item_name
       --, tmp1.value AS chart_value
       , 'chart_events'::text AS log_type_code
FROM
(
    SELECT *
    FROM chartevents
    WHERE subject_id = '64'
    LIMIT 10
) tmp1
LEFT OUTER JOIN d_items tmp2
             ON ( tmp1.itemid = tmp2.itemid)
GROUP BY time_id
         , hadm_id
         , subject_id
) st1
ON ( st0.subject_id = st1.subject_id)

;


