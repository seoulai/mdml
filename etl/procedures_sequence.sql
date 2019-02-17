SELECT tmp.subject_id
       , STRING_AGG( tmp.icd9_arr, ' -1 ' order by tmp.time_id)
FROM(
     SELECT st0.subject_id AS subject_id
            , st0.hadm_id AS hadm_id
            , st0.admittime AS time_id
            , st1.hadm_cnt AS hadm_cnt
            , st1.icd9_arr AS icd9_arr
     FROM admissions st0
     LEFT OUTER JOIN (
                         SELECT subject_id
                               , hadm_id
                               , COUNT(1) AS hadm_cnt
                               , STRING_AGG( DISTINCT icd9_code::TEXT, ' ') AS icd9_arr
                         FROM procedures_icd
                         GROUP BY subject_id
                                  , hadm_id) st1
                         ON ( st0.subject_id = st1.subject_id
                              AND st0.hadm_id = st1.hadm_id
                             )
    --WHERE st0.subject_id = '36'
    ) tmp
WHERE hadm_cnt > 1
GROUP BY subject_id
;
