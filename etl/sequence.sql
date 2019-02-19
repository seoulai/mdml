SELECT tmp.subject_id
       , COUNT(1) AS hadm_cnt
       , CONCAT( STRING_AGG( tmp.icd9_procedure_arr, ' -1 ' order by tmp.time_id), ' -2') AS icd9_procedure_sequence
       , CONCAT( STRING_AGG( tmp.icd9_procedure_title_arr, ' -1 ' order by tmp.time_id), ' -2') AS icd9_procedure_title_sequence
       , CONCAT( STRING_AGG( tmp.icd9_diagnoses_arr, ' -1 ' order by tmp.time_id), ' -2') AS icd9_diagnoses_sequence
       , CONCAT( STRING_AGG( tmp.icd9_diagnoses_title_arr, ' -1 ' order by tmp.time_id), ' -2') AS icd9_diagnoses_title_sequence
FROM(
         SELECT st0.subject_id AS subject_id
                , st0.hadm_id AS hadm_id
                , st0.admittime AS time_id
                , st1.icd9_procedure_arr AS icd9_procedure_arr
                , st1.icd9_procedure_title_arr AS icd9_procedure_title_arr
                , st2.icd9_diagnoses_arr AS icd9_diagnoses_arr
                , st2.icd9_diagnoses_title_arr AS icd9_diagnoses_title_arr
         FROM admissions st0
         LEFT OUTER JOIN (
                              SELECT tmp1.subject_id
                                    , tmp1.hadm_id
                                    , STRING_AGG( DISTINCT tmp1.icd9_code::TEXT, ' ') AS icd9_procedure_arr
                                    , STRING_AGG( DISTINCT LOWER(tmp2.short_title::TEXT), '|') AS icd9_procedure_title_arr
                              FROM procedures_icd tmp1
                              LEFT OUTER JOIN d_icd_procedures tmp2
                                           ON (tmp1.icd9_code = tmp2.icd9_code)
                              GROUP BY tmp1.subject_id
                                       , tmp1.hadm_id
                         ) st1
                      ON (
                           st0.subject_id = st1.subject_id
                           AND st0.hadm_id = st1.hadm_id
                         )
         LEFT OUTER JOIN (
                              SELECT tmp1.subject_id
                                    , tmp1.hadm_id
                                    , STRING_AGG( DISTINCT tmp1.icd9_code::TEXT, ' ') AS icd9_diagnoses_arr
                                    , STRING_AGG( DISTINCT LOWER(tmp2.short_title::TEXT), '|') AS icd9_diagnoses_title_arr
                              FROM diagnoses_icd tmp1
                              LEFT OUTER JOIN d_icd_diagnoses tmp2
                                           ON (tmp1.icd9_code = tmp2.icd9_code)
                              GROUP BY tmp1.subject_id
                                       , tmp1.hadm_id
                         ) st2
                      ON (
                           st0.subject_id = st2.subject_id
                           AND st0.hadm_id = st2.hadm_id
                         )
         --WHERE st0.subject_id IN( select DISTINCT subject_id from diagnoses_icd where icd9_code = '99591')
    ) tmp
GROUP BY subject_id
HAVING COUNT(1) > 2
;
