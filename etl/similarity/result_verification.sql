SELECT st1.subject_id
       , st1.hadm_id
       , MAX( st3.admittime) AS time_id
       , ARRAY_AGG( st1.icd9_code) AS icd9_code_array
       --, ARRAY_AGG( st2.short_title) AS icd9_name_array
       --, st1.icd9_code
       --, st2.short_title
FROM diagnoses_icd st1
LEFT OUTER JOIN d_icd_diagnoses st2
             ON ( st1.icd9_code = st2.icd9_code)
LEFT OUTER JOIN admissions st3
             ON ( st1.subject_id = st3.subject_id
                  AND st1.hadm_id = st3.hadm_id)
WHERE st1.subject_id IN( '353', '4577')
GROUP BY st1.hadm_id
         , st1.subject_id
ORDER BY st1.subject_id, time_id
;
