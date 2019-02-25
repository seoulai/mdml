CREATE EXTENSION hstore;
SELECT tmp.subject_id
       , hstore( ARRAY_AGG( tmp.icd9_code), ARRAY_AGG( CONCAT( tmp.icd9_name, ', ', tmp.icd9_code_cnt))) AS icd9_hstore
       -- , ARRAY_AGG( tmp.icd9_code) AS icd9_code_arr
       -- , ARRAY_AGG( CONCAT( tmp.icd9_code_cnt, ',', tmp.icd9_name)) AS icd9_name_arr
       , SUM( tmp.icd9_code_cnt) AS diagnoses_cnt
FROM (
          SELECT st1.subject_id AS subject_id
                 , st1.icd9_code AS icd9_code
                 , MAX( st2.short_title) AS icd9_name
                 , COUNT(1) AS icd9_code_cnt
          FROM diagnoses_icd st1
          LEFT OUTER JOIN d_icd_diagnoses st2
                       ON ( st1.icd9_code = st2.icd9_code)
          WHERE st1.icd9_code IN( '77181', '99591', '99592', '67020', '67022', '67024')
          GROUP BY st1.icd9_code
                   , st1.subject_id
     ) tmp
GROUP BY tmp.subject_id
ORDER BY diagnoses_cnt DESC;
