WITH detailed_patients_stat AS
(
SELECT age_group
       , gender
       , COUNT(1) AS patient_cnt
       , SUM( CASE WHEN is_death = 'true' THEN 1 ELSE 0 END) AS mortality_cnt
FROM detailed_patients
GROUP BY age_group
         , gender
)
SELECT *
       , ROUND( mortality_cnt*100.0 / patient_cnt, 2) AS mortality_rate
FROM detailed_patients_stat
;
