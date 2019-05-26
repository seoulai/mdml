WITH patients AS
(
    -- https://mimic.physionet.org/tutorials/intro-to-mimic-iii/#5-patient-age-and-mortality
    SELECT st0.subject_id
           , st0.gender
           , ROUND( ( CAST( st1.first_admittime AS date) - CAST(st0.dob AS date)) / 365.242,2) AS age
           , st0.dob AS birth_date
           , st1.first_admittime
           , st1.last_admittime
           , st0.dod AS death_date
           , CASE WHEN expire_flag = 0 THEN 'false'
                  ELSE 'true'
              END
             AS is_death
           , ROUND( ( CAST( st0.dod AS date) - CAST(st0.dob AS date)) / 365.242,2) AS life_span
           --, DATE_PART('year', dod) - DATE_PART('year', dob)AS life_span
    FROM patients st0
    LEFT OUTER JOIN (
                     SELECT subject_id
                            , MIN( admittime) AS first_admittime
                            , MAX( admittime) AS last_admittime
                       FROM admissions
                   GROUP BY subject_id
                     ) st1
                 ON ( st0.subject_id = st1.subject_id
                    )
)
SELECT *
       -- all ages > 89 in the database were replaced with 300
       , CASE WHEN age > 89 THEN '>89'
              WHEN age >= 14 THEN 'adult'
              WHEN age <= 1 THEN 'neonate'
              ELSE 'middle'
        END AS age_group
INTO detailed_patients
FROM patients
;
