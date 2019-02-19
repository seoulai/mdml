with icd9_arrays AS
( 
    SELECT tmp1.subject_id
          , ARRAY_AGG( DISTINCT tmp1.icd9_code order by tmp1.icd9_code) AS icd9_procedure_arr
          --, ARRAY_AGG( DISTINCT LOWER(tmp2.short_title) order by LOWER(tmp2.short_title)) AS icd9_procedure_title_arr
    FROM procedures_icd tmp1
    LEFT OUTER JOIN d_icd_procedures tmp2
                 ON (tmp1.icd9_code = tmp2.icd9_code)
    GROUP BY tmp1.subject_id
) , tmp AS
(
    SELECT ARRAY(
            SELECT UNNEST(a1)
            INTERSECT
            SELECT UNNEST(a2)
           ) AS i,
           ARRAY(
            SELECT UNNEST(a1)
            UNION
            SELECT UNNEST(a2)
           ) AS u
           , a1, a2
           , base, target
    FROM  (
           SELECT st0.icd9_procedure_arr AS a1
                  , st1.icd9_procedure_arr AS a2
                  , st0.subject_id AS base
                  , st1.subject_id AS target
           FROM
               (SELECT *
               FROM icd9_arrays
               WHERE subject_id = '353') st0,
               (SELECT *
               FROM icd9_arrays
               WHERE subject_id <>'353') st1
           --LIMIT 20
          ) preprocess
)
SELECT base
       , target
       , a1
       , a2
       --, i
       --, u
       , ARRAY_LENGTH(i, 1)/CAST( ARRAY_LENGTH(u, 1) AS FLOAT) AS simple_similarity
FROM tmp
WHERE ARRAY_LENGTH(i, 1) > 0
AND ARRAY_LENGTH(u, 1) > 0
ORDER BY simple_similarity DESC
;
