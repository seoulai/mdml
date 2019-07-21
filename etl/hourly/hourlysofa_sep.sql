
-- DROP MATERIALIZED VIEW IF EXISTS hourlyqsofa_sep CASCADE;
-- CREATE MATERIALIZED VIEW hourlyqsofa_sep AS
  WITH scorecomp AS
  (
      SELECT
        ie.icustay_id,
        ie.charttime_by_hour,
        v.SysBP_Min,
        v.RespRate_max,
        gcs.MinGCS
      FROM (
             SELECT
               ie.icustay_id,
               date_trunc('hour', ce.charttime) AS charttime_by_hour
             FROM icustays ie
             LEFT JOIN chartevents ce
             ON ie.subject_id = ce.subject_id and ie.hadm_id = ce.hadm_id and ie.icustay_id = ce.icustay_id
           ) ie
        LEFT JOIN vitalshours v
          ON ie.icustay_id = v.icustay_id AND ie.charttime_by_hour = v.charttime_by_hour
        LEFT JOIN gcshours gcs
          ON ie.icustay_id = gcs.icustay_id AND ie.charttime_by_hour = gcs.charttime_by_hour
  )
    , scorecalc AS
  (
    -- Calculate the final score
    -- note that if the underlying data is missing, the component is null
    -- eventually these are treated as 0 (normal), but knowing when data is missing is useful for debugging
      SELECT
        icustay_id,
        charttime_by_hour,
        CASE
        WHEN SysBP_Min IS NULL
          THEN NULL
        WHEN SysBP_Min <= 100
          THEN 1
        ELSE 0 END
          AS SysBP_score,
        CASE
        WHEN MinGCS IS NULL
          THEN NULL
        WHEN MinGCS <= 13
          THEN 1
        ELSE 0 END
          AS GCS_score,
        CASE
        WHEN RespRate_max IS NULL
          THEN NULL
        WHEN RespRate_max >= 22
          THEN 1
        ELSE 0 END
          AS RespRate_score
      FROM scorecomp
  )
  SELECT
    ie.subject_id,
    ie.hadm_id,
    ie.icustay_id,
    ie.charttime_by_hour,
    coalesce(SysBP_score, 0)
    + coalesce(GCS_score, 0)
    + coalesce(RespRate_score, 0)
      AS dailyqsofa,
    SysBP_score,
    GCS_score,
    RespRate_score
  FROM (
         SELECT
           ie.subject_id,
           ie.hadm_id,
           ie.icustay_id,
           date_trunc('hour', ce.charttime) AS charttime_by_hour
         FROM icustays ie
           LEFT JOIN (SELECT c.* FROM
                      chartevents c LEFT JOIN diagnoses_icd di
                      ON c.subject_id = di.subject_id
                      WHERE di.icd9_code IN ('77181', '99591', '99592', '67020', '67022', '67024')) as ce
             ON ie.subject_id = ce.subject_id AND ie.hadm_id = ce.hadm_id AND ie.icustay_id = ce.icustay_id
       ) ie
    LEFT JOIN scorecalc s
      ON ie.icustay_id = s.icustay_id AND ie.charttime_by_hour = s.charttime_by_hour
  ORDER BY ie.icustay_id, charttime_by_hour;
