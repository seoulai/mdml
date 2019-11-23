from base_etl import BaseETL


class TbTmpCohortLog2(BaseETL):

    @property
    def sql_dim_positive_culture(
        self,
    ):

        return """
           SELECT subject_id    -- add
                  , hadm_id
                  , chartdate
                  , charttime
                  , spec_type_desc
                  , MAX(CASE WHEN org_name IS NOT NULL AND org_name != '' THEN 1 ELSE 0 END) AS positive_culture
             FROM microbiologyevents
            GROUP BY subject_id    -- add
                     , hadm_id
                     , chartdate
                     , charttime
                     , spec_type_desc
        """

    @property
    def sql_suspected_infection(
        self,
    ):
        params = {
            "cond1": "base_term1 < antibiotic_startdate AND antibiotic_startdate <= end_term1",
            "cond2": "base_term2 < antibiotic_startdate AND antibiotic_startdate <= end_term2",
        }

        return """
            SELECT *
                   , CASE WHEN {cond1} THEN 1
                          WHEN {cond2} THEN 2
                          ELSE 0
                      END
                     AS suspected_infection
              FROM (
                    SELECT st0.subject_id
                           , st0.hadm_id
                           , st0.icustay_id
                           , st0.intime
                           , st0.outtime
                           , st0.dbsource
                           , st0.antibiotic_startdate
                           , st0.antibiotic_enddate
                           , st0.antibiotic_name
                           , st0.drug_type
                           , st0.drug_name_generic
                           , st0.route
                           , st1.chartdate
                           , st1.charttime
                           , st1.spec_type_desc
                           , st1.positive_culture
                           , st1.base_term1
                           , st1.base_term2
                           , st1.end_term1
                           , st1.end_term2
                      FROM (
                                SELECT *
                                  FROM tb_tmp_cohort_log1
                                 WHERE antibiotic_startdate IS NOT NULL
                           ) st0
                      LEFT OUTER JOIN (
                                           SELECT *
                                                  , CASE WHEN charttime IS NOT NULL THEN charttime
                                                         ELSE chartdate 
                                                     END
                                                    AS base_term1

                                                  , CASE WHEN charttime IS NOT NULL THEN charttime + interval '72' hour
                                                         ELSE chartdate + interval '96' hour
                                                     END
                                                    AS end_term1

                                                  , CASE WHEN charttime IS NOT NULL THEN charttime - interval '24' hour
                                                         ELSE chartdate
                                                     END
                                                    AS base_term2 
                                                  , CASE WHEN charttime IS NOT NULL THEN charttime
                                                         ELSE chartdate + interval '24' hour
                                                     END
                                                    AS end_term2
                                             FROM tb_tmp_dim_positive_culture
                                       ) st1
                                   ON (st0.subject_id = st1.subject_id
                                       AND st0.hadm_id = st1.hadm_id
                                      )
                    WHERE (
                               {cond1} OR
                               {cond2}
                          )
                    ) tmp

        """.format(**params)

    def run(
        self,
    ):

        dim_positive_culture = self.df_from_sql(db_name="mimic", sql=self.sql_dim_positive_culture)
        self.insert(dim_positive_culture, db_name="mimic_tmp", tb_name="tb_tmp_dim_positive_culture")
        # print(self.sql_suspected_infection)
        df = self.df_from_sql(db_name="mimic_tmp", sql=self.sql_suspected_infection)
        self.insert(df, db_name="mimic_tmp", tb_name="tb_tmp_cohort_log2")


if __name__ == "__main__":
    obj = TbTmpCohortLog2()
    obj.run()

