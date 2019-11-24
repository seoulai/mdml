from variables import suspected_infection_terms
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
                  {suspected_infection_terms}
             FROM microbiologyevents
            GROUP BY subject_id    -- add
                     , hadm_id
                     , chartdate
                     , charttime
                     , spec_type_desc
        """.format(suspected_infection_terms=suspected_infection_terms)

    @property
    def sql_suspected_infection(
        self,
    ):
        params = {
            "cond1": "base_term1 < antibiotic_startdate AND antibiotic_startdate <= end_term1",
            "cond2": "base_term2 < antibiotic_startdate AND antibiotic_startdate <= end_term2",
        }

        return """
            WITH tb_tmp_all AS
            (
                SELECT st0.*
                       , st1.chartdate
                       , st1.charttime
                       , st1.spec_type_desc
                       , st1.positive_culture
                       , st1.base_term1
                       , st1.end_term1
                       , st1.base_term2
                       , st1.end_term2
                  FROM (
                            SELECT *
                              FROM tb_tmp_cohort_log1
                             WHERE antibiotic_startdate IS NOT NULL
                       ) st0
                 LEFT OUTER JOIN tb_tmp_dim_positive_culture st1
                              ON (st0.subject_id = st1.subject_id
                                  AND st0.hadm_id = st1.hadm_id
                                 )
            )
            , tb_suspected_infection_all0 AS
            (
                SELECT *
                       , CASE WHEN {cond1} THEN 1
                              WHEN {cond2} THEN 2
                              ELSE 0
                          END
                         AS suspected_infection
                  FROM tb_tmp_all
            )
            , tb_suspected_infection_all AS
            (

                SELECT *
                       , RANK() OVER (
                             PARTITION BY subject_id
                             ORDER BY antibiotic_startdate
                         ) AS rank
                  FROM tb_suspected_infection_all0
                 WHERE (
                            {cond1} OR
                            {cond2}
                       )
            )
            -- tb_first_suspected_infection
            SELECT *
                   , antibiotic_startdate AS suspected_infection_time
              FROM tb_suspected_infection_all
             WHERE rank = 1
        """.format(**params)

    def run(
        self,
    ):

        # print(self.sql_dim_positive_culture)
        dim_positive_culture = self.df_from_sql(db_name="mimic", sql=self.sql_dim_positive_culture)
        self.insert(dim_positive_culture, db_name="mimic_tmp", tb_name="tb_tmp_dim_positive_culture")
        # print(self.sql_suspected_infection)
        df = self.df_from_sql(db_name="mimic_tmp", sql=self.sql_suspected_infection)
        self.insert(df, db_name="mimic_tmp", tb_name="tb_tmp_cohort_log2")


if __name__ == "__main__":
    obj = TbTmpCohortLog2()
    obj.run()

