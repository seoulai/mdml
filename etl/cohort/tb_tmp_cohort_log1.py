from base_etl import BaseETL

class TbTmpCohortLog1(BaseETL):

    @property
    def sql_icustays(
        self,
    ):
        return """
           SELECT subject_id
                  , hadm_id
                  , icustay_id
                  , intime
                  , outtime
                  , dbsource
             FROM icustays
        """

    @property
    def sql_icu_abx(
        self,
    ):
        return """
           SELECT st0.subject_id
                  , st0.hadm_id
                  , st0.icustay_id
                  , st0.intime
                  , st0.outtime
                  , st0.dbsource
                  , st1.antibiotic_startdate
                  , st1.antibiotic_enddate
                  , st1.antibiotic_name
                  , st1.drug_type
                  , st1.drug_name_generic
                  , st1.route
             FROM tb_tmp_icustays st0
            LEFT OUTER JOIN tb_tmp_cohort_log0 st1
                         ON (st0.subject_id = st1.subject_id
                             AND st0.hadm_id = st1.hadm_id
                            )
        """
    def run(
        self,
    ):

        icustays = self.df_from_sql(db_name="mimic", sql=self.sql_icustays)
        self.insert(icustays, db_name="mimic_tmp", tb_name="tb_tmp_icustays")

        icu_abx = self.df_from_sql(db_name="mimic_tmp", sql=self.sql_icu_abx)
        self.insert(icu_abx, db_name="mimic_tmp", tb_name="tb_tmp_cohort_log1")

if __name__ == "__main__":
    obj = TbTmpCohortLog1()
    obj.run()

