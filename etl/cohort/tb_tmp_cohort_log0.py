from base_etl import BaseETL
from variables import proper_routes, proper_drug_types, abxs

class TbTmpCohortLog0(BaseETL):

    def run(
        self,
    ):

        df = self.df_from_sql(db_name="mimic", sql=self.gen_sql())
        self.insert(df, db_name="mimic_tmp", tb_name="tb_tmp_cohort_log0")

    def gen_str_list(
        self,
        list_,
    ):
        lo_list = list(map(lambda x: "%%{0}%%".format(x.lower()), list_))
        # print(lo_list)
        ret = repr(lo_list)#[1:-1]
        return ret

    def gen_sql(
        self,
    ):

        conditions = {
            "routes": self.gen_str_list(proper_routes),
            "drug_types": self.gen_str_list(proper_drug_types),
            "drug": self.gen_str_list(abxs),
        }
        # print(conditions)
        sql = """
            SELECT subject_id
                   , hadm_id
                   , startdate AS antibiotic_startdate
                   , enddate AS antibiotic_enddate
                   , drug AS antibiotic_name
                   , drug_type
                   , drug_name_generic
                   , route
              FROM prescriptions
             WHERE LOWER(route) LIKE ANY (ARRAY{routes})
               AND LOWER(drug_type) LIKE ANY (ARRAY{drug_types})
               AND LOWER(drug) LIKE ANY (ARRAY{drug})
        """.format(**conditions)
        # print(sql)
        return sql

if __name__ == "__main__":
    obj = TbTmpCohortLog0()
    obj.run()

