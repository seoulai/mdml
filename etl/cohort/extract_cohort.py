from base_etl import BaseETL
from collections import OrderedDict

class ExtractCohort(BaseETL):

    def run(
        self,
    ):

        sql = """
            SELECT *
              FROM tb_tmp_cohort_log2
        """
        df = self.df_from_sql(db_name="mimic_tmp", sql=sql)

        row_cnt, _ = df.shape
        assert row_cnt == df["subject_id"].count()

        for i, s in df.iterrows():
            print(s.to_json(date_format="iso"))
            if i > 100:
                break
        # df.to_excel("cohort.xls")


if __name__ == "__main__":
    obj = ExtractCohort()
    obj.run()

