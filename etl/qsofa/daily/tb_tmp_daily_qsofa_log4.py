import pandas as pd

from sqlalchemy import create_engine
from psql_db import PsqlDB


class TbTmpDailyqSOFALog4():

    def run(
        self,
    ):

        df = self.select()
        self.insert(df)

    def select(
        self,
    ):
        psql = PsqlDB()
        engine = psql.conn("mimic_tmp")

        sql = """
            SELECT st0.date_id
                   , st0.subject_id
                   , st1.rr
                   , st2.bp
                   , st3.gcs
              FROM tb_tmp_daily_qsofa_log0 st0
            LEFT OUTER JOIN tb_tmp_daily_qsofa_log1 st1
                         ON ( st0.date_id = st1.date_id
                              AND st0.subject_id = st1.subject_id
                            )
            LEFT OUTER JOIN tb_tmp_daily_qsofa_log2 st2
                         ON ( st0.date_id = st2.date_id
                              AND st0.subject_id = st2.subject_id
                            )
            LEFT OUTER JOIN tb_tmp_daily_qsofa_log3 st3
                         ON ( st0.date_id = st3.date_id
                              AND st0.subject_id = st3.subject_id
                            )
        """
        print(sql)
        return pd.read_sql(sql, engine)

    def insert(
        self,
        df,
    ):

        # desc_tables = {
        #     'date_id': types.VARCHAR(length=255),
        #     'subject_id': types.VARCHAR(length=255),
        #     'rr': types.VARCHAR(length=10),
        #     'bp': types.VARCHAR(length=255),
        #     'gcs': types.VARCHAR(length=255),
        # }

        psql = PsqlDB()
        engine = psql.conn("mimic_tmp")

        df.to_sql(
            con=engine,
            name='tb_tmp_daily_qsofa_log4',
            if_exists='replace',
            # dtype=desc_table,
        )


if __name__ == "__main__":
    obj = TbTmpDailyqSOFALog4()
    obj.run()

