import pandas as pd

from sqlalchemy import create_engine
from psql_db import PsqlDB


class TbTmpDailyqSOFALog2():

    def run(
        self,
    ):

        df = self.select()
        self.insert(df)

    def select(
        self,
    ):
        psql = PsqlDB()
        engine = psql.conn("mimic")

        sql = """
            SELECT date_id
                   , subject_id
                   , MIN( CAST( bp AS INT)) AS bp
              FROM (
                    SELECT CAST(charttime AS DATE) AS date_id
                           , subject_id
                           , (REGEXP_MATCHES(value, '(\d*)'))[1] AS bp
                      FROM chartevents
                    WHERE itemid IN ( '51', '442', '455', '6701', '220179', '220050')
                   ) tmp
            WHERE LENGTH(bp) != 0
              AND CAST( bp AS INT) BETWEEN 0 AND 400
            GROUP BY subject_id
                     , date_id
        """
        print(sql)
        return pd.read_sql(sql, engine)

    def insert(
        self,
        df,
    ):
        psql = PsqlDB()
        engine = psql.conn("mimic_tmp")

        df.to_sql(
            con=engine,
            name='tb_tmp_daily_qsofa_log2',
            if_exists='replace',
        )


if __name__ == "__main__":
    obj = TbTmpDailyqSOFALog2()
    obj.run()

