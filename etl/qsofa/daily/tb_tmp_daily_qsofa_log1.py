import pandas as pd

from sqlalchemy import create_engine
from psql_db import PsqlDB


class TbTmpDailyqSOFALog1():

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
        # regexp_matches(value, '\>?(\d*)\.?\d*')
        sql = """
            SELECT date_id
                   , subject_id
                   , MAX( CAST( rr AS INT)) AS rr
              FROM (
                    SELECT CAST(charttime AS DATE) AS date_id
                           , subject_id
                           , (REGEXP_MATCHED(value, '\>?(\d*)'))[1] AS rr
                      FROM chartevents
                    WHERE itemid IN ('618', '615', '220210', '224690')
                   ) tmp
            WHERE LENGTH(rr) != 0
              AND CAST( rr AS INT) BETWEEN 0 AND 300
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
            name='tb_tmp_daily_qsofa_log1',
            if_exists='replace',
        )


if __name__ == "__main__":
    obj = TbTmpDailyqSOFALog1()
    obj.run()

