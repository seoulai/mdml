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

        sql = """
            SELECT date_id
                  , subject_id
                  , MAX(rr) AS rr
              FROM (
                  SELECT CAST(charttime AS DATE) AS date_id
                         , subject_id
                         , CASE WHEN SUBSTRING(value, 1, 1) = '>'
                                THEN CAST(SUBSTRING(value, 2, 2) AS INT)
                                ELSE CAST(valuenum AS INT)
                           END AS rr
                    FROM chartevents
                   WHERE itemid IN ('618', '615', '220210', '224690')
                   ) tmp
             WHERE rr BETWEEN 0 AND 300
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

