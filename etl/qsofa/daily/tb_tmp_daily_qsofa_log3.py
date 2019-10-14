import pandas as pd

from sqlalchemy import create_engine
from psql_db import PsqlDB


class TbTmpDailyqSOFALog3():

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
                   , SUM(gcs) AS gcs
            FROM (
                  SELECT CAST(charttime AS DATE) AS date_id
                         , subject_id
                         , itemid
                         , MIN(valuenum) AS gcs
                    FROM chartevents
                   WHERE itemid IN ( '723', '223900', '454', '223901', '184', '220739')
                   GROUP BY itemid
                            , subject_id
                            , CAST(charttime AS DATE)
                 ) tmp
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
            name='tb_tmp_daily_qsofa_log3',
            if_exists='replace',
        )


if __name__ == "__main__":
    obj = TbTmpDailyqSOFALog3()
    obj.run()

