import pandas as pd

from sqlalchemy import create_engine
from psql_db import PsqlDB
from sqlalchemy import types


class TbTmpDailyqSOFALog5():

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
            SELECT *
                   , rr_score + bp_score + gcs_score AS qsofa_score
              FROM (
                    SELECT date_id
                           , subject_id
                           , rr
                           , bp
                           , gcs
                           , CASE WHEN rr >= 22 THEN 1 ELSE 0 END AS rr_score
                           , CASE WHEN bp <= 100 THEN 1 ELSE 0 END AS bp_score
                           , CASE WHEN gcs < 15 THEN 1 ELSE 0 END AS gcs_score
                      FROM tb_tmp_daily_qsofa_log4
                   ) tmp
        """
        print(sql)
        return pd.read_sql(sql, engine)

    def insert(
        self,
        df,
    ):

        desc_table = {
            'date_id': types.DATE,
            'subject_id': types.INTEGER,
            'rr': types.INTEGER,
            'bp': types.INTEGER,
            'gcs': types.INTEGER,
            'rr_score': types.INTEGER,
            'bp_score': types.INTEGER,
            'gcs_score': types.INTEGER,
            'qsofa_score': types.INTEGER,
        }

        psql = PsqlDB()
        engine = psql.conn("mimic")

        df.to_sql(
            con=engine,
            name='daily_qsofa',
            if_exists='replace',
            dtype=desc_table,
        )


if __name__ == "__main__":
    obj = TbTmpDailyqSOFALog5()
    obj.run()

