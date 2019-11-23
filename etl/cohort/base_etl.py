import pandas as pd
from sqlalchemy import create_engine
from psql_db import PsqlDB


class BaseETL():

    def df_from_sql(
        self,
        db_name,
        sql,
    ):
        print(sql)
        psql = PsqlDB()
        engine = psql.conn(db_name)
        return pd.read_sql(sql, engine)

    def insert(
        self,
        df,
        db_name,
        tb_name,
        if_exists="replace",
    ):
        psql = PsqlDB()
        engine = psql.conn(db_name)

        df.to_sql(
            con=engine,
            name=tb_name,
            if_exists=if_exists,
        )


if __name__ == "__main__":
    obj = BaseETL()
    obj.run()

