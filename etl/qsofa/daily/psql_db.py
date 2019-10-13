from config import PSQL_ADDRESS
from sqlalchemy import create_engine

class PsqlDB():

    def __init__(
        self,
    ) -> None:
        """Initialize PsqlDB class
        Args:
            None
        Returns:
            None
        """

    def conn(
        self,
        database,
    ):
        engine = create_engine( PSQL_ADDRESS.format(database), encoding='utf-8')
        return engine
