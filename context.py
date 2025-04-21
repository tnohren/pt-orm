from typing import (
    Any,
    Optional,
    TYPE_CHECKING,
    Union
)

import psycopg2
from psycopg2._psycopg import connection

if TYPE_CHECKING:
    from model import Model

class Context:
    """
    Context Manager for all query operations.
    """

    ########################################################################
    #********************** Class Connection Methods **********************#
    ########################################################################

    _connection: connection = None

    @classmethod
    def establish_connection(cls, creds: dict, autocommit: bool = True):
        """
        Establish connection using given credentials.

        Parameters:
            creds (dict): Credentials for given database.
                Expected keys - "host", "database", "port", "user", "port".

            autocommit (bool): Optional. Default True. Determines if connection will autocommit.
        """

        if cls._connection is None:
            cls._connection = cls.generate_connection(creds)
            cls._connection.autocommit = autocommit

    @classmethod
    def generate_connection(cls, creds: dict) -> connection:
        """
        Generates a new connection using given credentials.

        Parameters:
            creds (dict): Credentials for given database.
                Expected keys - "host", "database", "port", "user", "port".
        """

        try:
            return psycopg2.connect(
                host=creds["host"],
                database=creds["database"],
                user=creds["user"],
                password=creds["password"],
                port=creds["port"]
            )
        except KeyError:
            return None

    ########################################################################
    #************************ Class Query Methods *************************#
    ########################################################################

    @classmethod
    def _run(
        cls,
        query,
        query_parameters: Optional[Any] = None,
        include_results: bool = False
    ) -> Union[None, list[tuple[Any, ...]]]:
        """
        Runs a given query with given parameters. Optionally include results as return value.

        Parameters:
            query (str): Given query to be run.
            query_parameters (list): Given query parameters to include along with the query.
            include_results (bool): Optional. Default False. If True, will return results from
                query.
        """

        cur = cls._connection.cursor()
        cur.execute(query=query, vars=query_parameters)
        if include_results:
            return cur.fetchall()
        return None

    ########################################################################
    #********************** Class Processing Methods **********************#
    ########################################################################

    @classmethod
    def _get_table_name(cls, model: "Model") -> str:
        """
        Get table name of a given model. This includes the model's schema.
        """

        if model.schema:
            return f"{model.name}.{model.name}"
        return model.name

    @classmethod
    def _get_column_names(
        cls,
        model: "Model",
        names_to_skip: Optional[list[str]] = None
    ) -> list[str]:
        """
        Get column names of a given model.
        """

        # Generate list of attribute names to skip.
        if names_to_skip is None:
            names_to_skip = []
        names_to_skip.extend(["schema", "name"])

        # Generate list of column names to return.
        return [
            attribute_name for attribute_name in vars(model) \
            if not attribute_name.startswith("_") \
            and attribute_name not in names_to_skip \
            and not callable(getattr(cls, attribute_name))
        ]

    ########################################################################
    #*********************** Instanced Init Methods ***********************#
    ########################################################################

    def __init__(self, model: "Model"):
        """
        Initializes a Context instance. Responsible for handling query processing for a given model.

        Parameters:
            model (Model): Model to instantiate a Context for.
        """

        self._model = model

    ########################################################################
    #******************** Instanced Processing Methods ********************#
    ########################################################################

    def get_table_name(self) -> str:
        """
        Get column names for this Context instance's model. This includes the models's schema.
        """

        return Context._get_table_name(self._model)
    
    def get_column_names(self, names_to_skip: Optional[list[str]] = None) -> list[str]:
        """
        Get column names for this Context instance's model.
        """

        return Context._get_column_names(self._model, names_to_skip)

    ########################################################################
    #********************** Instanced Query Methods ***********************#
    ########################################################################

    def create_table(self):
        """
        Create database table for this Context instance's model.
        """

        # Get columns to add to create script.
        columns = self.get_column_names()
        columns_query_part = ", ".join([
            f"{column} {getattr(self._model, column).type_str}" for column in columns
        ])

        # Get primary keys to add to create script.
        primary_keys_query_part = ", ".join([
            column for column in columns if getattr(self._model, column).primary_key
        ])

        # Put together full create script.
        table_name = Context._get_table_name(self._model)
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_query_part},
        PRIMARY KEY (
        {primary_keys_query_part}
        ))
        """

        # Run create table script.
        Context._run(query)
