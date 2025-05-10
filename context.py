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

    def table_alters(self):
        """
        Alter tables.
        """

        # Get current column names in DB. To be used in determining column additions.
        model_columns = self.get_column_names()
        db_columns_query = f"""
        SELECT column_name, character_maximum_length
        FROM information_schema.columns
        WHERE table_name = '{self._model.name}'
        """
        if self._model.schema:
            db_columns_query += f"\nAND table_schema = '{self._model.schema}'"
        db_columns = {
            column[0]: column[1] \
            for column in Context._run(db_columns_query, include_results=True)
        }

        # Detect table alters.
        column_addition_scripts: list = []
        column_drop_scripts: list = []
        column_length_change_scripts: list = []
        column_rename_scripts: list = []
        old_column_names: list[str] = []
        for model_column in model_columns:

            # Add column renames.
            original_name = getattr(self._model, model_column).original_name
            is_renamed: bool = False
            if original_name is not None and len(original_name) > 0:
                get_column_query = f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{self._model.name}'
                AND column_name = '{original_name}
                """
                if self._model.schema is not None and len(self._model.schema) > 0:
                    get_column_query += f"\nAND table_schema = '{self._model.schema}'"
                if Context._run(get_column_query, include_results=True):
                    old_column_names.append(original_name)
                    is_renamed = True
                    column_rename_scripts.append(f"RENAME COLUMN {original_name} TO {model_column}")

            # Add column additions.
            if not is_renamed and model_column not in db_columns:
                column_addition_scripts.append(
                    f"ADD COLUMN {model_column} {getattr(self._model, model_column).db_data_type}"
                )

        # Add column drops.
        for db_column in db_columns:
            if not hasattr(self._model, db_column) \
            and db_column not in old_column_names:
                column_drop_scripts.append(f"DROP COLUMN {db_column}")

        # Add VARCHAR length updates.
        for model_column in model_columns:
            new_length = getattr(self._model, model_column).length
            if new_length:
                if model_column in db_columns \
                and db_columns[model_column] != new_length:
                    column_length_change_scripts.append(
                        f"ALTER COLUMN {model_column} TYPE VARCHAR({new_length})"
                    )
                else:
                    original_name = getattr(self._model, model_column).original_name
                    if original_name is not None and len(original_name) > 0 \
                    and original_name in db_columns \
                    and db_columns[original_name] != new_length:
                        column_length_change_scripts.append(
                            f"ALTER COLUMN {model_column} TYPE VARCHAR({new_length})"
                        )

        all_queries = []

        # Run column renames.
        for column_rename in column_rename_scripts:
            all_queries.append(f"ALTER TABLE {self.get_table_name()} {column_rename}")

        # Run column additions.
        if len(column_addition_scripts) > 0:
            column_addition_query = f"ALTER TABLE {self.get_table_name()}\n"
            column_addition_query += ",\n".join(column_addition_scripts)
            all_queries.append(column_addition_query)

        # Run column drops.
        if len(column_drop_scripts) > 0:
            column_drop_query = f"ALTER TABLE {self._get_table_name()}\n"
            column_drop_query += ",\n".join(column_drop_scripts)
            all_queries.append(column_drop_scripts)

        # Run VARCHAR length updates.
        if len(column_length_change_scripts) > 0:
            column_length_change_query = f"ALTER TABLE {self._get_table_name()}\n"
            column_length_change_query += ",\n".join(column_length_change_scripts)
            all_queries.append(column_length_change_query)

        for query in all_queries:
            Context._run(query)
