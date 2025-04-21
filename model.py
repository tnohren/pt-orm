from meta import Meta

class Model(metaclass=Meta):
    """
    Model for defining table definitions.
    """

    schema: str = None
    name: str = ""

    @classmethod
    def generate_tables(cls):
        """
        Creates all tables.
        """

        for model in cls.__subclasses__():
            model.context.create_table()
