from uuid import UUID
from typing import Any, Optional, Union

from defaults import DefaultValue
from mappers import DATA_TYPE_MAPPER

class Column:
    """
    Column class for table/model column definitions.
    """

    def __init__(
        self,
        data_type: type,
        length: Optional[int] = None,
        primary_key: bool = False,
        db_default: Optional[Any] = None,
        original_name: Optional[str] = None,
        value: Optional[Any] = None
    ):
        """
        Instantiates a column.
        """

        self._data_type = data_type
        self._length = length
        self._value = value
        self._db_default = db_default
        self._original_name = original_name
        self._primary_key = primary_key

    def copy(self, value: Optional[Any] = None) -> "Column":
        """
        Create a new instance of this column.
        """

        return Column(
            data_type=self._data_type,
            length=self._length,
            primary_key=self._primary_key,
            db_default=self._db_default,
            original_name=self._original_name,
            value=value or self._value
        )

    @property
    def db_data_type(self) -> str:
        """
        Get string value of database type.
        """

        db_data_type = DATA_TYPE_MAPPER.get(self._data_type)
        if self._data_type == str:
            length = \
                self.length if self.length is not None and self.length > 0 \
                else DefaultValue.VARCHAR_LENGTH.value
            db_data_type = f"{db_data_type}({length})"
        if self._db_default:
            db_data_type = f"{db_data_type} {self._db_default}"
        return db_data_type

    @property
    def primary_key(self) -> bool:
        """
        True if this column is a primary key of its associated table/model.
        """

        return self._primary_key

    @property
    def value(self) -> Any:
        """
        Get appropriately typed value of column.
        """

        if self._value is None:
            return None
        if self._data_type == UUID:
            return str(self._value)
        return self._data_type(self._value)

    @property
    def original_name(self) -> Union[str, None]:
        """
        For renamed columns, get the original name.
        """

        return self._original_name
    
    @property
    def length(self) -> Union[int, None]:
        """
        VARCHAR length.
        """

        return self._length
