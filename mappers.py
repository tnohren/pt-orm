from datetime import datetime
from uuid import UUID as UUID_type

from db_types import (
    ARRAY,
    BOOLEAN,
    DECIMAL,
    JSON,
    NUMERIC,
    TIMESTAMP,
    UUID as DB_UUID,
    VARCHAR
)

DATA_TYPE_MAPPER = {
    bool: BOOLEAN,
    datetime: TIMESTAMP,
    dict: JSON,
    float: DECIMAL,
    int: NUMERIC,
    list: ARRAY,
    str: VARCHAR,
    UUID_type: DB_UUID
}
