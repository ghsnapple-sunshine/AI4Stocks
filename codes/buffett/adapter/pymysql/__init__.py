from pymysql import (
    IntegrityError as py_IntegrityError,
    connect as py_connect,
    DataError as py_DataError,
    DatabaseError as py_DatabaseError,
    ProgrammingError as py_ProgrammingError,
)

connect = py_connect

IntegrityError = py_IntegrityError
DatabaseError = py_DatabaseError
ProgrammingError = py_ProgrammingError
DataError = py_DataError
