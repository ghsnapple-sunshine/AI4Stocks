from pymysql import connect as py_connect
from pymysql import IntegrityError as py_IntegrityError
from pymysql.err import DatabaseError as py_DatabaseError
from pymysql.err import ProgrammingError as py_ProgrammingError
from pymysql.err import DataError as py_DataError

connect = py_connect

IntegrityError = py_IntegrityError
DatabaseError = py_DatabaseError
ProgrammingError = py_ProgrammingError
DataError = py_DataError
