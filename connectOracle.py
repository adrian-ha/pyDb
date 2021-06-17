"""
Author: Adrian Haerle
Date 17.06.2021
"""
import pandas as pd
import numpy as np
import cx_Oracle


def get_data(sql_code, conn_params):
    """
    Simple general purpose wrapper to retrieve data from an Oracle SQL database.
    For more tailored and optimized solutions, please refer to the cx_oracle documentation.

    :param sql_code: str of sql code that is executed
    :param conn_params: dict of database connection_parameters
    :return db_content: data table content
    """
    conn = None
    try:
        conn = cx_Oracle.connect(**conn_params)
        curs = conn.cursor()
        curs.execute(sql_code)
        data = curs.fetchall()
        colnames = [desc[0] for desc in curs.description]
        db_content = pd.DataFrame(data)
        db_content.columns = colnames
        curs.close()
        return db_content
    except(Exception, cx_Oracle.Error) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def upload_data(table, data, conn_params):
    """
    Uploads pd.DataFrame into Oracle SQL data table.

    :param table: table name where data is inserted
    :param data: pd.DataTable that should be uploaded
    :param conn_params: dict of database connection_parameters
    :return: finished upload
    """
    conn = None
    #Transforms NaNs to None to ensure that cx_Oracle correctly uploads NULLs to columns of the Oracle data type Number
    data_ = data.where(pd.notnull(data), None)
    data_ = [tuple(i) for i in data_.values.tolist()]

    #Test sql to get the correct column names
    test_sql = "select * from "+str(table)
    try:
        conn = cx_Oracle.connect(**conn_params)
        curs = conn.cursor()

        #Execute sample sql to get correct column headers
        curs.execute(test_sql)
        data_sample = curs.fetchone()
        colnames = [desc[0] for desc in curs.description]

        str_colnames = ",".join(colnames)
        num_col = ",:".join(str(i) for i in list(np.arange(1, len(colnames)+1)))
        sql = "INSERT INTO "+str(table)+"("+str_colnames+") VALUES (:"+num_col+")"
        curs.executemany(sql, data_)
        conn.commit()
        curs.close()
    except(Exception, cx_Oracle.Error) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def execute_sql(sql_code, conn_params):
    """
    The sql code provided is executed. A sample use case is to delete table content via python.

    :param sql_code: str of sql code that is executed
    :param conn_params: dict of database connection_parameters
    :return: execution of sql code
    """
    conn = None
    try:
        conn = cx_Oracle.connect(**conn_params)
        curs = conn.cursor()
        curs.execute(sql_code)
        conn.commit()
        curs.close()
    except(Exception, cx_Oracle.Error) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
