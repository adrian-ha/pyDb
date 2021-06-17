"""
Author: Adrian Haerle
Date 17.06.2021
"""
import pandas as pd
import numpy as np
import psycopg2


def get_data(sql_code, conn_params):
    """
    Downloads data from table.

    :param sql_code: str of sql code that is executed
    :param conn_params: dict of database connection_parameters
    :return:
    """
    try:
        query_data = execute_sql(sql_code, conn_params)
        df = pd.DataFrame(query_data[1:], columns=query_data[0])
        return df


def upload_data(table, data, conn_params):
    """
    Uploads pd.DataFrame into PostgresSQL data table.

    :param table: table name where data is inserted
    :param data: pd.DataTable that should be uploaded
    :param conn_params: dict of database connection_parameters
    :return: finished upload
    """
    # Sample sql to find out how many columns are to be inserted (based on database table)
    column_sql = 'SELECT * from public."' + str(table_name) + '" LIMIT 1;'
    (upload_rows, upload_columns) = upload_data.shape
    table_name_new = 'public."' + str(table_name) + '"'

    try:
        connection = psycopg2.connect(**params)
        cursor = connection.cursor()
        cursor.execute(column_sql)
        output = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        column_names = '", "'.join(colnames)
        database_columns = len(colnames)

        # Only upload data if the number of upload columns matches with number of columns in database table
        if upload_columns == database_columns:
            try:
                for i in range(0, upload_rows):
                    # Create tuple of values
                    args = tuple(upload_data.values[i])
                    if (upload_columns == 1):
                        # Define query if only one column is to be uploaded
                        query = 'INSERT INTO %s("%s") VALUES(%%s);' % (table_name_new, column_names)
                    else:
                        # Create sufficient number of placeholders if more than 1 column is uploaded
                        second_args = '%s' + ',%s' * (database_columns - 2)
                        query = 'INSERT INTO %s("%s") VALUES(%%s,%s);' % (table_name_new, column_names, second_args)
                    cursor.execute(query, args)
                    connection.commit()
            except:
                print("Error while uploading to PostgreSQL database")
        else:
            print("Number of upload columns does not match")
    except (Exception, psycopg2.Error) as error:
        print(error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def execute_sql(sql_code, conn_params):
    """
    Executes any code in PostgresSQL

    :param sql_code: str of sql code that is executed
    :param conn_params: dict of database connection_parameters
    :return: code execution
    """
    try:
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor()
        cursor.execute(str(sql_code))
        output = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        output.insert(0, colnames)
        return (output)
    except (Exception, psycopg2.Error) as error:
        print(error)
    finally:
        if connection:
            cursor.close()
            connection.close()
