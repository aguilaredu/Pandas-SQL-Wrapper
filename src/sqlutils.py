from pyodbc import Connection
from pandas import Series, DataFrame, read_sql_query
from numpy import nan
from re import finditer

def expand_sql_stmt_parameterers_with_df(parametrized_query: str, *filter_series: Series) -> dict:
    """Expands a SQL query parameters using passed series as arguments.   

    Args:
        parametrized_query (str): The query to be expanded. Example: "SELECT * FROM table WHERE column IN (?)"

    Raises:
        ValueError: When the number of parameters in the SQL statement differs from the passed series.
        ValueError: When any passed series is of length 0.

    Returns:
        dict: A dictionary containing the expanded SQL statement and the parameters as a list. {"stmt": str, "params": list}
    """
    try: 
        # Get the index of the "?" symbols in the query
        param_locations = [m.start() for m in finditer("\\?", parametrized_query)]
        
        # Raise error if number of series is not equal to number of parameters 
        if len(filter_series) != len(param_locations):
            raise ValueError("The number of parameters in the query should equal the number of filter series passed as arguments.")
        
        # Convert the SQL query into a list 
        listified_string  = list(parametrized_query)

        params = []
        # Iterate through the param locations and replace with the unique values in the filter dataframes
        for i, df in enumerate(filter_series):
            unique = df.unique().tolist()
            if len(unique) == 0:
                raise ValueError("The series passed as argument should have at least 1 element.")
            params.extend(unique)
            listified_string[param_locations[i]] = ",".join(["?"]*len(unique))

        final_query = "".join(listified_string)

        final = {
            "stmt": final_query,
            "params": params
        }
        return final
    except:
        raise

def perform_safe_truncate_insert(source_df: DataFrame, conn: Connection, schema: str, target_table_name:str)-> int:
    """Perform a safe truncate and insert of a DataFrame in one single transaction and rolls back in case of failure.
    Does not close the connection afterwards.

    Args:
        source_df (DataFrame): The DataFrame that will be inserted into the database.
        conn (Connection): Pyodbc Connection object.
        schema (str): The schema where the target table is.
        target_table_name (str): The name and schema of the target table. Eg. schema.table_name.
        
    Returns:
        status (int): 200 for success and 400 for failure
    """  

    try:
        # Executing truncate statement
        cursor = conn.cursor() 
        cursor.execute(f'TRUNCATE TABLE {schema}.{target_table_name}')
        
        # Preparing the insert statement
        insert_stmt = generate_insert_stmt(source_df, schema, target_table_name)
        cursor.fast_executemany = True
        
        # Adding the dataframe values to the insert statement
        cursor.executemany(insert_stmt['stmt'], insert_stmt['params'])
        
        # Commiting the transaction
        conn.commit()
    except:
        raise


def generate_insert_stmt(source_df: DataFrame, schema: str, target_table_name: str) -> dict:
    """Generates an insert statement for a given DataFrame, schema, and table. DataFrame columns have to match with database
    columns.

    Args:
        source_df (DataFrame): The DataFrame to insert into the database.
        schema (str): The name of the schema in the destination database.
        target_table_name (str): The name of the table in the destination database.

    Returns:
        dict: A dictionary containing the statement data. The key "stmt" contains the parametrized query
        to be ran in the database. The "params" key contains the data to be passed to the statement.
    """

    param_slots = '('+', '.join(['?']*len(source_df.columns))+')'

    insert_statement = f"INSERT INTO {schema}.{target_table_name} ({','.join(source_df.columns)}) VALUES {param_slots}"

    params = source_df.values.tolist()

    stmt_data = {
        "stmt": insert_statement,
        "params": params
    }

    return stmt_data