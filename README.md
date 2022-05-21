# Pandas SQL Wrapper and Utilities
SQL and Pandas convenience functions that are really usefull when designing, developing, and implementing Python ETL pipelines. Works around Pandas, pyodbc and MSSQL. 

## List of implemented functions
1. `expand_sql_stmt_parameterers_with_df()`: Expands a parametrized SQL statement based on the contents of a `DataFrame` (actually a Series). Really useful when you want to `SELECT` from a table based on the results of another query `DataFrame` possibly hosted in another server.
2. `fill_dataframe_nulls()`: Replaces null-like values in the `DataFrame` with `None`. I have observed that `pyodbc` does not play nice with null-like values, specially `NaN` and `NaT`.
3. `generate_insert_stmt()`: Generates a parametrized `INSERT` statement from a `DataFrame`. Way more performant that Pandas inbuilt `INSERT` function since everything is committed in one single statement.
4. `perform_safe_truncate_insert()`: Performs a truncate and an insert in one single transaction. Rolls back in case of failure, thus, ensuring atomicity.