from pandas import DataFrame
from numpy import nan

def fill_dataframe_nulls(df: DataFrame, *args) -> DataFrame:
    """Fills DataFrame with null-like values with None. Default list to replace is [np.nan, 'NULL', 'nan'].
    It is possible to add more values to the list using the *args parameter passing the values after the required arguments

    Args:
        df (DataFrame): The dataframe to change the values from.

    Returns:
        [Dataframe]: The dataframe with the replaced values. 
    """

    null_list = [['null'], ['NULL'], ['nan'], ['NAN'], ['None'], [nan]]
    
    # Adds optional items to the null key
    for i in range(len(args)):
        null_list.append([args[i]])

    # Iterate through the null list and replace with np.nan
    for null_value in null_list:
        df.replace([null_value], [nan], inplace=True)