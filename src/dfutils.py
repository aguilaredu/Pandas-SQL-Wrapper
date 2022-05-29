from pandas import DataFrame
import numpy as np

def fill_dataframe_nulls(df: DataFrame, *args):
    """Fills the dataframe with NULL like values with None. Default list to replace is [np.nan, 'NULL', 'nan'].
    It is possible to add more values to the list using the *args parameter passing the values after the required arguments

    Args:
        df (DataFrame): The dataframe to change the values from.

    Returns:
        [Dataframe]: The dataframe with the replaced values. 
    """

    null_list = [['NULL'], ['nan'], [np.nan]]
    
    # Adds optional items to the null key
    for i in range(len(args)):
        null_list.append([args[i]])

    # Iterate through the null list and replace with np.nan
    for null_value in null_list:
        df.replace([null_value], [np.nan], inplace=True)

    # Fill NAs with np.nan because pandas could've inferred datatypes for certain columns and 
    # replaced the np.nans that were filled in the last step with np.NaT, etc. Then replace the 
    # np.nan with None 
    return_df = df.fillna(np.nan).replace([np.nan], [None])

    return return_df