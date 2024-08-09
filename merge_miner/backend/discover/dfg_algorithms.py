from ..constants import ACTIVITY_ID_NAME
import polars as pl

def employee_frequency(dataframe: pl.DataFrame, employee: str | int,
                       employee_key: str = "usuarioID",
                       activity_key: str = ACTIVITY_ID_NAME,
                       ) -> dict[str, int]:
    """
    Get the frequency of each activity of an employee from a dataframe.
    Warning: The others activities are not considered.

    Parameters
    ------------
    dataframe (polars.DataFrame)
        The dataframe
    employee (str | int)
        The employee
    employee_key (str, optional)
        The column name of the employee. Defaults to "usuarioID".
    activity_key (str, optional)
        The column name of the activity. Defaults to ORIGINAL_ACTIVITY.

    Returns
    ------------
    dict[str, int]: The frequency of each activity of the employee
    """
    frequency_df = (dataframe.filter(pl.col(employee_key) == employee)
                             .get_column(activity_key).value_counts()
                             .rows_by_key(activity_key)).items()
    return { key:value[0] for key, value in frequency_df }
