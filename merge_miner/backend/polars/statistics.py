from pm4py.statistics.attributes.log.get import Parameters
from ..constants import CASE_CONCEPT_NAME
from pm4py.util import exec_utils
import polars as pl

def get_attribute_values(
    dataframe: pl.DataFrame, attribute_key: str, 
    parameters: dict[str|Parameters, int] | None = None
) -> dict[str, int]:
    """
    Get the attribute values of the dataframe for the specified
    attribute along with their count

    Parameters
    ----------
    dataframe
        Polars dataframe
    attribute_key
        Attribute for which we would like to
        know the values along with their count
    parameters
        Possible parameters of the algorithm

    Returns
    ----------
    attributes
        Dictionary of attributes associated with their count
    """

    if parameters is None:
        parameters = {}

    keep_once_per_case = exec_utils.get_param_value(
        Parameters.KEEP_ONCE_PER_CASE, parameters, False)
    case_id = exec_utils.get_param_value(Parameters.CASE_ID_KEY,
                                         parameters, CASE_CONCEPT_NAME)

    attribute_values = {}
    if keep_once_per_case:
        attribute_counts = (dataframe.groupby(case_id)
            .agg(pl.col(attribute_key).unique())
            .select(attribute_key))
        for count in attribute_counts.iter_rows():
            for value in count[0]:
                if value not in attribute_values:
                    attribute_values[value] = 0
                attribute_values[value] += 1
    else:
        attribute_counts = (dataframe.get_column(attribute_key)
                            .value_counts().rows_by_key(attribute_key))
        for key, value in attribute_counts.items():
            attribute_values[key] = value[0]

    return attribute_values
