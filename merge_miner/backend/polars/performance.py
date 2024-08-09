import polars as pl
from enum import Enum
from typing import Any

from ..types import dfg_type
from pm4py.util import constants, exec_utils
from ..constants import CASE_CONCEPT_NAME
from pm4py.util import xes_constants as xes_util
from .utils import get_hour_slots
from pm4py.util.business_hours import soj_time_business_hours_diff

class Parameters(Enum):
    START_TIMESTAMP_KEY = constants.PARAMETER_CONSTANT_START_TIMESTAMP_KEY
    TIMESTAMP_KEY = constants.PARAMETER_CONSTANT_TIMESTAMP_KEY
    ACTIVITY_KEY = constants.PARAMETER_CONSTANT_ACTIVITY_KEY
    CASE_KEY = constants.PARAMETER_CONSTANT_CASEID_KEY
    AGGREGATION_MEASURE = "aggregation_measure"
    BUSINESS_HOURS = "business_hours"
    FILTER_VALUE = "filter_value"
    WORKTIMING = "worktiming"
    FILTER_KEY = "filter_key"
    WEEKENDS = "weekends"

def apply_dfg_performance(
    dataframe: pl.DataFrame,
    parameters: dict[str | Parameters, Any] | None = None
) -> dfg_type:
    """
    Measure performance between couples of attributes in the DFG graph

    Parameters
    ----------
    log
        Log
    parameters
        Possible parameters passed to the algorithms:
            aggregationMeasure -> performance aggregation measure (min, 
                max, mean, median, sum, all, stdev, raw_values). Default:
                mean
            case_key -> Attribute to use as case
            activity_key -> Attribute to use as activity
            timestamp_key -> Attribute to use as timestamp
            start_timestamp_key -> Attribute to use as start timestamp
        - Parameters.BUSINESS_HOURS => calculates the difference of time
            based on the business hours, not the total time. Default: False
        - Parameters.WORKTIMING => work schedule of the company (provided
            as a list where the first number is the start of the work
            time, and the second number is the end of the work time), if
            business hours are enabled Default: [7, 17] (work shift from
            07:00 to 17:00)
        - Parameters.WEEKENDS => indexes of the days of the week that are 
            weekend Default: [6, 7] (weekends are Saturday and Sunday)
        - Parameters.FILTER_KEY => attribute to filter by
        - Parameters.FILTER_VALUE => value to filter by

    Returns
    -------
    dfg
        DFG graph
    """
    def apply_soj_time(df: pl.DataFrame):
        """
        Slower version of the sojourn time calculation, to works
        with polars DataFrame map_batches method.
        """
        time_index = df.columns.index(timestamp_key)
        hour_slots = get_hour_slots(worktiming, weekends)
        next_time_index = df.columns.index("next_start_timestamp")
        duration = df.map_rows(lambda y: soj_time_business_hours_diff(
            y[time_index], y[next_time_index], hour_slots)
        ).rename({ "map": "duration" })
        return df.with_columns(duration = duration["duration"])

    if parameters is None: parameters = {}

    case_key = exec_utils.get_param_value(
        Parameters.CASE_KEY, parameters, CASE_CONCEPT_NAME)
    activity_key = exec_utils.get_param_value(
        Parameters.ACTIVITY_KEY, parameters, xes_util.DEFAULT_NAME_KEY)
    start_timestamp_key = exec_utils.get_param_value(
        Parameters.START_TIMESTAMP_KEY, parameters, None)
    timestamp_key = exec_utils.get_param_value(Parameters.TIMESTAMP_KEY,
                             parameters, xes_util.DEFAULT_TIMESTAMP_KEY)
    aggregation_measure = exec_utils.get_param_value(
        Parameters.AGGREGATION_MEASURE, parameters, "mean")

    business_hours = exec_utils.get_param_value(
        Parameters.BUSINESS_HOURS, parameters, False)
    worktiming = exec_utils.get_param_value(Parameters.WORKTIMING,
                                            parameters, [7, 17])
    weekends = exec_utils.get_param_value(Parameters.WEEKENDS,
                                          parameters, [6, 7])

    filter_key = exec_utils.get_param_value(Parameters.FILTER_KEY,
                                            parameters, None)
    filter_value = exec_utils.get_param_value(Parameters.FILTER_VALUE,
                                              parameters, None)

    if start_timestamp_key is None:
        start_timestamp_key = "start_timestamp"
        dataframe = dataframe.with_columns(
            pl.col(timestamp_key).alias(start_timestamp_key)
        )
    
    columns_to_filter = [
        case_key, activity_key, filter_key,
        start_timestamp_key, timestamp_key,
    ]
    if filter_key is None: columns_to_filter.remove(None)
    events = dataframe.select(columns_to_filter).sort([
        case_key, timestamp_key, start_timestamp_key])
    shifted = events.shift(-1)

    df_successive_rows = events.lazy().with_columns(
        next_case = shifted.get_column(case_key),
        next_activity = shifted.get_column(activity_key),
        duration = pl.lit(0.0, dtype=pl.Float64),
        next_start_timestamp = shifted.get_column(start_timestamp_key),
    ).filter(pl.col(case_key) == pl.col("next_case"))

    if business_hours:
        df_successive_rows = df_successive_rows.map_batches(
            apply_soj_time, projection_pushdown=False)
    else:
        df_successive_rows = df_successive_rows.with_columns(
            (pl.col("next_start_timestamp")
             - pl.col(timestamp_key)).alias("duration"))
    
    if filter_key is not None and filter_value is not None:
        df_successive_rows = df_successive_rows.filter(
            pl.col(filter_key) == filter_value)
    
    dfg_performance = df_successive_rows.group_by(
        [activity_key, "next_activity"]
    )
    
    if aggregation_measure == "median":
        dfg_performance = dfg_performance.agg(
            pl.col("duration").median().alias("agg"))
    elif aggregation_measure == "min":
        dfg_performance = dfg_performance.agg(
            pl.col("duration").min().alias("agg"))
    elif aggregation_measure == "max":
        dfg_performance = dfg_performance.agg(
            pl.col("duration").max().alias("agg"))
    elif aggregation_measure == "stdev":
        dfg_performance = dfg_performance.agg(
            pl.col("duration").std().alias("agg"))
    elif aggregation_measure == "sum":
        dfg_performance = dfg_performance.agg(
            pl.col("duration").sum().alias("agg"))
    elif aggregation_measure == "all":
        return dfg_performance.agg(
            pl.col("duration").median().alias("median"),
            pl.col("duration").min().alias("min"),
            pl.col("duration").max().alias("max"),
            pl.col("duration").std().alias("stdev"),
            pl.col("duration").sum().alias("sum"),
            pl.col("duration").mean().alias("mean")
        ).collect().rows_by_key(
            key=[activity_key, "next_activity"]
        )
    elif aggregation_measure == "raw_values":
        return dfg_performance.agg(
            pl.col("duration").collect().alias("agg")
        ).collect().rows_by_key(
            key=[activity_key, "next_activity"]
        )
    else:
        dfg_performance = dfg_performance.agg(
            pl.col("duration").mean().alias("agg"))

    if not business_hours:
        dfg_performance = dfg_performance.with_columns(
            pl.col("agg").dt.cast_time_unit("ms").dt.seconds()
        )
    
    performance = dfg_performance.collect().rows_by_key(
        key=[activity_key, "next_activity"])

    return {key: value[0] for key, value in performance.items()}
