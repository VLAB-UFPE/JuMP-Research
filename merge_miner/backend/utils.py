from typing import Optional
import polars as pl

from .constants import (CASE_CONCEPT_NAME, ACTIVITY_NAME, TIMESTAMP_NAME,
                        START_TIMESTAMP_NAME)

def filter_columns_by_datetime(df: pl.DataFrame, columns: list | str | None):
    """
    Filter columns arguments if are not datetime

    Parameters
    ------------
    df
        Dataframe
    columns
        Columns that are datetime

    Returns
    ------------
    df
        Dataframe with datetime columns

    """  
    filtered_columns = list()
    if columns is not None:
        if isinstance(columns, list):
            for col in columns:
                if df[col].dtype != pl.Datetime:
                    filtered_columns.append(col)
        else:
            if df[columns].dtype != pl.Datetime:
                filtered_columns.append(columns)
    return filtered_columns

def convert_timestamp_columns_in_df(df: pl.DataFrame, timest_format=None,
                                    timest_columns=None):
    """
    Convert all dataframe columns in a dataframe

    Parameters
    -----------
    df
        Dataframe
    timest_format
        (If provided) Format of the timestamp columns in the CSV file
    timest_columns
        Columns of the CSV that shall be converted into timestamp

    Returns
    ------------
    df
        Dataframe with timestamp columns converted

    """  
    timest_columns = filter_columns_by_datetime(df, timest_columns)
    if timest_columns is not None:
        try:
            if timest_format is None:
                # makes operations faster if non-ISO8601 but anyhow regular dates are provided
                df = df.with_columns(
                    pl.col(timest_columns).str.strptime(pl.Datetime)
                )
            else:
                df = df.with_columns(
                    pl.col(timest_columns).str.strptime(pl.Datetime,
                                                        fmt=timest_format)
                )
        except Exception:
            pass
    return df

def format_dataframe(
    df: pl.DataFrame,
    case_id: str=CASE_CONCEPT_NAME,
    activity_key: str=ACTIVITY_NAME,
    timestamp_key: str=TIMESTAMP_NAME,
    start_timestamp_key: str=START_TIMESTAMP_NAME,
    timestamp_format: Optional[str] = "%Y-%m-%d %H:%M:%S%.f"
) -> pl.DataFrame:
    """
    Give the appropriate format on the dataframe, for process mining purposes

    Parameters
    --------------
    df
        Dataframe
    case_id
        Case identifier column
    activity_key
        Activity column
    timestamp_key
        Timestamp column
    start_timestamp_key
        Start timestamp column
    timestamp_format
        Timestamp format that is provided to Dataframe

    Returns
    --------------
    df
        Dataframe
    """
    string_error = "is not in the dataframe!"
    if case_id not in df.columns:
        raise KeyError(f"{case_id} column (case ID) {string_error}")
    if activity_key not in df.columns:
        raise KeyError(f"{activity_key} column (activity) {string_error}")
    if timestamp_key not in df.columns:
        raise KeyError(f"{timestamp_key} column (timestamp) {string_error}")

    df = df.rename({
        start_timestamp_key: START_TIMESTAMP_NAME,
        timestamp_key: TIMESTAMP_NAME,
        activity_key: ACTIVITY_NAME,
        case_id: CASE_CONCEPT_NAME,
    })

    timestamp_columns = [TIMESTAMP_NAME, START_TIMESTAMP_NAME]
    main_columns = [CASE_CONCEPT_NAME, ACTIVITY_NAME, TIMESTAMP_NAME]
    df = convert_timestamp_columns_in_df(df, timestamp_format,
                                         timestamp_columns)
    return (df.with_columns(pl.col(main_columns).drop_nulls())
              .with_columns(pl.col(CASE_CONCEPT_NAME).cast(pl.Utf8))
              .with_columns(pl.col(ACTIVITY_NAME).cast(pl.Utf8))
              .sort([CASE_CONCEPT_NAME, TIMESTAMP_NAME]))


def get_start_end_activities_df(
    dataframe: pl.DataFrame,
    activity_key: str = ACTIVITY_NAME,
    timestamp_key: str = TIMESTAMP_NAME
):
    """
    Return the start and end activities of the eventlog
    into a dataframe columns.

    Args:
        dataframe (pl.DataFrame): Eventlog dataframe
        activity_key (str, optional): Activity key.
            Defaults to ACTIVITY_NAME.
        timestamp_key (str, optional): Timestamp key.
            Defaults to TIMESTAMP_NAME.
    
    Returns:
        pl.DataFrame: Start and end activities dataframe
    """
    start_end_cols = (dataframe.lazy().sort(timestamp_key)
        .select([
            activity_key, CASE_CONCEPT_NAME
        ]).group_by(CASE_CONCEPT_NAME).agg(
            pl.col(activity_key).first().alias("start_activity"),
            pl.col(activity_key).last().alias("end_activity")
        ).select("start_activity", "end_activity")
        .collect())
    return (start_end_cols.get_column("start_activity"),
            start_end_cols.get_column("end_activity"))

def get_start_end_activities(
    dataframe: pl.DataFrame,
    activity_key: str = ACTIVITY_NAME,
    timestamp_key: str = TIMESTAMP_NAME
) -> tuple[list[str], list[str]]:
    """
    Get the list of start and end activities from the eventlog

    Args:
        dataframe (pl.DataFrame): Eventlog dataframe
        activity_key (str, optional): Activity key.
            Defaults to ORIGINAL_ACTIVITY.
        timestamp_key (str, optional): Timestamp key.
            Defaults to TIMESTAMP_KEY.
    
    Returns:
        tuple[list[str], list[str]]: Start and end activities
    """
    start_column, end_column = get_start_end_activities_df(
        dataframe, activity_key, timestamp_key)

    start_activities = start_column.unique().to_list()
    end_activities = end_column.unique().to_list()
    return start_activities, end_activities

def get_start_end_activities_count(
    dataframe: pl.DataFrame,
    activity_key: str = ACTIVITY_NAME,
    timestamp_key: str = TIMESTAMP_NAME
) -> tuple[dict[str, int], dict[str, int]]:
    """
    Get the count of start and end activities from the eventlog

    Args:
        dataframe (pl.DataFrame): Eventlog dataframe
        activity_key (str, optional): Activity key.
            Defaults to ORIGINAL_ACTIVITY.
        timestamp_key (str, optional): Timestamp key.
            Defaults to TIMESTAMP_KEY.
    
    Returns:
        tuple[dict[str, int], dict[str, int]]: Start and end activities count
    """
    start_column, end_column = get_start_end_activities_df(
        dataframe, activity_key, timestamp_key)

    start_activities = start_column.value_counts().to_dicts()
    start_activities = {x["start_activity"]: x["count"]
                        for x in start_activities}
    end_activities = end_column.value_counts().to_dicts()
    end_activities = {x["end_activity"]: x["count"]
                      for x in end_activities}
    return start_activities, end_activities