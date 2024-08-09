from .types import AnimationData, CircleStep, GetAnimationInfoReturn
from ..constants import ACTIVITY_NAME, TIMESTAMP_NAME, CASE_CONCEPT_NAME
from datetime import datetime, timedelta
from ..types import dfg_type
from functools import reduce
import importlib, logging
import polars as pl

try:
    from extend_polars import get_animation_data_rs
except ImportError:
    logging.error(f"{__name__}: extend_polars.get_animation_data_rs not found.")

DAYS_IN_SECONDS: float = 60 * 60 * 24

def time_elapsed(before_date: datetime, current_date: datetime) -> float:
    """
    Return the time elapsed of current date from first case date
    where the granularity is how many days between these two dates.

    Args:
        before_date (datetime): The target date
        current_date (datetime): The current date
    
    Returns:
        float: The time elapsed in days
    """
    difference_duration: timedelta = current_date - before_date
    difference_in_sec: float = (difference_duration).total_seconds()

    return difference_in_sec / DAYS_IN_SECONDS

def get_animation_data(event_log: pl.DataFrame, freq_dfg: dfg_type,
                       dfg_activities: list[str]) -> GetAnimationInfoReturn:
    """
    Get the data that represents each movement of the cases (the edges),
    and the cases amount, the first and last case date.

    Args:
        event_log (pl.DataFrame): The event log
        freq_dfg (dfg_type): The Directly-Follows Graph
        dfg_activities (list[str]): The activities in the DFG
    
    Returns:
        GetAnimationInfoReturn: The data to be used in the animation
    """
    total_cases = 0
    edges_id = set()
    first_case_date: datetime | None = None
    last_case_date: datetime | None = None
    edges: dict[str, list[CircleStep]] = {
        "unsigned": []
    }
    for case in event_log.partition_by(CASE_CONCEPT_NAME):
        if len(case) == 0: continue
        activity_series_df = case.get_column(ACTIVITY_NAME)
        time_series_df = case.get_column(TIMESTAMP_NAME)
        
        start_case_date = time_series_df[0]
        last_case_date = time_series_df[-1]

        activities: list[str] = list()

        if first_case_date is None: first_case_date = start_case_date

        for i in range(activity_series_df.len()):
            act1 = activity_series_df[i-1]
            act_hash = "n" + str(hash(act1))
            activities.append(act_hash)
            if i == 0: continue
            
            act2: str = activity_series_df[i]
            current_timestamp: datetime = time_series_df[i]
            last_timestamp: datetime = time_series_df[i - 1]

            timestamp = (current_timestamp - last_timestamp).total_seconds()
            start_time = time_elapsed(first_case_date, last_timestamp)
            duration = time_elapsed(last_timestamp, current_timestamp)

            if (act1 not in dfg_activities and
                act2 not in dfg_activities):
                continue

            if (act1, act2) in freq_dfg:
                key = act_hash + "n" + str(hash(act2))
                edges_id.add(key)
            else:
                key = "unsigned"

            if key not in edges:
                edges[key] = []
            
            edges[key].append({
                "activity": act_hash,
                "theFirstOne": i == 1,
                "timestamp": timestamp,
                "startTime": start_time,
                "duration": max(0.3, duration),
                "id": f"circle_{total_cases}_{i}",
                "theLastOne": i == len(activity_series_df) - 1,
            })

        if len(edges_id) == 0: continue
        total_cases += 1

    return {
        "first_case_date": first_case_date,
        "last_case_date": last_case_date,
        "total_cases": total_cases,
        "edges": edges,
    } 

def animation_data_handler(dataframe: pl.DataFrame, freq_dfg: dfg_type,
                           perf_dfg: dfg_type, activity_count: dict[str, int]
                           ) -> AnimationData:
    """
    Get the information to be used in the animation and choose the algorithm
    to use to get the data (Rust or Python). Finally, return the parsed data.

    Args:
        dataframe (pl.DataFrame): The event log
        freq_dfg (dfg_type): The Directly-Follows Graph
        perf_dfg (dfg_type): The Performance Directly-Follows Graph
        activity_count (dict[str, int]): The activities frequency
    
    Returns:
        AnimationData: The data to be used in the animation
    """
    for edge in perf_dfg.copy():
        if edge not in freq_dfg:
            del perf_dfg[edge]
    sorted_items = sorted(perf_dfg.values())
    median_durations = (0, 0)
    if len(sorted_items) > 0:
        median_durations = (sorted_items[0], sorted_items[-1])

    activity_list = set()
    for activities in freq_dfg.keys():
        activity_list = activity_list.union(activities)
    activity_list = list(activity_list)
    
    ordered_eventlog = (dataframe.select([
        CASE_CONCEPT_NAME, ACTIVITY_NAME, TIMESTAMP_NAME
    ]).filter(pl.col(ACTIVITY_NAME).is_in(activity_list))
      .sort(TIMESTAMP_NAME))
    
    dfg_activities = list(reduce(lambda x, y: set(x).union(y),
                                 freq_dfg.keys()))
    if importlib.find_loader('extend_polars') is not None:
        activities_hashes = dict()
        unique_acts = (ordered_eventlog.get_column(ACTIVITY_NAME)
                                       .unique().to_list())
        for act in unique_acts:
            activities_hashes[act] = str(hash(act))

        data = get_animation_data_rs(ordered_eventlog, freq_dfg, 
                                     activities_hashes, dfg_activities)
        info_data = {
            "first_case_date": data[0],
            "last_case_date": data[1],
            "total_cases": data[3],
            "edges": data[2],
        }
    else:
        info_data = get_animation_data(ordered_eventlog, freq_dfg,
                                       dfg_activities)

    nodes_frequency = [{
        "id": "n" + str(hash(key)), "frequency": value
    } for key, value in activity_count.items()]
    
    first_case_date = info_data["first_case_date"]
    last_case_date = info_data["last_case_date"]
    total_duration = time_elapsed(first_case_date, last_case_date)
    
    return {
        "edges": info_data["edges"],
        "nodes": nodes_frequency,
        "end_time": last_case_date,
        "total_cases": info_data["total_cases"],
        "medians": median_durations,
        "start_time": first_case_date,
        "total_duration": total_duration,
    }
