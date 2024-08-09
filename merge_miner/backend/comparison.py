from .constants import CASE_CONCEPT_NAME, TIMESTAMP_NAME
from .research_essentials import (get_edges_to_merge_islands,
                                  add_edge_to_islands)
from .utils import get_start_end_activities_count
from .heuristics import partition_case_dfgs
from collections import Counter
from .types import dfg_type
import polars as pl

def partition_df_into_cases_dfgs(event_log: pl.DataFrame, percentage: float
                                 ) -> tuple[dfg_type, dfg_type, dfg_type]:
    """
    Partition the EventLog into three parts and return the Directly-Follows
    where the values are the number of the cases with the edges.

    Parameters
    ------------
    event_log
        EventLog dataframe
    percentage
        Percentage of the traces to be considered as start and end dfgs.
        Defaults to 0.1 (10%)
    
    Returns
    ------------
    tuple[Counter, Counter, Counter]: The partitioned dfgs
    """
    ordered_eventlog = event_log.sort([CASE_CONCEPT_NAME, TIMESTAMP_NAME])
    start_dfgs, middle_dfgs, end_dfgs = list(), list(), list()
    for start_dfg, middle_dfg, end_dfg in partition_case_dfgs(
        ordered_eventlog, percentage):

        middle_dfgs.extend(set(middle_dfg))
        start_dfgs.extend(set(start_dfg))
        end_dfgs.extend(set(end_dfg))
    
    return Counter(start_dfgs), Counter(middle_dfgs), Counter(end_dfgs)

def get_splitted_comparison_dfgs(dataframes: list[pl.DataFrame],
                                 percentage: float=0.25
                                 ) -> tuple[Counter, Counter, Counter]:
    """
    Get the comparison of the Directly-Follows Graphs of the dataframes
    splitted into three parts. The values are the number of the cases with
    the edges and the values are normalized by the number of cases.

    Parameters
    ------------
    dataframes
        List of dataframes
    percentage
        Percentage of the traces to be considered as start and end dfgs.
        Defaults to 0.25 (to split into 3 parts because ceil function)
    
    Returns
    ------------
    tuple[Counter, Counter, Counter]: The comparison of the dfgs
    """
    start_dfgs, middle_dfgs, end_dfgs = list(), list(), list()
    for dataframe in dataframes:
        case_amount = len(dataframe[CASE_CONCEPT_NAME].unique())
        start_dfg, middle_dfg, end_dfg = partition_df_into_cases_dfgs(
            dataframe, percentage)

        start_dfg = {key: value / case_amount * 100
                for key, value in start_dfg.items()}
        middle_dfg = {key: value / case_amount * 100
                for key, value in middle_dfg.items()}
        end_dfg = {key: value / case_amount * 100
                for key, value in end_dfg.items()}

        start_dfgs.append(start_dfg)
        middle_dfgs.append(middle_dfg)
        end_dfgs.append(end_dfg)
    
    return start_dfgs, middle_dfgs, end_dfgs

def aggregate_by_threshold(frequency_list: list[dfg_type],
                           threshold: float=0.5) -> dfg_type:
    '''
    Aggregate the frequencies by a threshold, where the final frequency
    is the sum of the presence of the key in the list of frequencies,
    the key is present when the value is greater than the maximum value
    times the threshold.

    Parameters
    ------------
    frequency_list
        The list of frequencies
    threshold
        The threshold to aggregate the frequencies. Defaults to 0.5.

    Returns
    ------------
    dfg_type: The aggregated frequencies.
    '''
    frequencies: dict[tuple[str, str], list[float]] = dict()
    for count_info in frequency_list:
        for acts, freq in count_info.items():
            if acts not in frequencies:
                frequencies[acts] = list()
            frequencies[acts].append(freq)

    thresholds = { act: max(freq) * threshold
                  for act, freq in frequencies.items() }
    aggregated: dict[tuple[str, str], int] = dict()
    for act, frequencies in frequencies.items():
        count = [1 for f in frequencies if f >= thresholds[act]]
        aggregated[act] = sum(count)
    return aggregated

def get_filtered_edges(dfg: dfg_type, threshold: int
                       ) -> tuple[dfg_type, dfg_type]:
    """
    Get the filtered edges and the tolerance edges where
    the tolerance edges are the edges that have a value
    of the threshold - 1

    Parameters
    ------------
    dfg
        The direct-follows graph
    threshold
        The threshold to filter the edges

    Returns
    ------------
    The filtered edges and the tolerance edges
    """
    filtered_edges: dfg_type = dict()
    tolerance_edges: dfg_type = dict()

    items = sorted(dfg.items(), key=lambda x: x[1], reverse=True)
    for key, value in items:
        if value >= threshold:
            filtered_edges[key] = value
        elif value == threshold - 1:
            tolerance_edges[key] = value
        if value < threshold - 1:
            break
    return filtered_edges, tolerance_edges

def join_filtered_edges(dfgs: list[dfg_type]) -> dfg_type:
    """
    Join the filtered edges of the dfgs

    Parameters
    ------------
    dfgs
        The list of dfgs

    Returns
    ------------
    The joined dfgs
    """
    joined_edges: dfg_type = dict()
    for dfg in dfgs:
        for key, value in dfg.items():
            joined_edges[key] = joined_edges.get(key, 0) + value
    return joined_edges

def get_comparison_dfg(dataframes: list[pl.DataFrame], threshold: float=0.5,
                       filter_count: int=1, percentage: float=0.25, 
                       ) -> dfg_type:
    """
    Get the filtered comparison of the Directly-Follows Graphs of the
    dataframes, where the values are the number of the cases with the edges
    and the values are normalized by the number of cases. Finally it will
    verify if has islands and merge them, if necessary.

    Parameters
    ------------
    dataframes
        List of dataframes
    threshold
        The threshold to consider a valid relation comparing to the max
        occurrences between the dataframes. Defaults to 0.5.
    filter_count
        The minimum number of dataframes to be considered. Defaults to 1.
    percentage
        Percentage of the traces to be considered as start and end dfgs.
        Defaults to 0.25 (to split into 3 parts because ceil function)
    
    Returns
    ------------
    dict: The comparison of the dfgs
    """
    start_dfgs, middle_dfgs, end_dfgs = get_splitted_comparison_dfgs(
        dataframes, percentage)

    start_edges_count = aggregate_by_threshold(start_dfgs, threshold)
    middle_edges_count = aggregate_by_threshold(middle_dfgs, threshold)
    end_edges_count = aggregate_by_threshold(end_dfgs, threshold)

    mf_edges, mt_edges = get_filtered_edges(middle_edges_count,
                                            filter_count)
    sf_edges, st_edges = get_filtered_edges(start_edges_count,
                                            filter_count)
    ef_edges, et_edges = get_filtered_edges(end_edges_count,
                                            filter_count)

    filtered_edges = join_filtered_edges([mf_edges, sf_edges, ef_edges])
    tolerance_edges = join_filtered_edges([mt_edges, st_edges, et_edges])

    clusters: list[set[str]] = list()

    for edge in filtered_edges:
        clusters = add_edge_to_islands(clusters, edge)

    new_edges = get_edges_to_merge_islands(clusters, tolerance_edges)
    filtered_edges = join_filtered_edges([filtered_edges, new_edges])

    return filtered_edges
    

def get_comparison_start_end_acts(dataframes: list[pl.DataFrame],
                                  threshold: float=0.5, filter_count: int=1
                                  ) -> tuple[list[str], list[str]]:
    """
    Get the comparison of the start and end activities of the dataframes.

    Parameters
    ------------
    dataframes
        List of dataframes
    threshold
        The threshold to consider a valid relation comparing to the max
        occurrences between the dataframes. Defaults to 0.5.
    filter_count
        The minimum number of dataframes to be considered. Defaults to 1.
    
    Returns
    ------------
    tuple[list[str], list[str]]: The comparison of the start and end activities
    """
    def filter_acts(dfg):
        activities = list()
        for activity, frequency in dfg.items():
            if frequency >= filter_count:
                activities.append(activity)
            else:
                break
        return activities

    start_activities: list[dict[str, int]] = list()
    end_activities: list[dict[str, int]] = list()
    for dataframe in dataframes:
        sa, ea = get_start_end_activities_count(dataframe)
        start_activities.append(sa)
        end_activities.append(ea)

    filtered_sa = aggregate_by_threshold(start_activities, threshold)
    filtered_ea = aggregate_by_threshold(end_activities, threshold)
    start_activities: list[str] = filter_acts(filtered_sa)
    end_activities: list[str] = filter_acts(filtered_ea)

    return list(set(start_activities)), list(set(end_activities))
