from .constants import CASE_CONCEPT_NAME, TIMESTAMP_NAME
from functools import reduce
from .types import dfg_type
from math import ceil
import polars as pl

def update_cluster(clusters: list, edges: tuple):
    """
    Group the activities based on the edges
    Returning a list in this format:
    [
        set([str, ...]),
        ...
    ]
    """
    edge1, edge2 = edges

    edge1_cluster_index = -1
    edge2_cluster_index = -1

    for index, cluster in enumerate(clusters):
        if edge1 in cluster:
            edge1_cluster_index = index

        if edge2 in cluster:
            edge2_cluster_index = index

        if (edge1_cluster_index != -1 and
            edge2_cluster_index != -1):
            break

    if edge1_cluster_index == edge2_cluster_index:
        if edge1_cluster_index == -1:
            clusters.append(set([edge1, edge2]))
        return clusters

    if (edge1_cluster_index != -1 and
        edge2_cluster_index != -1):
        clusters[edge1_cluster_index] = clusters[edge1_cluster_index].union(clusters[edge2_cluster_index])
        clusters.pop(edge2_cluster_index)

    elif edge1_cluster_index != -1:
        clusters[edge1_cluster_index].add(edge2)
    else:
        clusters[edge2_cluster_index].add(edge1)
    return clusters

def partition_dataframe_into_dfgs(
    event_log: pl.DataFrame,
    activity_key_name: str,
    percentage: float = 0.1
) -> tuple[list[tuple[str, str]]]:
    """
    Partition the EventLog into three parts: start, middle and end dfgs
    by the proportion of the percentage, where the start and end dfgs
    are the first and last x% of the traces, and the middle dfgs are
    the rest of the traces.

    Parameters
    ------------
    event_log
        EventLog dataframe
    activity_key_name
        Column name that identifies the activity name
    percentage
        Percentage of the traces to be considered as start and end dfgs.
        Defaults to 0.1 (10%)
    
    Returns
    ------------
    tuple[list[dfg], list[dfg], list[dfg]] where the first is the start
    dfgs, the second is the middle dfgs and the third is the end dfgs,
    where "dfg" is a tuple[str, str].
    """
    ordered_eventlog = event_log.sort([CASE_CONCEPT_NAME, TIMESTAMP_NAME])
    start_dfgs, middle_dfgs, end_dfgs = [], [], []
    get_trace_edges = lambda events: [
        (events[i], events[i + 1]) for i in range(len(events) - 1)
    ]
    cases = ordered_eventlog.partition_by(CASE_CONCEPT_NAME)

    for case in cases:
        event_list = case.get_column(activity_key_name).to_list()
        side_amount = ceil(len(event_list) * percentage)
        start_events, middle_events, end_events = [], [], []
        for i, event in enumerate(event_list):
            if i < side_amount:
                start_events.append(event)
            elif i >= len(case) - side_amount:
                end_events.append(event)
            else:
                middle_events.append(event)
        if len(start_events) > len(end_events):
            middle_events.insert(0, start_events.pop())

        if len(middle_events) == 0:
            middle_events = [start_events[-1], end_events[0]]
        else:
            start_events.append(middle_events[0])
            end_events.insert(0, middle_events[-1])

        middle_dfgs.extend(get_trace_edges(middle_events))
        start_dfgs.extend(get_trace_edges(start_events))
        end_dfgs.extend(get_trace_edges(end_events))

    return start_dfgs, middle_dfgs, end_dfgs

def merge_partitioned_dfgs(start_edges: dfg_type, middle_edges: dfg_type,
                           end_edges: dfg_type) -> dfg_type:
    """
    Union the start, middle and end dfgs into a single dfg.

    Args:
        start_edges (dfg_type): Diretly-Follows Graph of the start traces
        middle_edges (dfg_type): Directly-Follows Graph of the middle traces
        end_edges (dfg_type): Directly-Follows Graph of the end traces

    Returns:
        edges: Directly-Follows Graph united
    """
    result_edges = middle_edges.copy()
    for edge, value in start_edges.items():
        if edge not in result_edges:
            result_edges[edge] = 0
        result_edges[edge] += value
    for edge, value in end_edges.items():
        if edge not in result_edges:
            result_edges[edge] = 0
        result_edges[edge] += value
    return result_edges

def filter_by_larger_cluster(dfg: dfg_type) -> dfg_type:
    """
    Group the dfg edges and return the largest group.

    Args:
        dfg (dfg_type): Directly-Follows Graph

    Returns:
        dfg_type: Filtered Directly-Follows Graph
    """
    edges_clusters = list()
    for edge in dfg:
        update_cluster(edges_clusters, edge)
    clusters_to_remove = sorted(edges_clusters, reverse=True,
                                key=lambda x: len(x))[1:]
    activities_to_remove = reduce(lambda x, y: x.union(y),
                                  clusters_to_remove, set())
        
    return { (act1, act2): value for (act1, act2), value in dfg.items()
             if act1 not in activities_to_remove
             and act2 not in activities_to_remove}
