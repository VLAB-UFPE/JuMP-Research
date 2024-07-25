from .heuristics import (partition_dataframe_into_dfgs,
                         update_cluster, merge_partitioned_dfgs)
from .constants import ACTIVITY_NAME
from collections import Counter
from .types import dfg_type
import polars as pl

def frequency_dfg(eventlog: pl.DataFrame, max_no_of_events: int = 100, 
                  keep_events: list = [], percentage: float = 0.1
                  ) -> dict[tuple[str, str], int]:
    """
    Counts the number of directly follows occurrences, i.e. of the form <...a,b...>.
    Remove the nodes on remove_events, and keep the nodes on keep_events
    Sort the edges_list by frequency and trim the edges of the edges_list
    based on the percentage, from 0% to 50%, eg:
    percentage = 0.1 so will divide the list in three (10% and 80% and 10%).

    Warning: It's used the concept of pointers to manipulate the lists
    """
    def merge_clusters(result_edges: dfg_type, all_edges: dfg_type) -> dfg_type:
        nonlocal edges_clusters

        for edge, value in all_edges.items():
            first, second = edge
            
            first_cluster = -1
            second_cluster = -1
            for index, cluster in enumerate(edges_clusters):
                if first in cluster:
                    first_cluster = index
                if second in cluster:
                    second_cluster = index
                if first_cluster != -1 and second_cluster != -1:
                    break
            
            if first_cluster == second_cluster: continue

            if first_cluster != -1 and second_cluster != -1:
                edges_clusters[first_cluster] = (
                    edges_clusters[first_cluster].union(
                        edges_clusters[second_cluster]))
                edges_clusters.pop(second_cluster)
                result_edges[edge] = value

            if len(edges_clusters) == 1:
                break

        if len(edges_clusters) > 1:
            edges_clusters = sorted(edges_clusters, key=lambda x: len(x),
                                    reverse=True)
            for cluster in edges_clusters[1:]:
                for activity in cluster:
                    keys_list = result_edges.keys()
                    for edge in list(keys_list):
                        if activity in edge:
                            del result_edges[edge]
        return result_edges

    def add_new_edge(result_edges: list, new_edge: tuple):
        unique_nodes.update(new_edge[0])
        unique_edges.add(new_edge[0])
        result_edges.append(new_edge)
        update_cluster(edges_clusters, new_edge[0])

    def add_edge_by_count(result_edges: list, edges_list: list,
                          initial_count: int, count_limit: int):
      index_pop_list = []
      for index, edge in enumerate(edges_list):
            current_count = len(unique_edges) - initial_count
            if current_count >= count_limit: break
            index_pop_list.insert(0, index)
            add_new_edge(result_edges, edge)
      for index in index_pop_list:
            edges_list.pop(index)

    def filter_by_count(edges_list: list, count: int) -> list:
        result_edges = []
        keep_list = set(keep_events)
        initial_count = len(unique_edges)
        sort_key = lambda x: (x[1], x[0][0], x[0][1])
        edges_list = sorted(edges_list, key=sort_key, reverse=True)
        
        new_count = count - len(keep_events)
        add_edge_by_count(result_edges, edges_list,
                          initial_count, new_count)

        for edge in edges_list:
            current_count = len(unique_edges) - initial_count
            if current_count >= count: break
            (a, b), _ = edge
            if (a in keep_list or b in keep_list) and (
                a in unique_nodes or b in unique_nodes
            ):
                keep_list = keep_list.difference({ a, b })
                add_new_edge(result_edges, edge)

        add_edge_by_count(result_edges, edges_list,
                          initial_count, count)
        return result_edges

    start_dfgs, middle_dfgs, end_dfgs = partition_dataframe_into_dfgs(
        eventlog, ACTIVITY_NAME, percentage)

    middle_edges = list(Counter(middle_dfgs).items())
    start_edges = list(Counter(start_dfgs).items())
    end_edges = list(Counter(end_dfgs).items())
    all_edges = Counter()
    all_edges.update(middle_edges)
    all_edges.update(start_edges)
    all_edges.update(end_edges)

    unique_edges = set()
    unique_nodes = set()
    edges_clusters = list()

    sides_count = round(max_no_of_events * percentage)
    middle_count = max_no_of_events - sides_count * 2
    
    start_edges = filter_by_count(start_edges, sides_count)
    end_edges = filter_by_count(end_edges, sides_count)
    result_edges = filter_by_count(middle_edges, middle_count)

    result_edges = merge_partitioned_dfgs(dict(start_edges),
                        dict(result_edges), dict(end_edges))

    if len(edges_clusters) > 1:
        result_edges = merge_clusters(result_edges, all_edges)

    return result_edges
