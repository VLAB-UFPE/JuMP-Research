from functools import reduce
from ..types import dfg_type

def search_for_island(activity_a: str, activity_b: str,
                      islands: list[set[str]]
                      ) -> tuple[set[str], set[str]]:
    """
    Search for the islands where the activities are in

    Parameters
    ------------
    activity_a
        The first activity
    activity_b
        The second activity
    islands
        The list of islands

    Returns
    ------------
    The islands where the activities are in
    """
    island_a = None
    island_b = None

    for island in islands:
        if activity_a in island:
            island_a = island
        if activity_b in island:
            island_b = island

        if island_a and island_b:
            break

    return island_a, island_b

def add_edge_to_islands(islands: list[set[str]],
                        edges: tuple[str, str]) -> list[set[str]]:
    """
    Add a new edge to the islands, where:
    - If both edges are in the same island, do nothing
    - If both edges are in different islands, merge the islands
    - If one edge is in a island and the other is not, add the edge to the island
    - If both edges are not in any island, create a new island with them

    Parameters
    ------------
    islands
        The list of islands
    edges
        The edges to add

    Returns
    ------------
    The updated islands
    """
    edge1, edge2 = edges
    result_islands = islands.copy()

    island_1, island_2 = search_for_island(edge1, edge2, islands)
    if island_1 is island_2:
        if island_1 is None:
            result_islands.append(set([edge1, edge2]))
        return result_islands

    if island_1 and island_2:
        island_1.update(island_2)
        result_islands.remove(island_2)
    elif island_1:
        island_1.add(edge2)
    elif island_2:
        island_2.add(edge1)
    else:
        result_islands.append(set([edge1, edge2]))

    return result_islands

def get_edges_to_merge_islands(islands: list[set[str]],
                               all_edges: dfg_type) -> dfg_type:
    """
    Get the edges to merge the islands

    Parameters
    ------------
    islands
        The list of islands
    all_edges
        The edges not in the islands

    Returns
    ------------
    The edges to merge the islands
    """
    activities = reduce(lambda x, y: x.union(y), islands, set())
    islands = islands.copy()

    edges_to_merge: dfg_type = dict()
    for (a, b), value in all_edges.items():
        if len(islands) == 1:
            break

        if a not in activities or b not in activities:
            continue

        island_1, island_2 = search_for_island(a, b, islands)
        if island_1 is island_2:
            continue

        if island_1 and island_2:
            edges_to_merge[(a, b)] = value
            island_1.update(island_2)
            islands.remove(island_2)

    return edges_to_merge

def filter_by_larger_island(dfg: dfg_type) -> dfg_type:
    """
    Group the dfg edges and return the largest group.

    Args:
        dfg (dfg_type): Directly-Follows Graph

    Returns:
        dfg_type: Filtered Directly-Follows Graph
    """
    islands = list()
    for edge in dfg:
        islands = add_edge_to_islands(islands, edge)
    islands_to_remove = sorted(islands, reverse=True,
                                key=lambda x: len(x))[1:]
    activities_to_remove = reduce(lambda x, y: x.union(y),
                                  islands_to_remove, set())

    return { (act1, act2): value for (act1, act2), value in dfg.items()
             if act1 not in activities_to_remove
             and act2 not in activities_to_remove}
