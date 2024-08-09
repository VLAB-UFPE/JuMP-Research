from collections import deque
from ..types import dfg_type

def get_dfg_backbone(dfg: dfg_type) -> list[tuple[str, str]]:
    """
    Get the main trace from the directly-follows graph, where
    the main trace is the largest path in the graph.

    Parameters
    ----------------
    dfg
        Directly-follows graph of tuples (source, target) and info
    
    Returns
    ----------------
    list[str]
        The main trace
    """
    next_activities: dict[str, list[str]] = dict()

    for (act, next_act), _ in dfg.items():
        if act not in next_activities:
            next_activities[act] = list()
        if next_act not in next_activities:
            next_activities[next_act] = list()
        next_activities[act].append(next_act)

    largest_trace: list[str] = []
    queue = deque([(initial_act, [initial_act]) for initial_act in next_activities.keys()])

    while queue:
        current_act, trace = queue.popleft()

        if len(trace) > len(largest_trace):
            largest_trace = trace

        for next_act in next_activities[current_act]:
            if next_act not in trace:
                queue.append((next_act, trace + [next_act]))

    return largest_trace