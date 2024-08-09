from .discover_utils import (get_activities_from_dfg,  break_lines,
                             get_activities_color_soj_time)
from .graphviz_utils import (Digraph, classify_activities_by_included_or_not,
                             get_treated_graphviz, create_not_exist_node,
                             add_start_end_nodes)
from .constants import (TOP_ACTIVITIES, BOTTOM_ACTIVITIES,
                        DF_COLOR_ACT, GREEN_COLORS)
from ...types import dfg_type


def create_nodes(graph: Digraph, activity_list: list[str],
                 activity_colors: dict[str, tuple[str, str]],
                 activity_map: dict[str, str]) -> dict[str, str]:
    """
    Add the nodes to the specified graph, based on the activity list.

    Parameters
    ----------------
    graph
        Graph where the nodes will be added
    activity_list
        List of activities to be added
    activity_colors
        Dict associating to each activity the font and background color
    activity_map
        Dict associating to each activity the node id
    
    Returns
    ----------------
    activity_map
        The modified dict associating to each activity the node id
    """
    for act in activity_list:
        label = f" {break_lines(act)} "
        font_color, bg_color = activity_colors.get(act, DF_COLOR_ACT)

        node_id = "n" + str(hash(act))
        graph.node(node_id, label, id=node_id, tooltip=act,
                   fontcolor=font_color, fillcolor=bg_color,
                   style="filled,rounded", margin="0.1")
        activity_map[act] = node_id
    return activity_map

def create_edges(viz: Digraph, edges_infos: dfg_type):
    """
    Add new edges to the specified graph.

    Parameters
    ----------------
    viz
        Graph where the edges will be added
    edges_infos
        Directly-follows graph of tuples (source, target) and info
    """
    for edge in edges_infos.keys():
        tooltip = f"{edge[0]} -> {edge[1]}"
        act1 = "n" + str(hash(edge[0]))
        act2 = "n" + str(hash(edge[1]))
        edge_id = act1 + act2
        viz.edge(act1, act2, id=edge_id, tooltip=tooltip,
                 labeltooltip=tooltip)

def compare_visualization(comparison_dfg: dfg_type,
                          activity_count: dict[str, int],
                          start_activities: list[str],
                          end_activities: list[str], **args):
    viz = get_treated_graphviz(**args)
    activity_mapping: dict[str, str] = dict()

    activity_list = get_activities_from_dfg(comparison_dfg)
    activity_count = { act: activity_count.get(act, 0)
                        for act in activity_list }
    activity_colors = get_activities_color_soj_time(activity_count,
                                                    GREEN_COLORS)

    top_act_to_include, top_act_not_included, activity_list = \
        classify_activities_by_included_or_not(activity_list, TOP_ACTIVITIES)
    bot_act_to_include, bot_act_not_included, activity_list = \
        classify_activities_by_included_or_not(activity_list,
                                               BOTTOM_ACTIVITIES)

    top_graph = get_treated_graphviz(is_subgraph=True, rank="source")
    activity_mapping = create_nodes(top_graph, top_act_to_include,
                                    activity_colors, activity_mapping)
    activity_mapping = create_not_exist_node(top_graph,
                top_act_not_included, activity_mapping)
        
    bottom_graph = get_treated_graphviz(is_subgraph=True, rank="sink")
    activity_mapping = create_nodes(bottom_graph, bot_act_to_include, 
                                    activity_colors, activity_mapping)
    activity_mapping = create_not_exist_node(bottom_graph,
                   bot_act_not_included, activity_mapping)

    activity_mapping = create_nodes(viz, activity_list, activity_colors,
                                    activity_mapping)
    create_edges(viz, comparison_dfg)
    add_start_end_nodes(viz, top_graph, bottom_graph, start_activities,
                        end_activities, activity_mapping)

    return viz
