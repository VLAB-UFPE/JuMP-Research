from .constants import GRAY_COLOR, DARK_GRAY_COLOR, START_ID, END_ID
from .discover_utils import break_lines
from tempfile import NamedTemporaryFile
from graphviz import Digraph

def get_treated_graphviz(bg_color: str="transparent", font_size: str="11",
                         image_format: str="svg", is_horizontal: bool=False,
                         is_subgraph: bool=False, rank: str="same", **args):
    """
    Returns a Digraph object with the specified background color and
    image format for the visualization. The graphviz object is created
    with the specified arguments.

    Parameters
    ----------------
    bg_color
        Background color of the graph
    font_size
        Font size of the graph
    image_format
        Image format of the graph
    is_horizontal
        If the graph is horizontal
    is_subgraph
        If the graph is a subgraph
    rank
        Position of the nodes in the graph
    args
        Arguments to be ignored in the **args
    
    Returns
    ----------------
    viz
        Digraph object
    """
    viz = Digraph(format=image_format)
    if not is_subgraph:
        filename = NamedTemporaryFile(suffix='.gv')
        viz = Digraph("", filename=filename.name, engine='dot',
                      graph_attr = { 'bgcolor': bg_color },
                      format=image_format)

    direction = "LR" if is_horizontal else "TB"

    viz.attr('node', shape='box')
    viz.attr(fontsize=font_size)
    viz.attr(fontname='Roboto')
    viz.attr(rankdir=direction)
    viz.attr(overlap='false')
    viz.attr(rank=rank)

    return viz

def classify_activities_by_included_or_not(
    activity_list: list[str], activities_to_check: list[str]
) -> tuple[list[str], list[str]]:
    """
    Classifies the activities into two categories: included and not included.
    The activities to be checked are compared with the list of all activities.
    If the activity is already included in the list, it is removed from the
    list of activities and added to the list of activities to include. The
    remaining activities are added to the list of activities not included.

    Parameters
    ----------------
    activity_list
        List of all activities
    activities_to_check
        List of activities to be checked
    
    Returns
    ----------------
    activities_to_include
        List of activities that are included in the list
    activities_not_included
        List of activities that are not included in the list
    activity_list
        The list of activities after removing the activities to include
    """
    activities_to_include = []
    activities_not_included = []

    for act in activities_to_check:
        if act in activity_list:
            activities_to_include.append(act)
            activity_list.remove(act)
        else:
            activities_not_included.append(act)

    return activities_to_include, activities_not_included, activity_list

def add_start_end_nodes(viz: Digraph, top_graph: Digraph, bottom_graph: Digraph,
                        start_activities: list[str], end_activities: list[str],
                        activity_mapping: dict[str, str]):
    """
    Add the start and end activities to the graph. The start activities
    are added at the top_graph and the end activities are added at the
    bottom_graph. The start activities are connected to the start node.
    The end activities are connected to the end node if they are not
    appearing in the start activities.

    Parameters
    ----------------
    viz
        Graphviz Digraph object
    top_graph
        Graphviz Digraph object for the top activities
    bottom_graph
        Graphviz Digraph object for the bottom activities
    start_activities
        List of start activities
    end_activities
        List of end activities
    activity_mapping
        Dict associating to each activity the node id
    """
    start_activities_to_include = [act for act in start_activities
                                   if act in activity_mapping]
    end_activities_to_include = [act for act in end_activities
                                 if act in activity_mapping and
                                 act not in start_activities_to_include]
    if start_activities_to_include:
        top_graph.node(START_ID, "Início", shape='circle', fontsize="15",
                       penwidth="0", fillcolor="#E0E3E3", style="filled")
        for act in start_activities_to_include:
            viz.edge(START_ID, activity_mapping[act], label="", style="dashed",
                     penwidth="1.8", color=DARK_GRAY_COLOR)
    if end_activities_to_include:
        bottom_graph.node(END_ID, "Último\nmovimento", shape='circle',
                          fontsize="13", penwidth="0", fillcolor="#E0E3E3",
                          style="filled", fixedsize="true", width="1.2")
        for act in end_activities_to_include:
            viz.edge(activity_mapping[act], END_ID, label="", style="dashed",
                     penwidth="1.8", color=DARK_GRAY_COLOR)

    viz.subgraph(top_graph)
    viz.subgraph(bottom_graph)

def create_not_exist_node(graph: Digraph, activities_to_include: list[str],
                          activities_map: dict[str, str]) -> dict[str, str]:
    """
    Add the activities that are not executed to the graph. The activities
    are added with a dashed style and gray color. The activities are added
    to the activities map with the node id.

    Parameters
    ----------------
    graph
        Graphviz Digraph object
    activities_to_include
        List of activities that are not executed
    activities_map
        Dict associating to each activity the node id
    
    Returns
    ----------------
    activities_map
        Dict associating to each activity the node id
    """
    for act in activities_to_include:
        act_label = f" {break_lines(act)} \n"
        label = f"{act_label} não executou"
        node_id = "n" + str(hash(act))
        graph.node(node_id, label, id=node_id, style="rounded,dashed",
                   fontcolor=GRAY_COLOR, color=GRAY_COLOR)
        activities_map[act] = node_id
    return activities_map
