from ...types import dfg_type
import math, re, colorsys

MAX_EDGE_PEN_WIDTH_GRAPHVIZ = 5.0
MIN_EDGE_PEN_WIDTH_GRAPHVIZ = 0.0
EDGE_COLORS = [
    "#004733", "#004733", "#006B55", "#227F67", #Green scale, fast colors
    "#E1B364", "#DBAD49", "#D4A92F",            #Yellow scale, median colors
    "#C4383F", "#BA1B1B", "#9D0B0E", "#7A0106"  #Red scale, slow colors
]

def get_min_max_value(dfg: dfg_type) -> tuple[int, int]:
    """
    Gets min and max value assigned to edges
    in DFG graph

    Parameters
    -----------
    dfg
        Directly follows graph

    Returns
    -----------
    min_value
        Minimum value in directly follows graph
    max_value
        Maximum value in directly follows graph
    """
    min_value = 9999999999
    max_value = -1

    for edge in dfg:
        if dfg[edge] < min_value:
            min_value = dfg[edge]
        if dfg[edge] > max_value:
            max_value = dfg[edge]

    return min_value, max_value

def get_arc_pen_width(arc_measure: float, min_arc_measure: float,
                     max_arc_measure: float) -> float:
    """
    Calculate arc width given the current arc measure value, the minimum arc measure value and the
    maximum arc measure value

    Parameters
    -----------
    arc_measure
        Current arc measure value
    min_arc_measure
        Minimum measure value among all arcs
    max_arc_measure
        Maximum measure value among all arcs

    Returns
    -----------
    pen_width
        Current arc width in the graph
    """
    return (MIN_EDGE_PEN_WIDTH_GRAPHVIZ
            + (MAX_EDGE_PEN_WIDTH_GRAPHVIZ - MIN_EDGE_PEN_WIDTH_GRAPHVIZ)
            * (arc_measure - min_arc_measure)
            / (max_arc_measure - min_arc_measure + 0.00001))

def assign_pen_width_edges(freq_dfg: dfg_type) -> dict[tuple[str, str], float]:
    """
    Assign pen_width to edges in directly-follows graph

    Parameters
    -----------
    dfg
        directly follows graph

    Returns
    -----------
    pen_width
        Graph pen_width that edges should have in the directly follows graph
    """
    pen_width = {}
    min_value, max_value = get_min_max_value(freq_dfg)
    for edge in freq_dfg:
        v0 = freq_dfg[edge]
        v1 = get_arc_pen_width(v0, min_value, max_value)
        pen_width[edge] = v1

    return pen_width

def get_activities_color_soj_time(soj_time: dfg_type, new_colors: list = []
                                  ) -> dict[str, tuple[str, str]]:
    """
    Gets the color for the activities based on the sojourn time

    Parameters
    ----------------
    soj_time
        Sojourn time
    new_colors
        A list of colors to be choose

    Returns
    ----------------
    act_color
        Dictionary associating each activity to a color based on the sojourn time
    """
    activities_color = {}

    min_soj_time, max_soj_time = get_min_max_value(soj_time)
    font_color = "black"
    for ac in soj_time:
        act_soj_time = soj_time[ac]
        relative = ((act_soj_time - min_soj_time) /
            (max_soj_time - min_soj_time + 0.00001))

        if len(new_colors) > 0:
            index = int(relative * len(new_colors))
            if index == 0 and act_soj_time > 0: index = 1
            bg_color = new_colors[index % len(new_colors)]
            is_black = relative < 0.5 or bg_color in ["#FFFFFF", "white"]
            font_color = "black" if is_black else "white"
        else:
            trans_base_color = int(255 - 100 * relative)
            trans_base_color_hex = str(hex(trans_base_color))[2:].upper()
            bg_color = f"#FF{trans_base_color_hex}{trans_base_color_hex}"
        activities_color[ac] = (font_color, bg_color)

    return activities_color

def get_list_item_by_normalization_range(
    normalized_value: float, # a float between, and including, 0 and 1
    list: list = EDGE_COLORS, 
):
    list_len: float = len(list)
    index = math.floor(normalized_value * list_len) - 1
    if index < 0: 
        index = 0
    if index >= list_len: 
        index = list_len - 1
    return list[index]

def get_edges_color_soj_time(soj_time: dfg_type):
    """
    Gets the color for the activities based on the sojourn time

    Parameters
    ----------------
    soj_time
        Sojourn time

    Returns
    ----------------
    act_color
        Dictionary associating each activity to a color based on the sojourn time
    """
    activities_color = {}

    if len(soj_time.values()) == 0:
        return activities_color

    min_soj_time, max_soj_time = get_min_max_value(soj_time)
    for ac in soj_time:
        act_soj_time = soj_time[ac]

        relative_difference = act_soj_time - min_soj_time
        absolute_difference = max_soj_time - min_soj_time
        if absolute_difference == 0: absolute_difference = 1

        difference_ratio = math.log10(
            (relative_difference / absolute_difference) * 9 + 1
        )
        
        activities_color[ac] = get_list_item_by_normalization_range(
            difference_ratio, EDGE_COLORS)

    return activities_color

def hsl_to_hex(string: str) -> str:
    regex = r'hsl\(\s*(\d+),\s*(\d+)%,\s*(\d+)%\s*\)'
    line = re.findall(regex, string)[0]
    rgb = colorsys.hsv_to_rgb(int(line[0]) / 360,
                              int(line[1]) / 100,
                              int(line[2]) / 100)
    return "#" + "".join("%02X" % round(i * 255) for i in rgb)

def human_readable_stat(c: float) -> str:
    """
    Transform a timedelta expressed in seconds into a human readable string

    Parameters
    ----------
    c
        Timedelta expressed in seconds

    Returns
    ----------
    string
        Human readable string
    """
    c = int(float(c))
    years = c // 31104000
    months = c // 2592000
    days = c // 86400
    hours = c // 3600 % 24
    minutes = c // 60 % 60
    seconds = c % 60

    if years > 0:
        year_str = f"{str(years)} anos"
        if years == 1:
            return year_str[:-1] 
        return year_str
    
    if months > 0:
        month_str = str(months)
        if months ==  1:
            return f"{month_str} mês"
        return f"{month_str} meses"
    
    if days > 0:
        day_str = f"{str(days)} dias"
        if days == 1:
            return day_str[:-1]
        return day_str
    
    if hours > 0:
        hour_str = f"{str(hours)} horas"
        if hours == 1:
            return hour_str[:-1]
            
        return hour_str
    
    if minutes > 0:
        minute_str = f"{str(minutes)} minutos"
        if minutes == 1:
            return minute_str[:-1] 
        return minute_str
    
    second_str = f"{seconds} segundos"
    return second_str[:-1] if seconds == 1 else second_str

def break_lines(text: str, max_len: int = 25,
                join_char: str = '\n') -> str:
    """
    Breaks a text into lines of maximum length

    Parameters
    ----------------
    text
        Text to be broken
    max_len
        Maximum length of each line

    Returns
    ----------------
    lines
        List of lines
    """
    lines = []
    line = ""
    for word in text.split():
        if len(line) + len(word) > max_len:
            lines.append(line)
            line = ""
        line += word + " "
    lines.append(line)
    return join_char.join(lines)

def treat_dfg_by_another(dfg: dfg_type, reference_dfg: dfg_type) -> dfg_type:
    """
    Get only the edges that are in the reference DFG from the DFG to be treated

    Parameters
    ----------------
    dfg
        DFG to be treated
    reference_dfg
        The reference DFG

    Returns
    ----------------
    cleaned_dfg
        Cleaned DFG
    """
    return { edge: dfg.get(edge, 0) for edge in reference_dfg }

def get_activities_from_dfg(dfg: dfg_type) -> set[str]:
    """
    Get the activities from the DFG

    Parameters
    ----------
    dfg
        Directly follows graph

    Returns
    ----------
    activities
        Set of activities
    """
    activities = set()
    for edge in dfg:
        activities.add(edge[0])
        activities.add(edge[1])
    return activities
