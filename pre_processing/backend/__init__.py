from .constants import *
from .types import dfg_type
from .charts import show_activities
from .dfg_discovery import frequency_dfg, partition_dataframe_into_dfgs
from .conformance import (get_conformance_stats, get_start_end_activities,
                          get_dataframe, filtering_df, show_conformance)
from .specialization import (remove_events, add_activity_id, 
                             add_activity_suffix, remove_suffix_if_grouped)
from .conversor import convert_pn_to_dfg
from .clustering import group_dataframe

from functools import reduce
import polars as pl
import pm4py

def get_activities_from_dfg(dfg):
    return set(reduce(lambda x, y: x + y, dfg.keys(), tuple()))

def filter_dict_by_dfg(dic: dict[str, int], dfg: dfg_type):
    return {key: value for key, value in dic.items()
            if key in get_activities_from_dfg(dfg)}

def show_flowchart(filtered_df: pl.DataFrame, ea_dfg: dfg_type,
                   sa_dfg: dfg_type, count: int=30):
    jump_dfg = frequency_dfg(filtered_df, count)
    filtered_ea_dfg = filter_dict_by_dfg(ea_dfg, jump_dfg)
    filtered_sa_dfg = filter_dict_by_dfg(sa_dfg, jump_dfg)
    pm4py.view_dfg(jump_dfg, filtered_sa_dfg, filtered_ea_dfg)
