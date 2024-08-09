from .types import AnimationData, default_animation_data, UnitsComparison
from ..comparison import get_comparison_dfg, get_comparison_start_end_acts
from .dfg_algorithms import employee_frequency
from .animation import animation_data_handler

from pm4py.objects.heuristics_net import defaults

from ..polars import (apply_dfg_performance, PerformanceParams, get_attribute_values)
from ..constants import ACTIVITY_NAME
from ..utils import get_start_end_activities
from .dfg_discovery import frequency_dfg
import polars as pl
from enum import Enum

class Parameters(Enum):
    START_ACTIVITIES = "start_activities"
    END_ACTIVITIES = "end_activities"
    FONT_SIZE = "font_size"
    BGCOLOR = "bgcolor"
    VARIANT = "variant"
    FORMAT = "format"
    MARKS = "marks"
    VERTICAL = "is_horizontal"
    COMPARISON = "comparison"
    OCCURRENCES = "occurrences"
    CASE_AMOUNT = "case_amount"
    RELATION_COUNT = "relation_count"
    START_END_NODES = "start_end_nodes"
    CLICKABLE_ARROWS = "clickable_arrows"

class ProcessDiscovery:
    @staticmethod
    def get_dfg_params(**kwargs):
        return {
            "weights": kwargs.get("weights", {}),
            "variant": kwargs.get("variant", "both"),
            "employee": kwargs.get("employee", None),
            "max_edges": kwargs.get("max_edges", 100),
            "animated": kwargs.get("animated", False),
            "keep_events": kwargs.get("keep_events", []),
            "file_format": kwargs.get("file_format", "svg"),
            "trim_percentage": kwargs.get("trim_percentage", 0.1),
            "remove_islands": kwargs.get("remove_islands", False),
            "min_act_count": kwargs.get("min_act_count", 2),
            "dfg_noise_thresh": kwargs.get("dfg_noise_thresh",
                defaults.DEFAULT_DFG_PRE_CLEANING_NOISE_THRESH),
            "dependency_thresh": kwargs.get("dependency_thresh",
                            defaults.DEFAULT_DEPENDENCY_THRESH),
            "min_dfg_occurrences": kwargs.get("min_dfg_occurrences", 2),
            "loops_length_two_thresh": kwargs.get("loops_length_two_thresh",
                                    defaults.DEFAULT_LOOP_LENGTH_TWO_THRESH),
            "participation_thresh": kwargs.get("participation_thresh", 2),
            "similarity_thresh": kwargs.get("similarity_thresh", 0.7),
        }
        
    @staticmethod
    def comparison_directly_follows_graph(
        dataframes: list[pl.DataFrame], **args
    ) -> tuple[str, list[UnitsComparison]]:
        params = ProcessDiscovery.get_dfg_params(**args)
        participation_thresh = params["participation_thresh"]
        similarity_thresh = params["similarity_thresh"]
        trim_percentage = params["trim_percentage"]

        freq_dfg = get_comparison_dfg(dataframes, similarity_thresh,
                                      percentage=trim_percentage,
                                      filter_count=participation_thresh)
        sa, ea = get_comparison_start_end_acts(dataframes, similarity_thresh,
                                               participation_thresh)
        activity_count: dict[str, int] = dict()
        for (a, b), count in freq_dfg.items():
            if a not in activity_count:
                activity_count[a] = 0
            if b not in activity_count:
                activity_count[b] = 0
            activity_count[a] += count
            activity_count[b] += count

        return dfg_visualizer(freq_dfg, comparison=True,
            activity_count=activity_count, parameters={
                Parameters.END_ACTIVITIES: ea,
                Parameters.START_ACTIVITIES: sa,
                Parameters.FORMAT: params["file_format"],
            }).render()

    @staticmethod
    def directly_follows_graph(event_log: pl.DataFrame, **args) -> tuple[str, AnimationData]:
        '''
        Return the svg file path where shows the directly follows graph.
        All the parameters are optional. The parameters that will modify
        significantly the graph are: employee (analysis the employee's
        events) and variant (frequency, performance or both).

        Args:
            event_log (pl.DataFrame): The event log to be analyzed.
            max_edges (int): The maximum number of edges to be shown.
            keep_events (list): The list of events to be kept.
            trim_percentage (float): The percentage of edges to be trimmed.
            file_format (str): The file format of the output file.
            employee (str, optional): The employee to be analyzed.
            variant (str, optional): The variant of the graph.

        Returns:
            str: The svg file path where shows the directly follows graph.
        '''
        params = ProcessDiscovery.get_dfg_params(**args)
        freq_dfg = frequency_dfg(event_log, params["max_edges"],
                                 params["keep_events"], 
                                 params["trim_percentage"])

        employee = params["employee"]
        perf_dfg, soj_time = freq_dfg, dict()
        is_employee_analysis = employee is not None
        if params["variant"] != "frequency" or is_employee_analysis:
            filter_key = "usuarioID" if is_employee_analysis else None
            perf_dfg = apply_dfg_performance(event_log, parameters={
                PerformanceParams.AGGREGATION_MEASURE: "median",
                PerformanceParams.ACTIVITY_KEY: ACTIVITY_NAME,
                PerformanceParams.FILTER_KEY: filter_key,
                PerformanceParams.FILTER_VALUE: employee,
            })
            if is_employee_analysis:
                soj_time = employee_frequency(event_log, employee,
                                        activity_key=ACTIVITY_NAME)

        start_acts, end_acts = get_start_end_activities(event_log)

        animation_data = default_animation_data
        activity_count = get_attribute_values(event_log, ACTIVITY_NAME)
        if params["animated"]:
            animation_data = animation_data_handler(event_log, freq_dfg,
                                                    perf_dfg, activity_count)

        return dfg_visualizer(freq_dfg, perf_dfg, activity_count, soj_time,
            False, False, is_employee_analysis, parameters={
                Parameters.END_ACTIVITIES: end_acts,
                Parameters.VARIANT: params["variant"],
                Parameters.START_ACTIVITIES: start_acts,
                Parameters.FORMAT: params["file_format"],
            }).render(), animation_data