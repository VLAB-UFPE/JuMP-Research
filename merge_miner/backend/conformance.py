from pm4py import (fitness_token_based_replay, precision_token_based_replay,
                   generalization_tbr)
from pm4py.objects.conversion.dfg import converter, variants
from pm4py.objects.petri_net.obj import PetriNet, Marking
from .types import dfg_type
import pandas as pd

CONFORMANCE_COLUMNS = [
    'tbr_percentage_of_fitting_traces', 
    'tbr_log_fitness', 
    'tbr_average_trace_fitness',
    'tbr_precision',
    'tbr_generalization',
    'f1_score'
]

def get_conformance_stats(dataframe: pd.DataFrame, petri_net: PetriNet, 
                          initial_marking: Marking, final_marking: Marking
                          ) -> pd.Series:
    """
    Calcula as métricas de conformidade de um modelo e sua rede de Petri.

    Args:
        dataframe: DataFrame com os dados dos casos.
        petri_net: Modelo de rede de Petri.
        initial_marking: Marcação inicial do modelo.
        final_marking: Marcação final do modelo.

    Returns:
        Lista com as métricas de conformidade do modelo.
    """
    model_tbr_fitness = fitness_token_based_replay(dataframe, petri_net, 
                                         initial_marking, final_marking)
    model_tbr_precision = precision_token_based_replay(dataframe, petri_net, 
                                             initial_marking, final_marking)
    model_tbr_generalization = generalization_tbr(dataframe, petri_net, 
                                        initial_marking, final_marking)
    
    avg_trace_fitness = model_tbr_fitness['average_trace_fitness']
    precision_times_fitness = model_tbr_precision * avg_trace_fitness
    precision_plus_fitness = model_tbr_precision + avg_trace_fitness
    f1_score = (2 *  precision_times_fitness) / precision_plus_fitness

    return pd.Series([
        model_tbr_fitness['percentage_of_fitting_traces'],
        model_tbr_fitness['log_fitness'],
        model_tbr_fitness['average_trace_fitness'],
        model_tbr_precision,
        model_tbr_generalization,
        f1_score
    ], index=CONFORMANCE_COLUMNS)

def transform_dfg_to_pn(dfg: dfg_type, start_activities: list[str],
                        end_activities: list[str]
                        ) -> tuple[PetriNet, Marking, Marking]:
    """
    Transforms the Directly-Follows Graph into a Petri Net.
    """
    parameters = variants.to_petri_net_invisibles_no_duplicates.Parameters
    variant = converter.Variants.VERSION_TO_PETRI_NET_INVISIBLES_NO_DUPLICATES

    return converter.apply(dfg, variant=variant, parameters={
        parameters.START_ACTIVITIES: start_activities,
        parameters.END_ACTIVITIES: end_activities
    })
