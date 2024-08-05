from pm4py.objects.conversion.dfg.variants.to_petri_net_invisibles_no_duplicates import Parameters
from pm4py import (fitness_token_based_replay, precision_token_based_replay,
                   generalization_tbr)
from .constants import ACTIVITY_NAME, CASE_CONCEPT_NAME, TIMESTAMP_NAME
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.conversion.dfg import converter
from .eventlog import format_df_to_eventlog
from .types import dfg_type
import pandas as pd
import polars as pl
import random

CONFORMANCE_COLUMNS = [
    'tbr_percentage_of_fitting_traces', 
    'tbr_log_fitness', 
    'tbr_average_trace_fitness',
    'tbr_precision',
    'tbr_generalization',
    'f1_score'
]

def filtering_df(dataframe: pd.DataFrame, cases_amount: int=100,
                 min_act_amount: int=10, random_state: int=42) -> pd.DataFrame:
    """
    Seleciona um número k de casos de um dataframe, de forma aleatoriamente
    determinística, que tenham mais de 10 atividades.

    Args:
        df: DataFrame com os dados dos casos.
        cases_amount: Número de casos a serem selecionados.
        min_act_amount: Número mínimo de atividades que um caso deve ter.
        random_state: Semente para a geração de números aleatórios.
    
    Returns:
        DataFrame com os casos selecionados.
    """
    random.seed(random_state)
    case_counts = dataframe['Case'].value_counts()
    filtered_ids = case_counts[case_counts > min_act_amount].index
    chosen_ids = random.choices(filtered_ids, k=cases_amount)
    return dataframe[dataframe['Case'].isin(chosen_ids)]

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

def get_start_end_activities(
    dataframe: pl.DataFrame,
    activity_key: str = ACTIVITY_NAME,
    timestamp_key: str = TIMESTAMP_NAME
) -> tuple[list[str], list[str]]:
    """
    Obtém as atividades de início e fim de cada caso em um dataframe.

    Args:
        dataframe: DataFrame com os dados dos casos.
        activity_key: Nome da coluna com as atividades.
        timestamp_key: Nome da coluna com os timestamps.
    
    Returns:
        Tupla com duas listas, a primeira com as atividades de
        início e a segunda com as atividades de fim.
    """
    start_end_cols = (dataframe.lazy().sort(timestamp_key)
        .select([
            activity_key, CASE_CONCEPT_NAME
        ]).group_by(CASE_CONCEPT_NAME).agg(
            pl.col(activity_key).first().alias("start_activity"),
            pl.col(activity_key).last().alias("end_activity")
        ).select("start_activity", "end_activity")
        .collect())

    start_activities = (start_end_cols.get_column("start_activity")
                                      .value_counts().to_dicts())
    start_activities = {x["start_activity"]: x["count"]
                        for x in start_activities}
    end_activities = (start_end_cols.get_column("end_activity")
                                    .value_counts().to_dicts())
    end_activities = {x["end_activity"]: x["count"]
                      for x in end_activities}
    return start_activities, end_activities

def get_dataframe(file_path: str) -> pl.DataFrame:
    """
    Lê um arquivo CSV e o converte para um DataFrame.

    Args:
        file_path: Caminho para o arquivo CSV.
    
    Returns:
        DataFrame com os dados do arquivo.
    """
    
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S%.f"
    dataframe = (pl.read_csv(file_path)
                   .with_columns(pl.col(["Start", "End"])
                   .str.strptime(pl.Datetime, format=DATE_FORMAT)))
    return format_df_to_eventlog(dataframe, case_id='Case',
                                 start_timestamp_key="Start",
                                 activity_key='activity',
                                 timestamp_key='End')

def show_conformance(polars_df: pl.DataFrame, dfg: dfg_type,
                     start_activities: list[str], end_activities: list[str]
                     ) -> None:
    """
    Transform a dataframe into a petri net and show the conformance metrics.
    """
    variant = converter.Variants.VERSION_TO_PETRI_NET_INVISIBLES_NO_DUPLICATES

    dfg_pn, dfg_im, dfg_fm = converter.apply(dfg, variant=variant, parameters={
        Parameters.START_ACTIVITIES: start_activities,
        Parameters.END_ACTIVITIES: end_activities
    })

    pandas_df = polars_df.to_pandas()
    return get_conformance_stats(pandas_df, dfg_pn, dfg_im, dfg_fm)
