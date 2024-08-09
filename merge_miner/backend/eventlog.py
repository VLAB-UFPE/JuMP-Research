from .constants import (CASE_CONCEPT_NAME, ACTIVITY_NAME, ORIGINAL_ACTIVITY,
				   		START_TIMESTAMP_NAME, ORIGINAL_ACTIVITY_NAME,
				   		TIMESTAMP_NAME, ACTIVITY_ID_NAME)
from .utils import format_dataframe
import polars as pl

CASE_KEY = "NPU"
ACTIVITY_KEY = "activity"
ST_KEY = "dataInicio"
ET_KEY = "dataFinal"

def get_dataframe(file_path: str) -> pl.DataFrame:
    """
    Lê um arquivo CSV e o converte para um DataFrame.

    Args:
        file_path: Caminho para o arquivo CSV.
    
    Returns:
        DataFrame com os dados do arquivo.
    """
    DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%.3f"
    dataframe = (pl.read_csv(file_path)
                   .with_columns(pl.col([ST_KEY, ET_KEY])
                   .str.strptime(pl.Datetime, format=DATE_FORMAT)))
    return format_df_to_eventlog(dataframe, case_id=CASE_KEY,
                                 start_timestamp_key=ST_KEY,
                                 activity_key=ACTIVITY_KEY,
                                 timestamp_key=ET_KEY)

def format_df_to_eventlog(dataframe: pl.DataFrame, columns:list = [],
						  **kwargs) -> pl.DataFrame:
	"""
	Format the dataframe to be a valid eventlog.

	Args:
		dataframe (pl.DataFrame): The eventlog to be formatted.
		columns (list, optional): The columns to be kept. Defaults to [].
		**kwargs: The columns names to be used.
	
	Returns:
		pl.DataFrame: The formatted eventlog dataframe.
	"""
	case_id = kwargs.get("case_id", CASE_CONCEPT_NAME)
	activity_key = kwargs.get("activity_key", ACTIVITY_NAME)
	timestamp_key = kwargs.get("timestamp_key", TIMESTAMP_NAME)
	start_timestamp_key = kwargs.get("start_timestamp_key",
										START_TIMESTAMP_NAME)

	if columns != []: dataframe = dataframe[columns]

	dataframe = dataframe.with_columns([
		pl.col(activity_key).alias(ORIGINAL_ACTIVITY_NAME),
		pl.col(ACTIVITY_ID_NAME).alias(ORIGINAL_ACTIVITY),
		pl.col(activity_key).cast(pl.Categorical),
		pl.col(case_id).cast(pl.Categorical),
	])

	return format_dataframe(dataframe, case_id, activity_key,
							timestamp_key, start_timestamp_key)
