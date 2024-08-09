from ..constants import ACTIVITY_ID_NAME, ACTIVITY_NAME
import polars as pl


def filter_by_especialista(dataframes):
    remove_list = [
    "Expedição de documento", "Publicação", "Petição",
    "Decurso de Prazo", "Mero expediente", "Conclusão",
    "Disponibilização no Diário da Justiça Eletrônico",
    ]
    for i, df in enumerate(dataframes):
        dataframes[i] = df.filter(~pl.col(ACTIVITY_NAME).is_in(remove_list))
    return dataframes

def add_activity_id(df: pl.DataFrame):
  """
  Add an activity id to the dataframe

  Parameters:
  df (pl.DataFrame): The dataframe to add the activity id to

  Returns:
  pl.DataFrame: The dataframe with the activity id added
  """
  return df.with_columns(pl.concat_str([
      pl.col(ACTIVITY_ID_NAME).map_elements(
        lambda s: f"[{s}]", return_dtype=pl.Utf8
      ),
      pl.col(ACTIVITY_NAME)
    ], separator=" ").alias(ACTIVITY_NAME))

def add_activity_sufix(df: pl.DataFrame, sufix_field: str):
    """
    Transform each activity name into:
    activity_name + ' ' + sufix_field
    """
    return df.with_columns(
        pl.when(pl.col(sufix_field) != 'NULL')
          .then(pl.concat_str([
                pl.col(ACTIVITY_NAME),
                pl.col(sufix_field).map_elements(lambda s: f"[{s}]",
                                                 return_dtype=pl.Utf8)
          ], separator=" ").alias(ACTIVITY_NAME))
          .otherwise(pl.col(ACTIVITY_NAME))
          .alias(ACTIVITY_NAME)
    )