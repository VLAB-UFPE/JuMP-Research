from .constants import ACTIVITY_NAME, ACTIVITY_ID_NAME, ORIGINAL_ACTIVITY
import polars as pl

def remove_events(df: pl.DataFrame, remove_list: list):
  """
  Remove events from the dataframe

  Parameters:
  df (pl.DataFrame): The dataframe to remove events from
  remove_list (list): The list of events to remove

  Returns:
  pl.DataFrame: The dataframe with the events removed
  """
  return df.filter(~pl.col(ACTIVITY_NAME).is_in(remove_list))

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

def add_activity_suffix(df: pl.DataFrame, suffix_field: str):
    """
    Transform each activity name into:
    activity_name + ' ' + suffix_field
    """
    return df.with_columns(
        pl.when(pl.col(suffix_field) != 'NULL')
          .then(pl.concat_str([
                pl.col(ACTIVITY_NAME),
                pl.col(suffix_field).map_elements(lambda s: f"[{s}]",
                                                 return_dtype=pl.Utf8)
          ], separator=" ").alias(ACTIVITY_NAME))
          .otherwise(pl.col(ACTIVITY_NAME))
          .alias(ACTIVITY_NAME)
    )

def remove_suffix_if_grouped(df: pl.DataFrame, suffix_field: str):
	"""
	Remove the suffix field value if the activity is grouped, i.e.,
	the ACTIVITY_ID_NAME is different from original_ACTIVITY_ID_NAME
	"""
	return df.with_columns(
        pl.when(pl.col(ACTIVITY_ID_NAME).ne(pl.col(ORIGINAL_ACTIVITY)))
        .then(pl.lit("NULL"))
		.otherwise(pl.col(suffix_field))
		.alias(suffix_field)
    )