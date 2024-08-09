import matplotlib.pyplot as plt
from ..constants import ACTIVITY_NAME
from ..types import dfg_type
from functools import partial
import seaborn as sns
import polars as pl
import numpy as np

def show_activities(dfs: pl.DataFrame, number_of_activities:int=10):
    df_list = list()
    for i, df in enumerate(dfs):
        df_list.append(df.with_columns(pl.lit(f"unidade_{i+1}").alias("Group")))
    concatenated_df = pl.concat(df_list)
    
    act_counts: pl.DataFrame = (concatenated_df
        .groupby(["Group", ACTIVITY_NAME])
        .agg(pl.count())
        .sort("count", descending=True))

    top_activities = (act_counts.select(ACTIVITY_NAME)
                      .unique(maintain_order=True)
                      .limit(number_of_activities))
    act_counts = act_counts.filter(
        pl.col(ACTIVITY_NAME).is_in(top_activities[ACTIVITY_NAME])
    )

    activities = top_activities.to_series().to_list()
    groups = act_counts["Group"].unique().sort().to_list()
    n_groups = len(groups)

    _, ax = plt.subplots(figsize=(20, 8))
    index = np.arange(len(activities))
    bar_width = 0.85 / n_groups

    for group, data in act_counts.group_by(pl.col("Group")):
        for activity in activities:
            if activity not in data[ACTIVITY_NAME]:
                act_counts = pl.concat([act_counts, pl.DataFrame({
                    "Group": group,
                    ACTIVITY_NAME: activity,
                    "count": 0,
                }).with_columns(pl.col("count").cast(pl.UInt32))])

    for i, group in enumerate(groups):
        group_data = act_counts.filter(pl.col("Group") == group)
        group_data = (group_data.select(ACTIVITY_NAME, "count")
                                .sort("count", descending=True))
        
        values = group_data["count"].to_numpy()
        bars = ax.bar(index + i * bar_width, values, bar_width, label=group)

        text_positions = [(bar.get_x() + bar.get_width() / 2) for bar in bars]
        for pos, value in zip(text_positions, values):
            ax.text(pos, value, str(value), ha="center", va="bottom", rotation=90)

    plt.xlabel('Activity')
    plt.ylabel('Counts')
    plt.title('Activity counts by unidade')
    plt.xticks(index + bar_width / n_groups, activities, rotation=90)
    plt.legend()
    plt.show()

def plot_sns_table(act_counts: pl.DataFrame, index_col: str=ACTIVITY_NAME,
                   value_col: str="count", column_col: str="Group"):
    pivoted = act_counts.to_pandas().pivot(
        index=index_col, columns=column_col, values=value_col
    )

    plt.figure(figsize=(15, 10))  # Define o tamanho da figura
    sns.heatmap(pivoted, cmap='coolwarm', annot=True, fmt=".2f")
    plt.show()

def filter_df_table_by_count(dataframe: pl.DataFrame, max_amount: int=25,
                             act_col: str=ACTIVITY_NAME, value_col: str="count"):
    top_activities = (dataframe.select(act_col)
                      .unique(maintain_order=True)
                      .limit(max_amount))
    return dataframe.filter(
        pl.col(act_col).is_in(top_activities[act_col])
    ).sort([act_col, value_col], descending=[False, True])

def concatenate_group_dfs(dfs: list):
    df_list = list()
    for i, df in enumerate(dfs):
        df_list.append(df.with_columns(pl.lit(f"unidade_{i+1}").alias("Group")))
    return pl.concat(df_list)

def show_dfgs_tb(dfgs: dict[tuple[str, str], float], number_of_activities: int=25):
  dfgs_dfs = list()
  for unidade in dfgs:
    activity_array = map(str, unidade.keys())
    value_array = map(partial(round, ndigits=2), unidade.values())
    unidade_df = pl.DataFrame({
      "relation": activity_array,
      "value": value_array
    })
    dfgs_dfs.append(unidade_df)

  concatenated_df = concatenate_group_dfs(dfgs_dfs)
  act_counts = filter_df_table_by_count(concatenated_df, number_of_activities,
                                        "relation", "value")
  plot_sns_table(act_counts, "relation", "value")


def show_dfg_table(dfg: dfg_type, number_of_relations: int=55):
    items = sorted(dfg.items(), key=lambda x: x[1],
                   reverse=True)[:number_of_relations]
    a = [x[0][0] for x in items]
    b = [x[0][1] for x in items]
    value = [x[1] for x in items]

    pd = pl.DataFrame({
        "a": a,
        "b": b,
        "value": value
    })

    plot_sns_table(pd, "a", "value", "b")