from .constants import ACTIVITY_NAME
import matplotlib.pyplot as plt
import polars as pl
import numpy as np

def show_activities(df: pl.DataFrame, number_of_activities: int=35):
    act_dict = df[ACTIVITY_NAME].value_counts().sort(
        'count', descending=True).to_dict()
    activities_len = len(act_dict[ACTIVITY_NAME])

    labels = act_dict[ACTIVITY_NAME][:number_of_activities]
    frequencies = act_dict["count"][:number_of_activities]

    plt.figure(figsize=(20, 8))

    y_pos = np.arange(len(labels))
    plt.bar(y_pos, frequencies)
    plt.xticks(y_pos, labels, rotation=90)

    xlocs, _ = plt.xticks()
    for i, v in enumerate(frequencies):
        plt.text(xlocs[i] - 0.50, v + 0.02, str(v))

    plt.xlabel(f'Showing {len(labels)}/{activities_len} activities')

    plt.show()
