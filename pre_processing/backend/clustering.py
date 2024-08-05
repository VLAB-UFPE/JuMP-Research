from backend.constants import (ORIGINAL_ACTIVITY_NAME, ACTIVITY_NAME,
                               ORIGINAL_ACTIVITY, ACTIVITY_ID_NAME)
from typing import NamedTuple
import polars as pl
import json

class ActivityParentInfo(NamedTuple):
    parent_id: int | None
    parent_name: str | None

class ActivityInfo(NamedTuple):
    activity_id: int
    activity_name: str

class UserCluster(NamedTuple):
    activity_id: int
    activity_name: str
    cluster_order: int

class ClusterInfo(NamedTuple):
    activity_id: int
    activity_name: str

ActivityParents = dict[ActivityInfo, ActivityParentInfo]
ClusterDict = dict[ActivityInfo, ClusterInfo]
ClusterMap = tuple[ActivityInfo, dict[int, int], dict[str, str]]

def get_parents_dict() -> dict[tuple[int, str], tuple[int, str]]:
    """
    Read the movimentos.json file and return a dictionary with
    the parent ID/activity of each ID/activity CNJ
    """
    with open('data/movimentos.json') as file:
        activity_parents = json.load(file)

    parents_dict: dict[tuple[int, str], tuple[int, str]] = dict()
    for item in activity_parents:
        parent_id = item['parentID']
        if parent_id != None: parent_id = int(parent_id)
        item_id = int(item['id'])
        item_activity = item['activity']
        parent_activity = item['parentActivity']
        parents_dict[(item_id,item_activity)] = (parent_id, parent_activity)
    return parents_dict

def format_cluster_dict(cluster_activities: list[UserCluster],
                        activity_parents: ActivityParents,
                        exclude_list: list[str] = []) -> ClusterDict:
    """
    From a list of UserCluster, return a dict identifying the cluster of each
    activity. If the activity is not in the cluster, the activity will be
    the cluster itself. The exclude_list is a list of activities that will not
    be clustered.

    Args:
        cluster_activities (list[UserCluster]): The list of clusters
        activity_parents (ActivityParents): The dict of activity parents
        exclude_list (list[str], optional): The list of activities to exclude.
            Defaults to [].
    
    Returns:
        ClusterDict: The dict with the cluster of each activity.
    """
    def add_edge_to_clusters_dict(cluster_id: int) -> None:
        if cluster_id in exclude_list: return 

        for act_id_name in activity_parents:
            activity_id, _ = act_id_name
            if activity_id is cluster_id: continue
            if activity_id in exclude_list: continue

            parent_id_name = activity_parents[act_id_name]
            if parent_id_name == (None, None): continue

            while parent_id_name[0] not in [None, cluster_id]:
                parent_id_name = activity_parents[parent_id_name]

            if parent_id_name[0] == cluster_id:
                cluster_dict[act_id_name] = (cluster_activity_id,
                                             cluster_activity_name)

    cluster_dict = dict()
    cluster_activities = sorted(cluster_activities, key=lambda x: x[2])
    for cluster_activity_id, cluster_activity_name, _ in cluster_activities:
        add_edge_to_clusters_dict(cluster_activity_id)

    for act_id_name in activity_parents:
        if act_id_name not in cluster_dict:
            cluster_dict[act_id_name] = act_id_name

    return cluster_dict

def get_cluster_keys_and_map_dicts(
    dataframe: pl.DataFrame, 
    cluster_dict: ClusterDict
) -> ClusterMap:
    mov_map_dict: dict[int, int] = dict()
    act_map_dict: dict[str, str] = dict()

    keys = (dataframe.select(pl.struct([
        ORIGINAL_ACTIVITY, ORIGINAL_ACTIVITY_NAME
    ])).unique().get_column(ORIGINAL_ACTIVITY))
    
    keys_list_temp = keys.to_list()
    keys_list = []
    for k in keys_list_temp:
        val_list = []
        for val in k.values():
            val_list.append(val)
        keys_list.append(tuple(val_list))

    temp_ids = list()
    temp_acts = list()
    for activity_id, activity_name in keys_list:
        key = (activity_id, activity_name)
        cluster_id, cluster_name = cluster_dict.get(key, (None, None))

        if cluster_id is not None:
            temp_ids.append(activity_id)
            temp_acts.append(activity_name)
            mov_map_dict[activity_id] = cluster_id
            act_map_dict[activity_name] = cluster_name  

    unique_acts = (pl.DataFrame({
        "ids": temp_ids,
        "acts": temp_acts
    }).select(pl.struct(["ids", "acts"]))
      .unique()
      .get_column("ids"))
    
    return unique_acts, mov_map_dict, act_map_dict

def cluster_dataframe(dataframe: pl.DataFrame,
                      cluster_dict: ClusterDict) -> pl.DataFrame:
    """
    Receives an DataFrame and a dict with clustering instructions.
    Replaces the child activity with the parent activity.
    """
    keys, mov_map_dict, act_map_dict = get_cluster_keys_and_map_dicts(
        dataframe, cluster_dict)
    
    return (dataframe.with_columns(
            pl.struct([ORIGINAL_ACTIVITY, ORIGINAL_ACTIVITY_NAME])
                .alias("key"),   
        )
        .with_columns(
            pl.when(pl.col("key").is_in(keys))
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias("is_in"),
        )
        .with_columns(
            pl.when(pl.col("is_in") == True)
                .then(
                    pl.col(ORIGINAL_ACTIVITY)
                        .map_dict(
                            mov_map_dict, default=pl.first()
                        ).cast(pl.Int64)
                        .alias(ACTIVITY_ID_NAME)
                )
                .otherwise(
                    pl.col(ACTIVITY_ID_NAME)
                ),
            pl.when(pl.col("is_in") == True)
                .then(
                    pl.col(ORIGINAL_ACTIVITY_NAME)
                        .map_dict(
                            act_map_dict, default=pl.first()
                        )
                        .alias(ACTIVITY_NAME)
                )
                .otherwise(
                    pl.col(ACTIVITY_NAME)
                )
        )
        .drop(["key", "is_in"]))

def group_dataframe(dataframe: pl.DataFrame,
                    clusters: list[tuple[int, str, int]])-> pl.DataFrame:
    """
    Run the clustering process on the dataframe and return the new dataframe.
    """
    parents_dict = get_parents_dict()
    cluster_dict = format_cluster_dict(clusters, parents_dict)
    return cluster_dataframe(dataframe, cluster_dict)
