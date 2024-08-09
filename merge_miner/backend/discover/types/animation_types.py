from datetime import datetime
from typing import TypedDict

class AnimationActivityData(TypedDict):
    id: str
    frequency: int

class CircleStep(TypedDict):
    id: str
    activity: str
    startTime: int
    duration: float
    timestamp: float
    theLastOne: bool
    theFirstOne: bool

class GetAnimationInfoReturn(TypedDict):
    first_case_date: datetime | None = None
    last_case_date: datetime | None = None
    edges: list[CircleStep]
    total_cases: int
    
class AnimationData(TypedDict):
    end_time: datetime
    total_duration: int
    start_time: datetime
    medians: tuple[float, float]
    edges: dict[str, list[CircleStep]]
    nodes: list[AnimationActivityData]

default_animation_data: AnimationData = {
    "edges": {},
    "nodes": [],
    "medians": (0, 0),
    "total_duration": 0,
    "end_time": datetime.now(),
    "start_time": datetime.now(),
}
