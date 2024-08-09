from math import floor

def get_hour_slots(worktiming: list[int], weekends: list[int]
                   ) -> list[tuple[int, int]]:
    """
    Get the hour slots of the business hours from the worktiming and
    weekends informations.

    Parameters
    ----------
    worktiming
        A list[int] of the work schedule of the company (provided as
        a list where the first number is the start of the work time,
        and the second number is the end of the work time).
        Example: [8, 12, 13, 17] (8:00-12:00 and 13:00-17:00)
    weekends
        A list[int] of indexes of the days of the week that are weekend
        Default: [6, 7] (weekends are Saturday and Sunday)

    Returns
    -------
    hour_slots
        A list[int] of the hour slots of the business hours.
    """
    hour_slots = []
    for i in range(len(worktiming) // 2):
        start_hour = worktiming[i * 2]
        end_hour = worktiming[i * 2 + 1]
        hour_slots.append((start_hour * 3600, end_hour * 3600))
    hour_slots = hour_slots * 7
    for index, value in enumerate(hour_slots):
        if floor(index / 2) + 1 not in weekends:
            hour_slots[index] = (value[0] + floor(index / 2) * 24 * 3600,
                                 value[1] + floor(index / 2) * 24 * 3600)
        else:
            hour_slots[index] = (0, 0)
    for x in range(len(hour_slots) - 1, 0, -1):
        if hour_slots[x] == (0, 0):
            hour_slots.pop(x)
    return hour_slots
