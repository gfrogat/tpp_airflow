from typing import List

ACTIVE_COMMENTS = {"active", "Active"}
INACTIVE_COMMENTS = {
    (
        "Not Active (inhibition < 50% @ 10 uM and "
        "thus dose-response curve not measured)"
    ),
    "Not Active",
    "inactive",
}

RELATIONS = {">", ">=", "<", "<=", "=", "~"}
INACTIVE_RELATIONS = {">", ">=", "=", "~"}
ACTIVE_RELATIONS = {"<", "<=", "=", "~"}


def generate_activity_labels(
    activity_comment: str, std_value: float, std_unit: str, std_relation: str
) -> int:
    if activity_comment in INACTIVE_COMMENTS:
        return 1  # inactive
    elif activity_comment in ACTIVE_COMMENTS:
        return 3  # active
    elif std_value is None or std_unit != "nM" or std_relation not in RELATIONS:
        return 0  # unspecified
    else:
        if std_value <= 10 ** (9.0 - 5.5) and std_relation in ACTIVE_RELATIONS:
            return 3  # active
        elif std_value < 10 ** (9.0 - 5.0) and std_relation in ACTIVE_RELATIONS:
            return 13  # active (weakly)
        elif std_value >= 10 ** (9.0 - 4.5) and std_relation in INACTIVE_RELATIONS:
            return 1  # inactive
        elif std_value > 10 ** (9.0 - 5.0) and std_relation in INACTIVE_RELATIONS:
            return 11  # inactive (weakly)
        elif 10 ** (9.0 - 5.5) < std_value < 10 ** (9.0 - 4.5):
            return 2  # indeterminate
        else:
            return 0  # unspecified


def clean_activity_labels(activities: List[int]) -> int:
    activities = set(activities)

    if len(activities) == 1:
        return list(activities)[0]

    is_garbage = 0 in activities
    is_inactive = 1 in activities
    is_inconclusive = 2 in activities
    is_active = 3 in activities

    if is_garbage or (is_inactive and is_active):  # garbage
        return 0
    elif is_inconclusive:  # inconclusive
        return 2
    elif is_inactive:  # inactive
        return 1
    elif is_active:  # active
        return 3

    # compute weak activity
    is_weakly_active = 11 in activities
    is_weakly_inactive = 13 in activities
    if is_weakly_inactive and is_weakly_active:  # weakly garbage
        return 10
    elif is_weakly_inactive:  # weakly inactive
        return 11
    elif is_weakly_active:  # weakly active
        return 13
