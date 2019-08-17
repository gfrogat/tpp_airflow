from typing import List

import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.pandas_udf(T.IntegerType(), F.PandasUDFType.GROUPED_AGG)
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
