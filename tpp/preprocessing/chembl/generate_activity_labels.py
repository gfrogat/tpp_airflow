import pyspark.sql.functions as F
import pyspark.sql.types as T

ACTIVE_COMMENTS = {"active", "Active"}
INACTIVE_COMMENTS = {
    ("Not Active (inhibition < 50% @ 10 uM and "
     "thus dose-response curve not measured)"),
    "Not Active",
    "inactive"
}

RELATIONS = {">", ">=", "<", "<=", "=", "~"}
INACTIVE_RELATIONS = {">", ">=", "=", "~"}
ACTIVE_RELATIONS = {"<", "<=", "=", "~"}


@F.udf(returnType=T.IntegerType())
def generate_activity_labels(
        activity_comment: str, std_value: float, std_unit: str,
        std_relation: str
) -> int:
    if activity_comment in INACTIVE_COMMENTS:
        return 1  # inactive
    elif activity_comment in ACTIVE_COMMENTS:
        return 3  # active
    elif std_value is None or std_unit != "nM" or std_relation not in RELATIONS:
        return 0  # unspecified
    else:
        if std_value <= 10 ** (
                9.0 - 5.5) and std_relation in ACTIVE_RELATIONS:
            return 3  # active
        elif std_value < 10 ** (
                9.0 - 5.0) and std_relation in ACTIVE_RELATIONS:
            return 13  # active (weakly)
        elif std_value >= 10 ** (
                9.0 - 4.5) and std_relation in INACTIVE_RELATIONS:
            return 1  # inactive
        elif std_value > 10 ** (
                9.0 - 5.0) and std_relation in INACTIVE_RELATIONS:
            return 11  # inactive (weakly)
        elif 10 ** (9.0 - 5.5) < std_value < 10 ** (9.0 - 4.5):
            return 2  # indeterminate
        else:
            return 0  # unspecified
