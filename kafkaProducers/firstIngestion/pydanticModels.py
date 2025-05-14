from pydantic import BaseModel, model_validator
from typing import Any, Dict, List, Union

class BoxScoreResultSet(BaseModel):
    """
    Pydantic model defining individual result set within the
    boxscore data.
    """
    name: str
    headers: List[str]
    rowSet: List[List[Union[str, int, float, None]]]

class LeagueGameLog(BaseModel):
    """
    Pydantic model defining the overall response structure
    for the NBA leaguegamelog endpoint.
    """
    resource: str
    parameters: Dict[str, Any]
    resultSets: List[BoxScoreResultSet]

    @model_validator(mode='after')
    def skip_no_data(cls, instance: "LeagueGameLog") -> "LeagueGameLog":
        """
        post-init validator that checks:
          1) `resource` is not empty
          2) `resultSets` is not empty
          3) each `resultSets[].rowSet` is not empty
        Raise a ValueError if any criteria is violated.
        """
        # 1) Ensure 'resource' is non-empty
        if not instance.resource:
            raise ValueError("Invalid record: 'resource' field is empty or missing.")

        # 2) Ensure 'resultSets' is present and non-empty
        if not instance.resultSets:
            raise ValueError("Invalid record: 'resultSets' is empty or missing.")

        # 3) Ensure each BoxScoreResultSet has a non-empty rowSet
        for idx, rs in enumerate(instance.resultSets):
            if not rs.rowSet:
                raise ValueError(
                    f"Invalid record: resultSets[{idx}].rowSet is empty."
                )

        # If all checks pass, return the instance
        return instance
