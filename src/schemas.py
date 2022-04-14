from enum import Enum
import pandera as pa


class Checks(Enum):
    TWITTER_COLUMN = pa.Check(lambda s: s.str.startswith("@"))


EVENT_DB_SCHEMA = pa.DataFrameSchema(
    {
        "event_id": pa.Column(int, coerce=True, nullable=False, unique=True),
        "event_name": pa.Column(str, coerce=True, nullable=False),
        "event_category": pa.Column(str, coerce=True, nullable=False),
        "event_twitter": pa.Column(str, coerce=True, nullable=False, checks=[
            Checks.TWITTER_COLUMN.value
        ]),
        "event_date": pa.Column(pa.DateTime, coerce=True, nullable=False)
    }
)

PARTICIPANT_SCHEMA = pa.DataFrameSchema(
    {
        "participant_name": pa.Column(str, coerce=True, nullable=True),
        "participant_site": pa.Column(str, coerce=True, nullable=True),
        "participant_twitter": pa.Column(str, coerce=True, nullable=True, checks=[
            Checks.TWITTER_COLUMN.value
        ]),
    }
)

EVENT_PARTICIPANTS_SCHEMA = EVENT_DB_SCHEMA and PARTICIPANT_SCHEMA
