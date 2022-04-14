import os

import pandas as pd

from src.schemas import EVENT_PARTICIPANTS_SCHEMA, EVENT_DB_SCHEMA

pd.set_option("display.max_rows", None, "display.max_columns", None)

DATA_PATH = 'data'
GAMMA_PATH = os.path.join(DATA_PATH, 'gamma.csv')
BETA_PATH = os.path.join(DATA_PATH, 'beta.csv')
ALPHA_PATH = os.path.join(DATA_PATH, 'alpha.csv')
EVENT_DB_PATH = os.path.join(DATA_PATH, 'event_db.csv')
PARTICIPANT_COLUMN_MAPPING = {
    'eventname': 'event_name',
    'eventdate': 'event_date',
    'who': 'participant_name',
    'site': 'participant_site',
    'eventid': 'event_id',
    'name': 'event_name',
    'twitter': 'participant_twitter',
    'person_twitter': 'participant_twitter'
}
EVENT_COLUMN_MAPPING = {
    'eventid': 'event_id',
    'name': 'event_name',
    'category': 'event_category',
    'twitter': 'event_twitter',
    'date': 'event_date'
}
PARTICIPANT_COLUMNS = ['participant_name', 'participant_site', 'participant_twitter']


def read_participant_dataframes() -> pd.DataFrame:
    df_alpha = pd.read_csv(ALPHA_PATH, parse_dates=['eventdate'])
    df_alpha.rename(
        columns=PARTICIPANT_COLUMN_MAPPING,
        inplace=True
    )

    df_gamma = pd.read_csv(GAMMA_PATH, parse_dates=['event_date'])
    df_gamma.rename(
        columns=PARTICIPANT_COLUMN_MAPPING,
        inplace=True
    )

    df_beta = pd.read_csv(BETA_PATH)
    df_beta.rename(
        columns=PARTICIPANT_COLUMN_MAPPING,
        inplace=True
    )
    return pd.concat([df_alpha, df_beta, df_gamma]).reset_index(drop=True)


def read_event_dataframe() -> pd.DataFrame:
    df_event_db = pd.read_csv(EVENT_DB_PATH, parse_dates=['date'])
    df_event_db.rename(
        columns=EVENT_COLUMN_MAPPING,
        inplace=True
    )
    return df_event_db


def create_event_participants_dataframe(df_participants: pd.DataFrame, df_event_db: pd.DataFrame) -> pd.DataFrame:
    event_name_not_null = df_participants['event_name'].notnull()
    event_twitter_not_null = df_participants['event_twitter'].notnull()

    df_event_participants = (
            pd.concat([
                df_participants.loc[event_name_not_null, PARTICIPANT_COLUMNS + ['event_name']].merge(
                    df_event_db, how='left', on='event_name'
                ).copy(),
                df_participants.loc[event_twitter_not_null, PARTICIPANT_COLUMNS + ['event_twitter']].merge(
                    df_event_db, how='left', on='event_twitter'
                ).copy(),
            ])
            # Some events are not recorded in event_db, we're dropping them, because event_db is a fact table
            # and should contain all information about events
            .dropna(axis=0, subset=['event_id'])
            # Convert to best possible data type
            .convert_dtypes()
    )
    return df_event_participants


if __name__ == "__main__":
    df_participants = read_participant_dataframes()
    # Remove unnecessary columns
    df_participants.drop(columns=['event_date', 'event_month'], inplace=True)

    df_event_db = read_event_dataframe()
    # Make sure that event data is clean
    EVENT_DB_SCHEMA.validate(df_event_db)

    df_event_participants = create_event_participants_dataframe(df_participants, df_event_db)

    # Make sure that at least one participant information point is present
    assert (df_event_participants[PARTICIPANT_COLUMNS].isnull().sum(axis=1) == 0).sum() == 0

    # Validate results schema
    EVENT_PARTICIPANTS_SCHEMA.validate(df_event_participants)

    # Display results
    print(df_event_participants)
