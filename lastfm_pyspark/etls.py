from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as DF
from pyspark.sql import window as W
from typing import Sequence

def assign_session_id(df: DF, threshold: int, user_col: str, time_col: str):
    """
    Assigns a session id to each row in a time series dataframe.
    A session is defined as a period of time where the user is active.
    The user is considered active if the time between two consecutive events
    is less than the threshold.
    The session id is a monotonically increasing integer.
    
    Parameters
    ----------
    df : pyspark.sql.dataframe.DataFrame
        Time series dataframe to assign session ids to.
        threshold : int
        The threshold in seconds.
        user_col : str
        The name of the user column.
        time_col : str
        The name of the time column.
        
        Returns
        -------
        pyspark.sql.dataframe.DataFrame
        The dataframe with the session id column.
    """

    w = W.Window.partitionBy(user_col).orderBy(time_col)
    return (
        df
        .select(
            '*',
            (F.col(time_col) - F.lag(time_col).over(w)).cast("long")
            .alias('inactive_time'),
        )
        .select(
            *df.columns,
            F.sum(F.when(F.col('inactive_time') > threshold, 1)
            .otherwise(0)).over(w).alias('session_id'),
        )
    )

def get_top_n_sessions(df: DF, session_key: Sequence[str], time_col: str, n: int):
    """
    Returns the top n sessions by duration in a time series dataframe.
    
    Parameters
    ----------
    df : pyspark.sql.dataframe.DataFrame
        Time series dataframe with identified sessions.
        session_key : Sequence[str]
        The columns that define a session.
        time_col : str
        The name of the time column.
        n : int
        The number of sessions to return.
        
        Returns
        -------
        pyspark.sql.dataframe.DataFrame
        The top n sessions.
    """
    return (
        df
        .groupBy(session_key)
        .agg(
            (F.max(time_col) - F.min(time_col)).cast('long').alias('session_duration'),
        )
        .orderBy(F.col('session_duration').desc())
        .limit(n)
    )
