# pip freeze > requirements.txt
import re
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime, timedelta
logger = logging.getLogger("omonschool_etl")


def add_timestamp(df: pd.DataFrame, col: str = "updated_timestamp") -> pd.DataFrame:
    """
    Add (or overwrite) a timestamp column in Uzbekistan time (Asia/Tashkent).
    Timestamp is formatted as a string for safe Postgres inserts.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    col : str, optional
        Column name to store the timestamp. Default = 'event_time'.

    Returns
    -------
    pd.DataFrame
        DataFrame with added timestamp column.
    """
    tz = "Asia/Tashkent"
    df_out = df.copy()
    df_out[col] = pd.Timestamp.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S")

    return df_out


def clean_string_columns(df: pd.DataFrame, allow_extra: str = "") -> pd.DataFrame:
    """
    Clean all string/object columns in a DataFrame:
    - Keep only letters, numbers, spaces (plus any allowed extra characters).
    - Remove everything else (e.g., quotes, backticks).
    - Strip leading/trailing spaces.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame to clean.
    allow_extra : str, optional
        Extra characters to allow (e.g., "_-"). Default = "".

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame (copy).
    """
    df_clean = df.copy()
    # Build regex pattern dynamically
    pattern = rf'[^A-Za-z0-9\s{allow_extra}:.%+=() -]'

    for col in df_clean.select_dtypes(include=['object', 'string']).columns:
        df_clean[col] = (
            df_clean[col]
            .astype(str)
            .str.replace(pattern, '', regex=True)
            .str.strip()
        )

    return df_clean


def fill_and_numeric(series, fill_value=0, dtype="float"):
    """
    Fill NA values and convert a pandas Series to numeric type.

    Parameters:
        series (pd.Series): Input column.
        fill_value (int/float/str): Value to fill NA.
                                    If "mean" or "median", compute from series.
        dtype (str): "float" (default) or "int".

    Returns:
        pd.Series: Cleaned column.
    """
    if fill_value == "mean":
        value = series.mean()
    elif fill_value == "median":
        value = series.median()
    else:
        value = fill_value

    series = series.fillna(value)
    series = pd.to_numeric(series, errors="coerce")

    if dtype == "int":
        # cast safely: drop NaN first or fill again
        series = series.fillna(0).astype(int)
    else:
        series = series.astype(float)

    return series


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename DataFrame columns into snake_case:
    - Split camelCase / PascalCase
    - Replace spaces / dashes with underscores
    - Lowercase everything

    Example:
    headTeacher_firstName -> head_teacher_first_name
    FirstName -> first_name
    some-column -> some_column
    """

    def to_snake(name: str) -> str:
        # Replace camelCase or PascalCase with underscores
        name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
        # Replace multiple separators with single underscore
        name = re.sub(r'[\s\-]+', '_', name)
        # Lowercase and clean double underscores
        return name.lower().strip('_')

    df_out = df.rename(columns=lambda x: to_snake(str(x)))

    return df_out


def save_df_with_timestamp(
    df: pd.DataFrame,
    df_name: str,
    backup_dir: str = "data_backup"
) -> str:
    """
    Save DataFrame to CSV in backup folder.

    Args:
        df (pd.DataFrame): DataFrame to save.
        df_name (str): Base name for the CSV file.
        backup_dir (str): Directory to store backups (default: data_backup).

    Returns:
        str: Path of the saved CSV file.
    """
    try:
        # Uzbekistan is UTC+5
        now_uzbek = datetime.utcnow() + timedelta(hours=5)

        # ensure backup folder exists
        backup_folder = Path(backup_dir)
        backup_folder.mkdir(parents=True, exist_ok=True)

        # build file name
        filename = f"{df_name}__{now_uzbek.strftime('%Y_%m_%d_%H-%M-%S')}.csv"
        file_path = backup_folder / filename

        # save to CSV
        df.to_csv(file_path, index=False, encoding="utf-8")
        logging.info(f"✅ DataFrame saved: {file_path}")
        return str(file_path)

    except Exception as e:
        logging.error(f"❌ Failed to save DataFrame {df_name}: {e}")
        raise


__all__ = ["clean_string_columns",
           "add_timestamp",
           "normalize_columns",
           "fill_and_numeric",
           "save_df_with_timestamp"]