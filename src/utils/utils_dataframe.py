# pip freeze > requirements.txt
import re
from io import StringIO
import logging
import numpy as np
from configs import logging_config
from pathlib import Path
from datetime import datetime, timedelta
from etl_metadata.blueprints import expected_columns_dict
import pandas as pd

dataframe_log = logging_config.get_logger(name="dataframe_log", level=logging.DEBUG)

def add_timestamp(df: pd.DataFrame, col: str = "fetched_timestamp") -> pd.DataFrame:
    """
        Add a current timestamp column in Asia/Tashkent timezone for ETL tracking.
        Convert any existing date columns from ISO 8601 to Postgres TIMESTAMP format,
        rename them to <original>_timestamp, and drop the original column.

        Parameters
        ----------
        df : pd.DataFrame
            Input DataFrame.
        col : str, optional
            Column name to store the current timestamp. Default = 'fetched_timestamp'.

        Returns
        -------
        pd.DataFrame
            DataFrame with:
            - new column `col` containing current Tashkent timestamp
            - date columns converted to Postgres format, renamed to `<original>_timestamp`
    """
    tz = "Asia/Tashkent"
    df_out = df.copy()

    # 1. Add current timestamp column (Tashkent time)
    now_str = pd.Timestamp.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S")
    df_out[col] = now_str

    # 2. Known date columns to normalize
    date_cols = [
        'date', 'created_at', 'starts_at', 'ends_at',
        'updated_at', 'birthday', 'contract_date',
        'lesson_date', 'attendance_date', 'contract_end_date',
        'closed_at', 'closest_task_at', 'complete_till', 'actual_date',
        'due_date', 'last_activity', 'last_view', 'date_start', 'date_stop'
    ]

    # 3. Convert date columns to Postgres TIMESTAMP format
    for dc in date_cols:
        if dc in df_out.columns:
            new_col = f"{dc}_timestamp"

            # Try parsing as datetime
            dt = pd.to_datetime(df_out[dc], errors='coerce')

            # Fill invalid or missing with 1970-01-01 UTC
            dt = dt.fillna(pd.Timestamp('1970-01-01T00:00:00.000Z'))

            # If tz-naive, assume UTC before converting
            if dt.dt.tz is None:
                dt = dt.dt.tz_localize("UTC")

            # Convert to Tashkent and format as string
            df_out[new_col] = (
                dt.dt.tz_convert(tz)
                  .dt.strftime("%Y-%m-%d %H:%M:%S")
            )

            # Drop original column
            df_out.drop(columns=dc, inplace=True)

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


def log_df(df: pd.DataFrame):
    df_name = df.attrs.get("name") or "unidentified"
    dataframe_log.info(f"{'=' * 50}\n\nStarting analysis for DataFrame: {df_name}\n\n{'=' * 50}")
    try:
        # Check expected columns if provided
        if df_name in expected_columns_dict:
            expected_cols = expected_columns_dict[df_name]
            actual_cols = set(df.columns)

            missing = expected_cols - actual_cols
            extra = actual_cols - expected_cols

            if missing or extra:
                error_msg = f"❌🏛️ COLUMNS MISMATCH FOR {df_name}: "
                if missing:
                    error_msg += f"MISSING: {missing}. "
                if extra:
                    error_msg += f"EXTRA: {extra}. "
                logging.error(error_msg)

                # KEEP ONLY EXPECTED COLUMNS
                # Create missing columns as NaN to avoid KeyError
                for col in missing:
                    df[col] = None

                df = df[list(expected_cols)]
                logging.info("✅ Cleaned DataFrame to expected columns only.")

            else:
                logging.info("✅🏛️ EXPECTED COLUMNS MATCH.")

        # 1. Shape Check
        dataframe_log.info(f"1. Shape: {df.shape}")

        # Columns List
        # dataframe_log.info(f"Columns: {list(df.columns)}")

        # Data Types
        # dataframe_log.info(f"2. Data Types:\n{df.dtypes.to_string()}")

        # 2. Info Summary
        buffer = StringIO()
        df.info(buf=buffer)
        dataframe_log.info(f"2. Info:\n{buffer.getvalue()}")

        # 3. Descriptive Statistics
        dataframe_log.info(f"3. Describe:\n{df.describe().to_string()}")

        # Null Values Count
        # dataframe_log.info(f"Null Values:\n{df.isnull().sum().to_string()}")

        # 4. Null Percentage
        null_pct = (df.isnull().sum() / len(df)) * 100
        dataframe_log.info(f"4. Null Percentages:\n{null_pct.to_string()}")

        # 5. Duplicate Rows Count
        dataframe_log.info(f"5. Duplicate Rows: {df.duplicated().sum()}")

        # 6. Duplicate Percentage
        dup_pct = (df.duplicated().sum() / len(df)) * 100 if len(df) > 0 else 0
        dataframe_log.info(f"6. Duplicate Percentage: {dup_pct}")

        # 7. Unique Values per Column
        dataframe_log.info(f"7. Unique Values:\n{df.nunique().to_string()}")

        # Head Preview
        # dataframe_log.info(f"Head:\n{df.head(5).to_string()}")

        # 8. Outlier Detection (IQR)
        numeric_cols = df.select_dtypes(include=np.number).columns
        if not numeric_cols.empty:
            dataframe_log.info("8. Outliers (IQR method):")
            for col in numeric_cols:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                outliers_count = ((df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR))).sum()
                dataframe_log.info(f"   - Outliers in {col}: {outliers_count}")
        else:
            dataframe_log.info("8. No numeric columns for outlier detection.")

    except Exception as e:
        dataframe_log.error(f"Error analyzing {df_name}: {str(e)}")

    dataframe_log.info(f"Finished analysis for DataFrame: {df_name}")

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

    also use log df function to analyze df before saving
    """
    try:
        log_df(df)
        df_name = df.attrs.get("name") or "unidentified"
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
           "log_df",
           "save_df_with_timestamp"]