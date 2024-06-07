import psycopg2 as pg2
import pandas as pd
from typing import Optional
import numpy as np

DATABASE_HOST: str = "postgres_server"
DATABASE_USER: str = "postgres"


def connect_to_postgress(database_name: str) -> tuple:
    connection = pg2.connect(
        host=DATABASE_HOST, user=DATABASE_USER, database=database_name
    )

    cursor = connection.cursor()

    return connection, cursor


def column_value_as_string(column_value) -> str:
    if type(column_value) == str:
        return f"'{column_value}'"
    return column_value


def is_k_anonymized(
    dataframe: pd.DataFrame, k_value: int, quasi_identifiers: list[str]
) -> bool:
    """
    Based on: https://programming-dp.com/ch2.html
    """

    for _, row in dataframe.iterrows():
        query: str = " and ".join(
            [
                f"{column_name} == {column_value_as_string(row[column_name])}"
                for column_name in quasi_identifiers
            ]
        )

        matching_rows: pd.DataFrame = dataframe.query(query)
        if len(matching_rows) < k_value:
            return False

    return True


def get_albumin_per_gender(patient_data: pd.DataFrame) -> pd.DataFrame:
    gender_and_albumin: pd.DataFrame = patient_data[["gender", "albumin"]]
    albumin_per_gender = gender_and_albumin.groupby("gender").sum()
    return albumin_per_gender


def clip_column(
    data: pd.DataFrame,
    column: str,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> pd.DataFrame:
    data_clipped: pd.DataFrame = data.copy()

    data_clipped[column] = data_clipped[column].clip(
        lower=minimum, upper=maximum
    )

    return data_clipped


def query_patients_older_than(age: int, patient_data: pd.DataFrame) -> int:
    return len(patient_data.query(f"age >= {age}"))


def query_patients_by_range(
    column: str, minimum: int, maximum: int, data: pd.DataFrame
) -> int:
    return len(data.query(f"{column} >= {minimum} and {column} < {maximum}"))


def get_noise_using_laplace(
    function_sensitivity: int, privacy_parameter: float
) -> float:
    scale: float = function_sensitivity / privacy_parameter
    return np.random.laplace(loc=0, scale=scale)


def query_patients_with_disease(
    age: int, gender: str, data: pd.DataFrame
) -> int:
    return len(
        data.query(f"dataset == 1 and age == {age} and gender == '{gender}'")
    )


def query_patients_with_disease_v2(
    age: int, gender: str, data: pd.DataFrame
) -> float:
    count_sensitivity: int = 1
    privacy_budget: float = 0.1

    random_noise: float = get_noise_using_laplace(
        function_sensitivity=count_sensitivity,
        privacy_parameter=privacy_budget,
    )

    original_count: int = query_patients_with_disease(age, gender, data)

    return original_count + random_noise


def generate_samples(
    samples: int, item_list: list, probabilities: np.ndarray
) -> np.ndarray:
    return np.random.choice(item_list, samples, p=probabilities)


def group_by_bins(data: pd.DataFrame, column: str, bins: list[int]):
    out, bin_values = pd.cut(data[column], bins, retbins=True)
    data_by_bin = data.groupby(out)

    return data_by_bin


def add_laplace_noise(
    value: int,
    function_sensitivity: int,
    privacy_parameter: float,
    positive_only: bool,
) -> float:
    value_with_noise: float = value + get_noise_using_laplace(
        function_sensitivity, privacy_parameter
    )

    if not positive_only:
        return value_with_noise

    return max(value_with_noise, 0)


def normalise_series(original_series: pd.Series) -> pd.Series:
    total_sum: float = original_series.sum()
    return original_series.apply(
        lambda current_value: current_value / total_sum
    )


def get_percent_error(actual_value: float, estimated_value: float) -> float:
    return (actual_value - estimated_value) / actual_value * 100
