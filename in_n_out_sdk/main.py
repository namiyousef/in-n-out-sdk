import json
import requests
import pandas as pd
import io

from in_n_out_sdk.config import IN_N_OUT_URL

from typing import Optional

# TODO needs to be moved in a separate module entirely
from in_n_out_sdk.utils import _convert_timezone_columns_to_utc

# TODO need to understand what content-type and accept do...
# TODO need to run speed comparison for Streaming vs. nonStreaming response!

# TODO need to see what file response does!
# TODO need to see if what we've done now applies for get/put and post request!

headers = {
    "Content-Type": "application/json",
    "Accept": "application/octet-stream",
}


def _is_status_code_valid(status_code):
    if str(status_code).startswith("2"):
        return True


def health_check():
    """Checks if API healthy

    :return: `True` if API healthy, else `False`
    :rtype: bool
    """
    url = f"{IN_N_OUT_URL}/health_check"

    resp = requests.get(url)
    status_code = resp.status_code

    return _is_status_code_valid(status_code)


def write_data(
    table_name: str,
    database_type: str,
    data,
    limit: int = -1,
    database_name: str = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    port: Optional[int] = None,
    host: Optional[str] = None,
    dataframe_content_type: Optional[str] = None,
    on_asset_conflict: str = "append",
    on_data_conflict: str = "fail",
    data_conflict_properties: Optional[list] = None,
    dataset_name: str = None,
):
    """_summary_

    :param database_name: _description_
    :type database_name: _type_
    :param table_name: _description_
    :type table_name: _type_
    :param limit: _description_, defaults to -1
    :type limit: int, optional
    :param database_type: _description_, defaults to "pg"
    :type database_type: str, optional
    :param df: _description_, defaults to pd.DataFrame()
    :type df: _type_, optional
    :param content_type: _description_, defaults to "csv"
    :type content_type: str, optional
    :param conflict_resolution_strategy: _description_, defaults to "fail"
    :type conflict_resolution_strategy: str, optional
    :param username: _description_, defaults to "postgres"
    :type username: str, optional
    :param port: _description_, defaults to 5432
    :type port: int, optional
    :param host: _description_, defaults to "localhost"
    :type host: str, optional
    :param dataframe_content_type: _description_, defaults to None
    :type dataframe_content_type: _type_, optional
    :return: _description_
    :rtype: _type_
    """

    url = f"{IN_N_OUT_URL}/insert?limit={limit}"

    if isinstance(data, pd.DataFrame):
        read_data_as_dataframe = True
        if dataframe_content_type is None:
            raise ValueError(
                "If input is a dataframe, you must specify a `dataframe_content_type`"
            )
        elif dataframe_content_type == "parquet":
            # TODO later might need to think... how to expand dataframe_content_type to allow for say image being parsed etc...
            memory_buffer = io.BytesIO()
            data.to_parquet(memory_buffer, engine="pyarrow")
            memory_buffer.seek(0)

            files = {
                "file": (
                    "upload_file",
                    memory_buffer,
                    "application/octet-stream",
                )
            }
        else:
            raise NotImplementedError(
                "For data inputs of type dataframe, the only `dataframe_content_type` allowed is currently parquet"
            )
    else:
        read_data_as_dataframe = False
        files = {"file": ("upload_file", json.dumps(data), "application/json")}

    url = f"{url}&read_data_as_dataframe={read_data_as_dataframe}"

    insertion_params = {
        "database_name": database_name,
        "table_name": table_name,
        "database_type": database_type,
        "on_data_conflict": on_data_conflict,
        "on_asset_conflict": on_asset_conflict,
        "username": username,
        "password": password,
        "port": port,
        "host": host,
        "dataset_name": dataset_name,
        "data_conflict_properties": data_conflict_properties,
    }

    insertion_params = {
        k: v for k, v in insertion_params.items() if v is not None
    }

    data = {"insertion_params": json.dumps(insertion_params)}
    # TODO need to update this for diff content types
    try:
        resp = requests.post(url, data=data, files=files)
    except requests.exceptions.ConnectionError as connection_error:
        return (
            f"Failed to connect to API. Is API healthy? Full details: {connection_error}",
            503,
        )

    return resp.text, resp.status_code


def read_data(
    ingestion_param,
    limit=-1,
    stream_data=False,
):
    url = f"{IN_N_OUT_URL}/ingest?limit={limit}"

    if not stream_data:
        resp = requests.post(url, json=ingestion_param, headers=headers)
        status_code = resp.status_code
        if _is_status_code_valid(status_code):
            df = pd.read_parquet(io.BytesIO(resp.content), engine="pyarrow")
            return df, status_code
    else:
        with requests.post(
            url, json=ingestion_param, headers=headers, stream=True
        ) as resp:
            data = b""
            for line in resp.iter_content(1048):
                data += line
            memory_buffer = io.BytesIO(data)
            print(len(data))
            print(pd.read_parquet(memory_buffer, engine="pyarrow"))
        return None, None
    status_code = resp.status_code

    if _is_status_code_valid(status_code):
        df = pd.read_parquet(resp.text, engine="pyarrow")
        # df = resp.json()
        return df, status_code
    return resp.text, resp.status_code


if __name__ == "__main__":
    import datetime as dt
    import pytz

    DF_INITIAL = pd.DataFrame(
        {
            "datetime": [dt.datetime.now()],
            "datetime_with_timestamp": [
                dt.datetime.now(tz=pytz.timezone("UTC"))
            ],
            "float": [1.0],
            "string": ["test_string"],
            "int": [1],
            "boolean": [True],
        }
    )

    limit = -1
    database_type = "test"
    df = DF_INITIAL
    content_type = "parquet"
    on_data_conflict = "replace"
    on_asset_conflict = "fail"
    username = "postgres"
    password = "postgres"
    port = 5432
    database_name = "postgres"
    table_name = "testing_2"
    host = "localhost"
    text, status_code = write_data(
        limit=limit,
        database_type=database_type,
        port=port,
        data=df,
        dataframe_content_type=content_type,
        username=username,
        password=password,
        database_name=database_name,
        table_name=table_name,
        host=host,
    )

    print(text, status_code)

    data = [
        {
            "end": {"date": "2023-08-14"},
            "iCalUID": "testing12345",
            "recurrence": ["RRULE:FREQ=YEARLY;BYMONTH=8;BYMONTHDAY=14"],
            "start": {"date": "2023-08-14"},
            "summary": "Emmeran Johnson testing",
        }
    ]
    resp, status_code = write_data(
        database_type="google_calendar",
        data=data,
        table_name="yousefofthenamis@gmail.com",
        on_asset_conflict="append",
        on_data_conflict="ignore",
        data_conflict_properties=["iCalUID"],
    )

    print(resp, status_code)
