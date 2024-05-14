from dagster import asset, multi_asset, AssetOut, AssetIn, Output, StaticPartitionsDefinition
import numpy as np
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
import json
import os

LAYER = "bronze_layer"
KEY_PREFIX = ["the_movies", "bronze"] # container = "bronze"
ABS_IO_MANAGER = "abs_io_manager"
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1915, 2021)]
)
RATINGS = StaticPartitionsDefinition(
    [str(rate) for rate in np.arange(0,5,0.5)]
)

@asset(
    description="Convert npy file 'imdb_user_rating_dataset' to parquet and load to ABS.",
    group_name=LAYER,
    key_prefix=KEY_PREFIX,
    io_manager_key=ABS_IO_MANAGER,
)
def bronze_imdb_user_rating_dataset() -> Output[pd.DataFrame]:
    file_path = "../dataset_preprocessing/Dataset.npy"

    dataset = np.load(file_path)
    transformed_data = []

    for record in dataset:
        data = str(record).split(",")
        # print(data)
        new_dict = {
            "user_id": data[0],
            "movie_id": data[1],
            "rating": data[2],
            "review_date": data[3],
        }
        transformed_data.append(new_dict)

    df = pd.DataFrame(transformed_data)

    yield Output(
        df,
        metadata={
            "table name": "bronze_imdb_user_rating_dataset",
            "records count": df.shape[0],
            "columns count": df.shape[1],
            "columns": df.columns.to_list(),
            # "container": "bronze",
        },
    )

# mysql -> df -> parquet -> blob
@asset(
    description="Load the 'movies' table from MySQL to ABS.",
    required_resource_keys={"mysql_io_manager"},
    group_name=LAYER,
    compute_kind="MySQL",
    key_prefix=KEY_PREFIX,
    io_manager_key=ABS_IO_MANAGER,
)
def bronze_movies(context) -> Output[pd.DataFrame]:
    table_name = "bronze_movies"
    sql_stm = "select * from " + table_name
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)

    return Output(
        pd_data,
        metadata={
            "table name": table_name,
            "records count": pd_data.shape[0],
            "columns count": pd_data.shape[1],
            "columns": pd_data.columns.to_list()
        },
    )

@asset(
    description="Load the 'reviews' table from MySQL to ABS.",
    required_resource_keys={"mysql_io_manager"},
    group_name=LAYER,
    compute_kind="MySQL",
    key_prefix=KEY_PREFIX,
    io_manager_key=ABS_IO_MANAGER,
    partitions_def=YEARLY,
)
def bronze_reviews(context) -> Output[pd.DataFrame]:
    table_name = "bronze_reviews"
    sql_stm = "select * from " + table_name
    try:
        partition_str = context.asset_partition_key_for_output()
        partitioned_by = "year"
        sql_stm += f" where {partitioned_by} = {partition_str}"
        context.log.info(f"Partitioning {partitioned_by} = {partition_str}")
    except Exception as e:
        context.log.error(str(e))

    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    return Output(
        pd_data,
        metadata={
            "table name": table_name,
            "records count": pd_data.shape[0],
            "columns count": pd_data.shape[1],
            "columns": pd_data.columns.to_list()
        },
    )

@asset(
    description="Download 'The Movies Dataset' from Kaggle",
    group_name=LAYER,
    key_prefix=KEY_PREFIX,
    compute_kind="KaggleAPI",
)
def download_kaggle_dataset(context):
    dataset_name = "rounakbanik/the-movies-dataset"
    # kaggle==1.5.16
    download_path = "../dataset_preprocessing/kaggle_dataset"
    try:
        api_key = ""
        with open("/root/.kaggle/kaggle.json", "r") as json_file:
            kaggle_api = json.load(json_file)
            api_key = kaggle_api["key"]
        
        api = KaggleApi(api_client=api_key)
        api.authenticate()

        api.dataset_download_files(dataset=dataset_name, path=download_path, unzip=True)

        context.log.info(f"Download kaggle dataset successfully!")

        csv_files = os.listdir(download_path)

        return Output(
            download_path,
            metadata={
                "files count": len(csv_files),
                "file names": csv_files,
            },
        )
    except Exception as e:
        context.log.error(str(e))
        return None

# kaggle api -> parquet -> blob
@multi_asset(
    compute_kind="KaggleAPI",
    group_name=LAYER,
    ins={
        "download_kaggle_dataset": AssetIn(
            key_prefix=KEY_PREFIX,
        ),
    },
    outs={
        "bronze_credits": AssetOut(
            description="Load table 'credits' to ABS.",
            key_prefix=KEY_PREFIX,
            io_manager_key=ABS_IO_MANAGER,
            metadata={
                "batch_size": 4500,
            }
        ),
        "bronze_keywords": AssetOut(
            description="Load table 'keywords' to ABS.",
            key_prefix=KEY_PREFIX,
            io_manager_key=ABS_IO_MANAGER,
            metadata={
                # "table name": "keywords",
                "batch_size": 4500,
            },
        ),
        "bronze_movies_metadata": AssetOut(
            description="Load table 'movies_metadata' to ABS.",
            key_prefix=KEY_PREFIX,
            io_manager_key=ABS_IO_MANAGER,
        ),
    }
)
def bronze_kaggle_dataset(download_kaggle_dataset) -> Output[pd.DataFrame]:
    if download_kaggle_dataset is None:
        pass

    download_path = download_kaggle_dataset

    for file in os.listdir(download_path):
        table_name = "bronze_" + file.split(".")[0]

        if table_name.startswith("bronze_links") or table_name.startswith("bronze_ratings"):
            continue

        file_path = os.path.join(download_path, file)
        
        pd_data = pd.read_csv(file_path)
        if "popularity" in pd_data.columns:
            pd_data.drop("popularity", axis=1, inplace=True)
        yield Output(
            pd_data,
            output_name=table_name,
            metadata={
                "table name": table_name,
                "records count": pd_data.shape[0],
                "columns count": pd_data.shape[1],
                "columns": pd_data.columns.to_list()
            },
        )

@asset(
    description="Load table 'ratings' to ABS.",
    key_prefix=KEY_PREFIX,
    io_manager_key=ABS_IO_MANAGER,
    compute_kind="KaggleAPI",
    group_name=LAYER,
    partitions_def=RATINGS,
    ins={
        "download_kaggle_dataset": AssetIn(
            key_prefix=KEY_PREFIX,
        ),
    },
)
def bronze_ratings(context, download_kaggle_dataset) -> Output[pd.DataFrame]:
    if download_kaggle_dataset is None:
        pass

    table_name = "bronze_ratings"

    file_path = os.path.join(download_kaggle_dataset, f"{table_name[7:]}.csv")

    pd_data = pd.read_csv(file_path)
    try:
        partition_str = context.asset_partition_key_for_output()
        # pd_data = next(df_iter)
        pd_data = pd_data[pd_data['rating'] == float(partition_str)]
        context.log.info(f"Partitioning rating = {partition_str}")
    except Exception as e:
        context.log.error(str(e))
    
    yield Output(
        pd_data,
        metadata={
            "table name": table_name,
            "records count": pd_data.shape[0],
            "columns count": pd_data.shape[1],
            "columns": pd_data.columns.to_list()
        },
    )