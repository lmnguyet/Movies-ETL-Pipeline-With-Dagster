import pandas as pd
import ast
import numpy as np

from dagster import asset, multi_asset, AssetIn, AssetOut, Output, StaticPartitionsDefinition

LAYER="gold_layer"
KEY_PREFIX=["the_movies", "gold"]
ABS_IO_MANAGER = "abs_io_manager"
UPSTREAM_KEY_PREFIX=["the_movies", "silver"]
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1915, 2021)]
)
RATINGS = StaticPartitionsDefinition(
    [str(rate) for rate in np.arange(0,5,0.5)]
)

@asset(
    description="Sort table 'movies'.",
    group_name=LAYER,
    io_manager_key=ABS_IO_MANAGER,
    key_prefix=KEY_PREFIX,
    compute_kind="pandas",
    ins={
        "silver_movies": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def gold_movies(silver_movies):
    df = silver_movies.sort_values(by="tmdb_id")
    return Output(
        df,
        metadata={
            "table name": "gold_movies",
            "records count": df.shape[0],
            "columns count": df.shape[1],
            "columns": df.columns.to_list(),
        }
    )

@multi_asset(
    group_name=LAYER,
    compute_kind="Pandas",
    ins={
        "silver_genres": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
    outs={
        "gold_genres": AssetOut(
            description="Create table 'genres' to store metadata of each genre.",
            io_manager_key=ABS_IO_MANAGER,
            key_prefix=KEY_PREFIX,
        ),
        "gold_movies_genres": AssetOut(
            description="Create table 'movies_genres' for each pair of movie and its genre.",
            io_manager_key=ABS_IO_MANAGER,
            key_prefix=KEY_PREFIX,
        ),

    }
)
def gold_movies_genres(silver_genres):
    """
        create table genres and movies genres
    """

    df = silver_genres
    df.dropna(inplace=True)
    df.drop(df[df["genres"].apply(lambda x: len(x)==0)].index, inplace=True)

    df_movies_genres = pd.DataFrame(columns=["tmdb_id", "genre_id"])
    df_genres = pd.DataFrame(columns=["genre_id", "name"])

    for index, row in df.iterrows():
        for genre in row["genres"]:
            df_movies_genres.loc[len(df_movies_genres)] = [row["id"], genre["id"]]

            if not df_genres["genre_id"].isin([genre["id"]]).any():
                df_genres.loc[len(df_genres)] = [genre["id"], genre["name"]]

    df_genres.sort_values(by="genre_id", inplace=True)
    df_movies_genres.sort_values(by="tmdb_id", inplace=True)

    table_names = ["gold_genres", "gold_movies_genres"]
    df_list = [df_genres, df_movies_genres]

    for i in range(len(table_names)):
        yield Output(
            df_list[i],
            output_name=table_names[i],
            metadata={
                "table name": table_names[i],
                "records count": df_list[i].shape[0],
                "columns count": df_list[i].shape[1],
                "columns": df_list[i].columns.to_list(),
            }
        )

@asset(
    group_name=LAYER,
    compute_kind="Pandas",
    description="Create table 'languages' to store metadata of each language.",
    io_manager_key=ABS_IO_MANAGER,
    key_prefix=KEY_PREFIX,
    ins={
        "silver_languages": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
)
def gold_languages(silver_languages):
    """
        create table languages
    """

    df = silver_languages
    df.dropna(inplace=True)
    df.drop(df[df["spoken_languages"].apply(lambda x: len(x)==0)].index, inplace=True)

    df_lang = pd.DataFrame(columns=["iso_code", "name"])

    for row in df["spoken_languages"]:
        for lang in row:
            if not df_lang["iso_code"].isin([lang["iso_639_1"]]).any():
                df_lang.loc[len(df_lang)] = [lang["iso_639_1"], lang["name"]]

    return Output(
        df_lang,
        metadata={
            "table name": "gold_languages",
            "records count": df_lang.shape[0],
            "columns count": df_lang.shape[1],
            "columns": df_lang.columns.to_list(),
        }
    )


@multi_asset(
    group_name=LAYER,
    compute_kind="Pandas",
    ins={
        "silver_companies": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
    outs={
        "gold_companies": AssetOut(
            description="Create table 'companies' to store metadata of each production company.",
            io_manager_key=ABS_IO_MANAGER,
            key_prefix=KEY_PREFIX,
        ),
        "gold_movies_production": AssetOut(
            description="Create table 'movies_production' for each movie.",
            io_manager_key=ABS_IO_MANAGER,
            key_prefix=KEY_PREFIX,
        ),

    }
)
def gold_movies_companies(silver_companies):
    """
        create table genres and movies genres
    """

    df = silver_companies
    df.dropna(inplace=True)
    df.drop(df[df["production_companies"].apply(lambda x: len(x)==0)].index, inplace=True)

    df_movies_production = pd.DataFrame(columns=["tmdb_id", "comp_id"])
    df_companies = pd.DataFrame(columns=["comp_id", "name"])

    for index, row in df.iterrows():
        for comp in row["production_companies"]:
            df_movies_production.loc[len(df_movies_production)] = [row["id"], comp["id"]]

            if not df_companies["comp_id"].isin([comp["id"]]).any():
                df_companies.loc[len(df_companies)] = [comp["id"], comp["name"]]

    df_companies.sort_values(by="comp_id", inplace=True)
    df_movies_production.sort_values(by="tmdb_id", inplace=True)

    table_names = ["gold_companies", "gold_movies_production"]
    df_list = [df_companies, df_movies_production]

    for i in range(len(table_names)):
        yield Output(
            df_list[i],
            output_name=table_names[i],
            metadata={
                "table name": table_names[i],
                "records count": df_list[i].shape[0],
                "columns count": df_list[i].shape[1],
                "columns": df_list[i].columns.to_list(),
            }
        )


@multi_asset(
    group_name=LAYER,
    compute_kind="Pandas",
    ins={
        "silver_crew": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
    outs={
        "gold_staff": AssetOut(
            description="Create table 'staff' to store metadata of each person.",
            io_manager_key=ABS_IO_MANAGER,
            key_prefix=KEY_PREFIX,
        ),
        "gold_movies_crew": AssetOut(
            description="Create table 'movies_crew' for each movie.",
            io_manager_key=ABS_IO_MANAGER,
            key_prefix=KEY_PREFIX,
        ),

    }
)
def gold_movies_crew(silver_crew):
    """
        create table crew and staff
    """

    df = silver_crew
    df.dropna(inplace=True)
    df.drop(df[df["crew"].apply(lambda x: len(x)==0)].index, inplace=True)

    df_movies_crew = pd.DataFrame(columns=["tmdb_id", "credit_id", "staff_id", "job"])
    df_staff = pd.DataFrame(columns=["staff_id", "name", "gender", "deparment"])

    for index, row in df.iterrows():
        for staff in row["crew"]:
            df_movies_crew.loc[len(df_movies_crew)] = [row["tmdb_id"], staff["credit_id"], staff["id"], staff["job"]]

            if not df_staff["staff_id"].isin([staff["id"]]).any():
                df_staff.loc[len(df_staff)] = [staff["id"], staff["name"], staff["gender"], staff["department"]]

    df_staff.sort_values(by="staff_id", inplace=True)
    df_movies_crew.sort_values(by="tmdb_id", inplace=True)

    table_names = ["gold_staff", "gold_movies_crew"]
    df_list = [df_staff, df_movies_crew]

    for i in range(len(table_names)):
        yield Output(
            df_list[i],
            output_name=table_names[i],
            metadata={
                "table name": table_names[i],
                "records count": df_list[i].shape[0],
                "columns count": df_list[i].shape[1],
                "columns": df_list[i].columns.to_list(),
            }
        )

@multi_asset(
    group_name=LAYER,
    compute_kind="Pandas",
    ins={
        "silver_cast": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
    outs={
        "gold_actors": AssetOut(
            description="Create table 'actors' to store metadata of each person.",
            io_manager_key=ABS_IO_MANAGER,
            key_prefix=KEY_PREFIX,
        ),
        "gold_movies_cast": AssetOut(
            description="Create table 'movies_cast' for each movie.",
            io_manager_key=ABS_IO_MANAGER,
            key_prefix=KEY_PREFIX,
        ),

    }
)
def gold_movies_cast(silver_cast):
    """
        create table crew and staff
    """

    df = silver_cast
    df.dropna(inplace=True)
    df.drop(df[df["cast"].apply(lambda x: len(x)==0)].index, inplace=True)

    df_movies_cast = pd.DataFrame(columns=["tmdb_id", "credit_id", "cast_id", "character"])
    df_actors = pd.DataFrame(columns=["actor_id", "name", "gender", "profile_path"])

    for index, row in df.iterrows():
        for cast in row["cast"]:
            df_movies_cast.loc[len(df_movies_cast)] = [row["tmdb_id"], cast["credit_id"], cast["id"], cast["character"]]

            if not df_actors["actor_id"].isin([cast["id"]]).any():
                df_actors.loc[len(df_actors)] = [cast["id"], cast["name"], cast["gender"], cast["profile_path"]]

    df_actors.sort_values(by="actor_id", inplace=True)
    df_movies_cast.sort_values(by="tmdb_id", inplace=True)

    table_names = ["gold_actors", "gold_movies_cast"]
    df_list = [df_actors, df_movies_cast]

    for i in range(len(table_names)):
        yield Output(
            df_list[i],
            output_name=table_names[i],
            metadata={
                "table name": table_names[i],
                "records count": df_list[i].shape[0],
                "columns count": df_list[i].shape[1],
                "columns": df_list[i].columns.to_list(),
            }
        )

@multi_asset(
    group_name=LAYER,
    compute_kind="Pandas",
    ins={
        "silver_keywords": AssetIn(
            key_prefix=UPSTREAM_KEY_PREFIX,
        ),
    },
    outs={
        "gold_keywords": AssetOut(
            description="Create table 'keywords' for the movies.",
            io_manager_key=ABS_IO_MANAGER,
            key_prefix=KEY_PREFIX,
        ),
        "gold_movies_keywords": AssetOut(
            description="Create table 'movies_keywords' for the movies.",
            io_manager_key=ABS_IO_MANAGER,
            key_prefix=KEY_PREFIX,
        )
    }
)
def gold_keywords(silver_keywords):
    """
        create table languages
    """

    df = silver_keywords
    df.dropna(inplace=True)
    df.drop(df[df["keywords"].apply(lambda x: len(x)==0)].index, inplace=True)

    df_movies_keywords = pd.DataFrame(columns=["tmdb_id", "keyword_id"])
    df_keywords = pd.DataFrame(columns=["keyword_id", "name"])

    for index, row in df.iterrows():
        for keyword in row["keywords"]:
            df_movies_keywords.loc[len(df_movies_keywords)] = [row["tmdb_id"], keyword["id"]]

            if not df_keywords["keyword_id"].isin([keyword["id"]]).any():
                df_keywords.loc[len(df_keywords)] = [keyword["id"], keyword["name"]]

    df_keywords.sort_values(by="keyword_id", inplace=True)
    df_movies_keywords.sort_values(by="tmdb_id", inplace=True)

    table_names = ["gold_keywords", "gold_movies_keywords"]
    df_list = [df_keywords, df_movies_keywords]

    for i in range(len(table_names)):
        yield Output(
            df_list[i],
            output_name=table_names[i],
            metadata={
                "table name": table_names[i],
                "records count": df_list[i].shape[0],
                "columns count": df_list[i].shape[1],
                "columns": df_list[i].columns.to_list(),
            }
        )