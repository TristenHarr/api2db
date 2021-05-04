# -*- coding: utf-8 -*-
"""
Contains the FileConverter class
================================
"""
import os
import pickle
import pandas as pd
from ..app.log import get_logger
from typing import Optional, Union


class FileConverter(object):
    """Serves as a base-class for all Streams/Stores and is used to store/load pandas DataFrames to different formats"""

    def __init__(self,
                 name: Optional[str] = None,
                 dtypes: Optional[dict] = None,
                 path: Optional[str] = None,
                 fmt: Optional[str] = None
                 ):
        """
        Creates a FileConverter object and attempts to build its dtypes

        Args:
            name: The name of the collector associated with the FileConverter
            dtypes: The dictionary of dtypes for the collector associated with the FileConverter
            path: Either a path to a file, or a path to a directory, dictated by super class
            fmt:

                * `fmt="parquet"` (recommended) sets the FileConverter format to use parquet format
                * `fmt="json"` sets the FileConverter format to use JSON format
                * `fmt="pickle"` sets the FileConverter format to use pickle format
                * `fmt="csv"` sets the FileConverter format to use CSV format

        """
        self.name = name
        self.dtypes = dtypes
        self.path = path
        self.fmt = fmt
        self.dtypes = self.build_dtypes()

    def build_dtypes(self) -> Union[dict, None]:
        """
        Attempts to build the dtypes so that a loaded pandas DataFrame can be type-casted

        Return:
            dtypes that can be used with pandas.DataFrame.astype(dtypes)
        """
        if self.dtypes is None and self.name is not None:
            dtypes_path = os.path.join("CACHE", f"{self.name}_dtypes.pkl")
            if os.path.isfile(dtypes_path):
                with open(dtypes_path, "rb") as f:
                    dtypes = pickle.load(f)
                return dtypes
        return self.dtypes

    @staticmethod
    def static_compose_df_from_dir(path: str,
                                   fmt: str,
                                   move_shards_path: Optional[str]=None,
                                   move_composed_path: Optional[str]=None,
                                   force: bool=True) -> Union[pd.DataFrame, None]:
        """
        Attempts to build a single DataFrame from all files in
        a directory.

        Args:
            path: The directory path to compose files from
            fmt:

                * `fmt="parquet"` (recommended) stores the DataFrame using parquet format
                * `fmt="json"` stores the DataFrame using JSON format
                * `fmt="pickle"` stores the DataFrame using pickle format
                * `fmt="csv"` stores the DataFrame using csv format

            move_shards_path: The path to move the file shards to after composing the DataFrame
            move_composed_path: The path to move the composed shards to, with naming schema of filename1_filenameN.fmt
            force: Forces creation of the directories to move files to if they do not exist.

        Returns:
            The DataFrame composed from the sharded files if successful, else None

        Example:

            **Original Directory Structure**

            ::

                store/
                |    |- file1.parquet
                |    |- file2.parquet
                |    |- file3.parquet
                |
                sharded/
                |
                composed/
                |
                main.py

            The following files contain pandas DataFrames stored using parquet format.

            **file1.parquet**

            =  =  =
            A  B  C
            =  =  =
            1  2  3
            =  =  =

            **file2.parquet**

            =  =  =
            A  B  C
            =  =  =
            4  5  6
            =  =  =

            **file3.parquet**

            =  =  =
            A  B  C
            =  =  =
            7  8  9
            =  =  =

            >>> FileConverter.static_compose_df_from_dir(path="store/", fmt="parquet")

            **Returns (pandas.DataFrame):**

            =  =  =
            A  B  C
            =  =  =
            1  2  3
            4  5  6
            7  8  9
            =  =  =

            **By default, files will be deleted when the DataFrame is returned**

            .. code-block:: python3

                FileConverter.static_compose_df_from_dir(path="store/",
                                                         fmt="parquet",
                                                         move_shards_path=None,
                                                         move_composed_path=None)

            ::

                store/
                |
                sharded/
                |
                composed/
                |
                main.py

            ``move_shards_path`` **specifies the path the sharded files should be moved to**

            .. code-block:: python3

                FileConverter.static_compose_df_from_dir(path="store/",
                                                         fmt="parquet",
                                                         move_shards_path="sharded/",
                                                         move_composed_path=None)

            ::

                store/
                |
                sharded/
                |      |- file1.parquet
                |      |- file2.parquet
                |      |- file3.parquet
                |
                composed/
                |
                main.py

            ``move_composed_path`` **speficies the path that the recomposed files should be moved to**

            .. code-block:: python3

                FileConverter.static_compose_df_from_dir(path="store/",
                                                         fmt="parquet",
                                                         move_shards_path=None,
                                                         move_composed_path="composed/")

            ::

                store/
                |
                sharded/
                |
                composed/
                |       |- file1_file3.parquet
                |
                main.py


        """
        logger = get_logger()
        df = None
        if os.path.isdir(path):
            files = os.listdir(path)
            for fname in files:
                new_df = FileConverter.static_load_df(path=os.path.join(path, fname), fmt=fmt)
                if df is None:
                    df = new_df
                else:
                    if new_df is not None:
                        try:
                            df = df.append(new_df)
                        except Exception as e:
                            logger.exception(e)
            if df is None:
                return None
            if move_shards_path is not None:
                if not os.path.isdir(move_shards_path) and force:
                    try:
                        os.makedirs(move_shards_path)
                    except Exception as e:
                        logger.exception(e)
                if os.path.isdir(move_shards_path):
                    for fname in files:
                        try:
                            os.rename(os.path.join(path, fname), os.path.join(move_shards_path, fname))
                        except Exception as e:
                            logger.exception(e)
            else:
                for fname in files:
                    try:
                        os.remove(os.path.join(path, fname))
                    except FileNotFoundError:
                        pass
                    except Exception as e:
                        logger.exception(e)
            if move_composed_path is not None:
                if not os.path.isdir(move_composed_path) and force:
                    try:
                        os.makedirs(move_composed_path)
                    except Exception as e:
                        logger.exception(e)
                fname = None
                if os.path.isdir(move_composed_path) and len(files) > 1:
                    fname = f"{files[0].split('.')[0]}_{files[-1].split('.')[0]}.{fmt}"
                elif len(files) == 1:
                    fname = f"{files[0].split('.')[0]}_None.{fmt}"
                if fname is not None:
                    FileConverter.static_store_df(df=df, path=os.path.join(move_composed_path, fname), fmt=fmt)
        if df is not None:
            df = df.reset_index(drop=True)
        return df

    @staticmethod
    def static_store_df(df: pd.DataFrame, path: str, fmt: str) -> bool:
        """
        Stores a DataFrame to a file

        Args:
            df: The DataFrame to store to a file
            path: The path to the file the DataFrame should be stored in
            fmt:

                * `fmt="parquet"` (recommended) stores the DataFrame using parquet format
                * `fmt="json"` stores the DataFrame using JSON format
                * `fmt="pickle"` stores the DataFrame using pickle format
                * `fmt="csv"` stores the DataFrame using csv format

        Returns:
            True if successful, else False
        """
        if fmt == "pickle":
            return FileConverter.pickle_store(path, df)
        elif fmt == "json":
            return FileConverter.json_store(path, df)
        elif fmt == "csv":
            return FileConverter.csv_store(path, df)
        elif fmt == "parquet":
            return FileConverter.parquet_store(path, df)
        return False

    @staticmethod
    def pickle_store(path: str, df: pd.DataFrame, force: bool=True) -> bool:
        """
        Stores a DataFrame as a .pickle file

        Args:
            path: The path to store the DataFrame to
            df: The DataFrame to store
            force: If the directories in the path do not exist, forces them to be created

        Returns:
            True if successful, otherwise False
        """
        try:
            if FileConverter.store_valid(path, force):
                df = df.reset_index(drop=True)
                df.to_pickle(path)
                return True
        except Exception as e:
            logger = get_logger()
            logger.exception(e)
        return False

    @staticmethod
    def json_store(path: str, df: pd.DataFrame, force: bool=True) -> bool:
        """
        Stores a DataFrame as a .json file

        Args:
            path: The path to store the DataFrame to
            df: The DataFrame to store
            force: If the directories in the path do not exist, forces them to be created

        Returns:
            True if successful, otherwise False
        """
        try:
            if FileConverter.store_valid(path, force):
                df.to_json(path, orient="table", index=False)
                return True
        except Exception as e:
            logger = get_logger()
            logger.exception(e)
        return False

    @staticmethod
    def csv_store(path: str, df: pd.DataFrame, force: bool=True) -> bool:
        """
        Stores a DataFrame as a .csv file

        Args:
            path: The path to store the DataFrame to
            df: The DataFrame to store
            force: If the directories in the path do not exist, forces them to be created

        Returns:
            True if successful, otherwise False
        """
        try:
            if FileConverter.store_valid(path, force):
                df.to_csv(path, index=False)
                return True
        except Exception as e:
            logger = get_logger()
            logger.exception(e)
        return False

    @staticmethod
    def parquet_store(path: str, df: pd.DataFrame, force: bool=True) -> bool:
        """
        Stores a DataFrame as a .parquet file

        Args:
            path: The path to store the DataFrame to
            df: The DataFrame to store
            force: If the directories in the path do not exist, forces them to be created

        Returns:
            True if successful, otherwise False
        """
        try:
            if FileConverter.store_valid(path, force):
                df.to_parquet(path, index=False)
                return True
        except Exception as e:
            logger = get_logger()
            logger.exception(e)
        return False

    @staticmethod
    def store_valid(path: str, force: bool) -> bool:
        """
        Determines if a provided path for storage is valid. I.e. The directory structure exists

        Args:
            path: The path to check
            force: When True, will attempt to create necessary directories if they do not exist

        Returns:
             True if path is valid, otherwise False
        """
        dir_path = os.path.join(*list(os.path.split(path))[0:-1])
        if os.path.isdir(dir_path):
            return True
        else:
            if force:
                try:
                    os.makedirs(dir_path)
                    return True
                except Exception as e:
                    logger = get_logger()
                    logger.exception(e)
        return False

    @staticmethod
    def static_load_df(path: str, fmt: str, dtypes: Optional[dict]=None) -> Union[pd.DataFrame, None]:
        """
        Loads a DataFrame from a file

        Args:
            path: The path to the file the DataFrame should be loaded from
            fmt:

                * `fmt="parquet"` (recommended) loads the DataFrame using parquet format
                * `fmt="json"` loads the DataFrame using JSON format
                * `fmt="pickle"` loads the DataFrame using pickle format
                * `fmt="csv"` loads the DataFrame using csv format

            dtypes: The dtypes to cast the DataFrame to before returning it. I.e. DataFrame.astype(dtypes)

        Returns:
            Loaded DataFrame if successful, otherwise None
        """
        df = None
        if fmt == "pickle":
            df = FileConverter.pickle_load(path)
        elif fmt == "json":
            df = FileConverter.json_load(path)
        elif fmt == "csv":
            df = FileConverter.csv_load(path)
        elif fmt == "parquet":
            df = FileConverter.parquet_load(path)
        if df is not None and dtypes is not None:
            try:
                df = df.astype(dtypes)
            except Exception as e:
                logger = get_logger()
                logger.exception(e)
                df = None
        if df is not None:
            df = df.reset_index(drop=True)
        return df

    @staticmethod
    def pickle_load(path: str) -> Union[pd.DataFrame, None]:
        """
        Loads a DataFrame from a .pickle file

        Args:
            path: The path to load the DataFrame from

        Returns:
            The loaded DataFrame if successful, otherwise None
        """
        try:
            if os.path.isfile(path):
                return pd.read_pickle(path)
        except Exception as e:
            logger = get_logger()
            logger.exception(e)

    @staticmethod
    def json_load(path: str) -> Union[pd.DataFrame, None]:
        """
        Loads a DataFrame from a .json file

        Args:
            path: The path to load the DataFrame from

        Returns:
            The loaded DataFrame if successful, otherwise None
        """
        try:
            if os.path.isfile(path):
                return pd.read_json(path, orient="table")
        except Exception as e:
            logger = get_logger()
            logger.exception(e)

    @staticmethod
    def csv_load(path: str) -> Union[pd.DataFrame, None]:
        """
        Loads a DataFrame from a .csv file

        Args:
            path: The path to load the DataFrame from

        Returns:
            The loaded DataFrame if successful, otherwise None
        """
        try:
            if os.path.isfile(path):
                return pd.read_csv(path)
        except Exception as e:
            logger = get_logger()
            logger.exception(e)

    @staticmethod
    def parquet_load(path: str) -> Union[pd.DataFrame, None]:
        """
        Loads a DataFrame from a .parquet file

        Args:
            path: The path to load the DataFrame from

        Returns:
            The loaded DataFrame if successful, otherwise None
        """
        try:
            if os.path.isfile(path):
                return pd.read_parquet(path)
        except Exception as e:
            logger = get_logger()
            logger.exception(e)
