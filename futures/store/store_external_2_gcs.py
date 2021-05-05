from .store_external import StoreExternal
from google.oauth2 import service_account
from google.cloud import storage
import os


class StoreExternal2Gcs(StoreExternal):

    def __init__(self,
                 name,
                 seconds,
                 path,
                 load_format,
                 auth_path,
                 pid,
                 drop_duplicate_subset=None,
                 move_shards_path=None,
                 location="us",
                 storage_class="STANDARD",
                 move_store_path=None):
        super().__init__(name=name,
                         seconds=seconds,
                         path=path,
                         load_format=load_format,
                         drop_duplicate_subset=drop_duplicate_subset,
                         move_shards_path=move_shards_path)
        self.auth_path = auth_path
        self.pid = pid
        self.location = location
        self.storage_class = storage_class
        self.move_store_path = move_store_path
        if self.load_format != "parquet":
            # TODO: Add additional data format support
            raise NotImplementedError(f"Support for {self.load_format} has not been implemented in api_2_pandas yet")

    def store(self):
        if os.path.isdir(self.path):
            files_to_store = os.listdir(self.path)
            if self.dtypes is None:
                self.dtypes = self.build_dtypes()
            if len(files_to_store) > 1 and self.dtypes is not None:
                ts1 = files_to_store[0].split('.')[0]
                ts2 = files_to_store[-1].split('.')[0]
                blob_name = f"{ts1}_{ts2}.{self.load_format}"
                tmp_path = os.path.join(self.path, f"STORE_{ts1}.{self.load_format}")
                df = self.compose_files(files_to_store)
                if df is not None:
                    self.store_df(tmp_path, df)
                    if os.path.isfile(tmp_path):
                        cred = service_account.Credentials.from_service_account_file(self.auth_path)
                        client = storage.Client(project=self.pid, credentials=cred)
                        bucket = client.bucket(f"{self.name}-store")
                        if not bucket.exists():
                            bucket.storage_class = self.storage_class
                            bucket = client.create_bucket(bucket, location=self.location)
                        else:
                            bucket = client.get_bucket(bucket)
                        blob = bucket.blob(blob_name)
                        blob.upload_from_filename(tmp_path)
                        if self.move_shards_path is None:
                            for file in files_to_store:
                                if os.path.join(self.path, file):
                                    os.remove(os.path.join(self.path, file))
                        else:
                            if not os.path.isdir(self.move_shards_path):
                                os.makedirs(self.move_shards_path)
                            for file in files_to_store:
                                os.rename(os.path.join(self.path, file), os.path.join(self.move_shards_path, file))
                        if self.move_store_path is None and os.path.isfile(tmp_path):
                            os.remove(tmp_path)
                        else:
                            os.rename(tmp_path, self.move_store_path)
                        print(f"INSERTED {len(df)} ROWS INTO {self.pid}.{self.name}-store.{blob_name}")
