"""Wrapper around MSSQL"""

import logging
from contextlib import contextmanager
from typing import Any, Generator, Optional, Tuple
import time
from datetime import datetime
import multiprocessing as mp
import threading
import os
import tempfile

from ..api import VectorDB, DBCaseConfig
from vectordb_bench.backend.filter import Filter, FilterOp

import pyodbc
import json

import struct
import azure.identity

from filelock import FileLock

log = logging.getLogger(__name__) 

# --- Constants for Token Authentication ---
SQL_COPT_SS_ACCESS_TOKEN = 1256 
SQL_SERVER_TOKEN_SCOPE = "https://database.windows.net/.default"

class MSSQL(VectorDB):
    # Use a file-based lock for cross-process synchronization in user's home directory
    _cache_dir = os.path.join(os.path.expanduser("~"), ".vectordbbench")
    _lock_file_path = os.path.join(_cache_dir, "mssql_token_refresh.lock")
    _token_cache_path = os.path.join(_cache_dir, "mssql_token_cache.json")
    _file_lock = None
    _thread_lock = threading.Lock()  # For thread safety within a process
    
    @classmethod
    def _ensure_cache_dir(cls):
        """Ensure the cache directory exists."""
        if not os.path.exists(cls._cache_dir):
            try:
                os.makedirs(cls._cache_dir, mode=0o700)  # Create with restricted permissions
                log.debug(f"Created cache directory: {cls._cache_dir}")
            except Exception as e:
                log.warning(f"Failed to create cache directory: {e}")
    
    @classmethod
    def _get_file_lock(cls):
        """Get or create the file lock instance."""
        if cls._file_lock is None:
            cls._ensure_cache_dir()
            cls._file_lock = FileLock(cls._lock_file_path, timeout=30)
        return cls._file_lock
    
    @classmethod
    def _load_token_from_cache(cls):
        """Load token and expiration time from cache file."""
        try:
            if os.path.exists(cls._token_cache_path):
                with open(cls._token_cache_path, 'r') as f:
                    cache_data = json.load(f)
                    token = cache_data.get('token')
                    expires_on = cache_data.get('expires_on')
                    if token and expires_on:
                        log.debug(f"[Process {mp.current_process().name}] Loaded token from cache, expires at {datetime.fromtimestamp(expires_on).strftime('%Y-%m-%d %H:%M:%S')}")
                        return token, expires_on
        except Exception as e:
            log.warning(f"Failed to load token from cache: {e}")
        return None, None
    
    @classmethod
    def _save_token_to_cache(cls, token: str, expires_on: float):
        """Save token and expiration time to cache file."""
        try:
            cls._ensure_cache_dir()
            cache_data = {
                'token': token,
                'expires_on': expires_on
            }
            # Write to temporary file first, then rename for atomic operation
            temp_path = cls._token_cache_path + '.tmp'
            with open(temp_path, 'w') as f:
                json.dump(cache_data, f)
            # Set restrictive permissions on the cache file (owner read/write only)
            os.chmod(temp_path, 0o600)
            # Atomic rename
            os.replace(temp_path, cls._token_cache_path)
            log.debug(f"[Process {mp.current_process().name}] Saved token to cache, expires at {datetime.fromtimestamp(expires_on).strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            log.warning(f"Failed to save token to cache: {e}")

    supported_filter_types: list[FilterOp] = [
        FilterOp.NonFilter,
        FilterOp.NumGE,
        FilterOp.StrEqual,
    ]

    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config: DBCaseConfig,
        collection_name: str = "vector",
        drop_old: bool = False,
        **kwargs,
    ):
        self.db_config = db_config
        self.case_config = db_case_config
        self.table_name = collection_name + "_" + str(dim)
        self.dim = dim
        self.schema_name = "benchmark"
        self.drop_old = drop_old

        log.info("db_case_config: " + str(db_case_config))

        log.info(f"Connecting to MSSQL...")
        #log.info(self.db_config['connection_string'])
        cnxn = self.connect()
        cursor = cnxn.cursor()

        log.info(f"Creating schema...")
        cursor.execute(f""" 
            if (schema_id('{self.schema_name}') is null) begin
                exec('create schema [{self.schema_name}] authorization [dbo];')
            end;
        """)
        cnxn.commit()
        
        if drop_old:
            log.info(f"Dropping existing table...")
            cursor.execute(f""" 
                drop table if exists [{self.schema_name}].[{self.table_name}]
            """)           
            cnxn.commit()


        log.info(f"Creating vector table '[{self.schema_name}].[{self.table_name}]' if not already there...")
        cursor.execute(f""" 
            if object_id('[{self.schema_name}].[{self.table_name}]') is null begin
                create table [{self.schema_name}].[{self.table_name}] (
                    id int not null primary key clustered,
                    [vector] vector({self.dim}) not null
                )                
            end
        """)
        cnxn.commit()
 
        log.info(f"Dropping old loading vector table type and stored procedure")
        cursor.execute(f"""
            drop procedure if exists stp_load_vectors
            drop type if exists dbo.vector_payload
        """)
        cnxn.commit()
           
        log.info(f"Creating table type...")
        cursor.execute(f""" 
            if type_id('dbo.vector_payload') is null begin
                create type dbo.vector_payload as table
                (
                    id int not null,
                    [vector] vector({self.dim}) not null
                )
            end
        """)
        cursor.commit()

        log.info(f"Creating stored procedure...")
        cursor.execute(f""" 
            create or alter procedure dbo.stp_load_vectors
            @dummy int,
            @payload dbo.vector_payload readonly
            as
            begin
                set nocount on
                insert into [{self.schema_name}].[{self.table_name}] (id, [vector]) select id, [vector] from @payload;
            end
        """)
        cnxn.commit()

        cursor.close()
        cnxn.close()
            
    @contextmanager
    def init(self) -> Generator[None, None, None]:
        self.cnxn = self.connect()
        self.cnxn.autocommit = True
        self.cursor = self.cnxn.cursor()
        try:
            yield
        finally: 
            self.cursor.close()
            self.cnxn.close()
            self.cursor = None
            self.cnxn = None

    def ready_to_load(self):
        log.info(f"MSSQL ready to load")
        pass

    def optimize(self, data_size: int):       
        log.info(f"MSSQL optimize")
        search_param = self.case_config.search_param()
        metric_function = search_param["metric"]
        cursor = self.cursor
        if self.drop_old:
            cursor.execute(f"""            
                if exists(select * from sys.indexes where object_id = object_id('[{self.schema_name}].[{self.table_name}]') and type=8)
                begin
                    drop index vec_idx on [{self.schema_name}].[{self.table_name}];
                end
                """, 
                )
        
        cursor.execute(f"""            
            create vector index vec_idx on [{self.schema_name}].[{self.table_name}]([vector]) with (metric = '{metric_function}', type = 'DiskANN'); 
            """                
            )

    def ready_to_search(self):
        log.info(f"MSSQL ready to search")
        pass


    def prepare_filter(self, filters: Filter):
        #log.debug(f"Preparing filters: {filters}")
        if filters.type == FilterOp.NonFilter:
            self.where_clause = ""
        elif filters.type == FilterOp.NumGE:
            self.where_clause = f"where id >= {filters.int_value}"
        else:
            msg = f"Not support Filter for MSSQL - {filters}"
            raise ValueError(msg)

    def insert_embeddings(
        self,
        embeddings: list[list[float]],
        metadata: list[int],
        **kwargs: Any,
    ) -> Tuple[int, Optional[Exception]]:   
        try:            
            log.info(f'Loading batch of {len(metadata)} vectors...')
            #return len(metadata), None
        
            log.info(f'Generating param list...')
            params = [(metadata[i], json.dumps(embeddings[i])) for i in range(len(metadata))]

            log.info(f'Loading batch...')
            cursor = self.cursor          
            cursor.execute("EXEC dbo.stp_load_vectors @dummy=?, @payload=?", (1, params))     

            log.info(f'Batch loaded successfully.')
            return len(metadata), None
        except Exception as e:
            #cursor.rollback()
            log.warning(f"Failed to insert data into vector table ([{self.schema_name}].[{self.table_name}]), error: {e}")   
            return 0, e
    
    def search_embedding(        
        self,
        query: list[float],
        k: int = 100,        
        timeout: int | None = None,
    ) -> list[int]:        
        search_param = self.case_config.search_param()
        metric_function = search_param["metric"]
        #efSearch = search_param["efSearch"]
        cursor = self.cursor

        cursor.execute(f"""
            declare @v vector({self.dim}) = ?;        
            select 
                t.id
            from
                vector_search(
                    table = [{self.schema_name}].[{self.table_name}] AS t, 
                    column = [vector], 
                    similar_to = @v,
                    metric = '{metric_function}', 
                    top_n = ?
                ) AS s
            {self.where_clause}
            order by
                t.id   
            """, 
            json.dumps(query),      
            k,                                                      
        )
        rows = cursor.fetchall()
        res = [row.id for row in rows]
        return res
        
    def connect(self):
        authentication = self.db_config.get("authentication")

        # --- Case 1: Standard SQL Authentication ---   

        if authentication == "SqlPassword":
            cnxn = pyodbc.connect(
                self.db_config.get("connection_string"),
            )
            return cnxn
        
        # --- Case 2: Entra ID Managed Identity (Manual Token Auth) ---
        
        # Thread-level lock first (for threads within the same process)
        with self._thread_lock:
            # Then file-level lock (for processes) - cross-platform
            file_lock = self._get_file_lock()
            
            with file_lock:
                token_str = self._refresh_token_if_needed(authentication)

        token_bytes = token_str.encode("UTF-16-LE")
        token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)

        cnxn = pyodbc.connect(
            self.db_config.get("connection_string"),
            attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}
        )
        return cnxn
    
    def _refresh_token_if_needed(self, authentication: str) -> str:
        """Check and refresh the access token if needed. Returns the token string."""
        # First, try to load from cache
        cached_token, cached_expires_on = self._load_token_from_cache()
        
        # Check if cached token is valid and not expiring soon
        if cached_token and cached_expires_on:
            remaining_seconds = cached_expires_on - time.time()
            if remaining_seconds >= 300:  # Token has at least 5 minutes left
                log.debug(f"[Process {mp.current_process().name}] Using cached token, {remaining_seconds:.0f} seconds remaining")
                return cached_token
            else:
                # Token is expiring soon or already expired
                expiration_datetime = datetime.fromtimestamp(cached_expires_on)
                if remaining_seconds <= 0:
                    log.info(f"[Process {mp.current_process().name}] Cached token expired on {expiration_datetime.strftime('%Y-%m-%d %H:%M:%S')} ({abs(remaining_seconds):.0f} seconds ago), acquiring a new one.")
                else:
                    log.info(f"[Process {mp.current_process().name}] Cached token expires on {expiration_datetime.strftime('%Y-%m-%d %H:%M:%S')} (in {remaining_seconds:.0f} seconds), acquiring a new one.")
        
        # Need to acquire a new token
        log.info(f"[Process {mp.current_process().name}] Acquiring token for authentication...")

        if authentication == "AzureCLICredential":
            log.info("Using Azure CLI Credentials for authentication.")
            credential = azure.identity.AzureCliCredential()

        if authentication == "ManagedIdentityCredential":
            log.info(f"Using Managed Identity Credential with client_id: {self.db_config.get('principal')}")
            credential = azure.identity.ManagedIdentityCredential(client_id=self.db_config.get("principal"))

        access_token = credential.get_token(SQL_SERVER_TOKEN_SCOPE)
        log.info(f"[Process {mp.current_process().name}] Token acquired successfully, expires at {datetime.fromtimestamp(access_token.expires_on).strftime('%Y-%m-%d %H:%M:%S')}")

        # Save to cache for other processes to use
        self._save_token_to_cache(access_token.token, access_token.expires_on)
        
        return access_token.token 