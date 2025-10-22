"""Wrapper around MSSQL"""

import logging
from contextlib import contextmanager
from typing import Any, Generator, Optional, Tuple, Sequence
import time
from datetime import datetime

from ..api import VectorDB, DBCaseConfig
from vectordb_bench.backend.filter import Filter, FilterOp

import pyodbc
import json

import struct
import azure.identity 

log = logging.getLogger(__name__) 

# --- Constants for Token Authentication ---
SQL_COPT_SS_ACCESS_TOKEN = 1256 
SQL_SERVER_TOKEN_SCOPE = "https://database.windows.net/.default"

class MSSQL(VectorDB):    
    
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
        self.access_token = None

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


        log.info(f"Creating vector table '[{self.schema_name}].[{self.table_name}]'...")
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
        log.info(f"Preparing filters: {filters}")
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
       
        # check if token exists and if it expires within the next hour (or is already expired)
        if self.access_token is not None:
            remaining_seconds = self.access_token.expires_on - time.time()
            if remaining_seconds < 300:  # expires within 5 minutes or already expired
                expiration_datetime = datetime.fromtimestamp(self.access_token.expires_on)
                if remaining_seconds <= 0:
                    log.info(f"Token expired on {expiration_datetime.strftime('%Y-%m-%d %H:%M:%S')} ({abs(remaining_seconds):.0f} seconds ago), acquiring a new one.")
                else:
                    log.info(f"Token expires on {expiration_datetime.strftime('%Y-%m-%d %H:%M:%S')} (in {remaining_seconds:.0f} seconds), acquiring a new one.")
                self.access_token = None
    
        if self.access_token is None:
            log.info(f"Acquiring token for authentication...")

            if authentication == "AzureCLICredential":
                log.info("Using Azure CLI Credentials for authentication.")
                credential = azure.identity.AzureCliCredential()

            if authentication == "ManagedIdentityCredential":
                log.info(f"Using Managed Identity Credential with client_id: {self.db_config.get('principal')}")
                credential = azure.identity.ManagedIdentityCredential(client_id=self.db_config.get("principal"))
    
            access_token = credential.get_token(SQL_SERVER_TOKEN_SCOPE)
            log.info("Token acquired successfully.")

            self.access_token = access_token

        token_bytes = self.access_token.token.encode("UTF-16-LE")
        token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)

        cnxn = pyodbc.connect(
            self.db_config.get("connection_string"),
            attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}
        )
        return cnxn 