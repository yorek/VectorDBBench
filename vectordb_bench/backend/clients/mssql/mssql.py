"""Wrapper around MSSQL"""

import logging
from contextlib import contextmanager
from typing import Any

from ..api import VectorDB, DBCaseConfig

import pyodbc
import json

log = logging.getLogger(__name__) 

class MSSQL(VectorDB):    
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

        log.info("db_case_config: " + str(db_case_config))

        log.info(f"Connecting to MSSQL...")
        log.info(self.db_config['connection_string'])
        cnxn = pyodbc.connect(self.db_config['connection_string'])     
        cursor = cnxn.cursor()

        log.info(f"Creating schema...")
        cursor.execute(f""" 
            if (schema_id('{self.schema_name}') is null) begin
                exec('create schema [{self.schema_name}] authorization [dbo];')
            end;
        """)
        cnxn.commit()
        
        if drop_old:
            log.info(f"Dropping existing tables...")
            cursor.execute(f""" 
                drop table if exists [{self.schema_name}].[{self.table_name}]
            """)           
            cnxn.commit()

            log.info(f"Creating vector table...")
            cursor.execute(f""" 
                create table [{self.schema_name}].[{self.table_name}] (
                    id int not null primary key nonclustered,
                    [vector] varbinary(8000) not null
                )
            """)
            cnxn.commit()
        
        cursor.close()
        cnxn.close()
            
    @contextmanager
    def init(self) -> None:
        cnxn = pyodbc.connect(self.db_config['connection_string'])     
        self.cnxn = cnxn    
        cnxn.autocommit = False
        yield 
        self.cnxn.close()

    def ready_to_load(self):
        log.info(f"MSSQL ready to load")
        pass

    def optimize(self):
        log.info(f"MSSQL optimize")
        pass

    def ready_to_search(self):
        log.info(f"MSSQL ready to search")
        pass

    def insert_embeddings(
        self,
        embeddings: list[list[float]],
        metadata: list[int],
        **kwargs: Any,
    ) -> (int, Exception):        
        try:            
            log.info(f'Loading batch of {len(metadata)} vectors...')
            #return len(metadata), None
        
            log.info(f'Generating param list...')
            params = [(metadata[i], str(embeddings[i])) for i in range(len(metadata))]

            log.info(f'Loading table...')
            cursor = self.cnxn.cursor()
            cursor.fast_executemany = True   
            cursor.executemany(f"insert into [{self.schema_name}].[{self.table_name}] (id, [vector]) values (?, vector(cast(? as varchar(max))))", params)
            cursor.commit()           

            return len(metadata), None
        except Exception as e:
            #cursor.rollback()
            log.warning(f"Failed to insert data into vector table ([{self.schema_name}].[{self.table_name}]), error: {e}")   
            return 0, e

    def search_embedding(        
        self,
        query: list[float],
        k: int = 100,
        filters: dict | None = None,
        timeout: int | None = None,
    ) -> list[int]:        
        log.info(f'Query {k} {filters} {timeout}...')
        cursor = self.cnxn.cursor()
        cursor.execute(f"""            
            select top({k})
                id,         
                vector_distance('cosine', [vector], vector(cast(? as varchar(max)))) as cosine_similarity
            from
                [{self.schema_name}].[{self.table_name}] v
            order by
                cosine_similarity desc
            """, str(query))
        rows = cursor.fetchall()
        res = [row.id for row in rows]
        return list(res)
        
        