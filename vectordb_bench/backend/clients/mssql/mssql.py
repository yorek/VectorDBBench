"""Wrapper around MSSQL"""

import logging
from contextlib import contextmanager
from typing import Any

from ..api import VectorDB, DBCaseConfig

import pyodbc
import struct

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
        #log.info(self.db_config['connection_string'])
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
            log.info(f"Dropping existing table...")
            cursor.execute(f""" 
                drop table if exists [{self.schema_name}].[{self.table_name}]
            """)           
            cnxn.commit()

        log.info(f"Creating vector table...")
        cursor.execute(f""" 
            if object_id('[{self.schema_name}].[{self.table_name}]') is null begin
                create table [{self.schema_name}].[{self.table_name}] (
                    id int not null primary key clustered,
                    [vector] varbinary(8000) not null
                )                
            end
        """)
        cnxn.commit()
        
        log.info(f"Creating table type...")
        cursor.execute(f""" 
            if type_id('dbo.vector_payload') is null begin
                create type dbo.vector_payload as table
                (
                    id int not null,
                    [vector] varbinary(8000) not null
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
                insert into [{self.schema_name}].[{self.table_name}] (id, vector) select id, [vector] from @payload;
            end
        """)
        cnxn.commit()

        cursor.close()
        cnxn.close()
            
    @contextmanager
    def init(self) -> None:
        cnxn = pyodbc.connect(self.db_config['connection_string'])     
        self.cnxn = cnxn    
        cnxn.autocommit = True
        self.cursor = cnxn.cursor()
        yield 
        self.cursor.close()
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

    def array_to_vector(self, a:list[float]) -> bytearray:
        # header
        b = bytearray([169, 1])

        # number of items
        b += bytearray(struct.pack("i", len(a)))
        pf = f"{len(a)}f"

        # filler
        b += bytearray([0,0])

        # items
        b += bytearray(struct.pack(pf, *a))

        return b
    
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
            params = [(metadata[i], self.array_to_vector(embeddings[i])) for i in range(len(metadata))]

            log.info(f'Loading table...')
            cursor = self.cursor
            #cursor.fast_executemany = True               
            cursor.execute("EXEC dbo.stp_load_vectors @dummy=?, @payload=?", (1, params))     

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
        search_param = self.case_config.search_param()
        metric_fun = search_param["metric_fun"]
        probes = int(search_param["probes"]), 
        #log.info(f'Query top:{k} metric:{metric_fun} filters:{filters} params: {search_param} timeout:{timeout}...')
        cursor = self.cursor
        if filters:
            cursor.execute(f"""            
                exec [$vector].[stp_filter_similar] @id=?, @v=?, @k=?, @m=?
                """, 
                int(filters.get('id')),
                self.array_to_vector(query), 
                k,                
                metric_fun
                )
        else:
            cursor.execute(f"""            
                exec [$vector].[stp_find_similar] @v=?, @k=?, @m=?
                """, 
                self.array_to_vector(query), 
                k,                
                metric_fun
                )
        rows = cursor.fetchall()
        res = [row.id for row in rows]
        return res
        
        