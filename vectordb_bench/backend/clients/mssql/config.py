import logging
from abc import abstractmethod
from pydantic import BaseModel, SecretStr, validator
from typing import Optional
from ..api import DBCaseConfig, DBConfig, IndexType, MetricType

log = logging.getLogger(__name__)

class MSSQLConfig(DBConfig):
    server: str
    database: str
    uid: Optional[str] = None
    pwd: Optional[SecretStr] = None
    entraid: Optional[str] = None

    def to_dict(self) -> dict:
        """
        Prepares connection parameters. If entraid is provided, it fetches a token
        manually and returns connection attributes for pyodbc.
        """
        authentication = "SqlPassword"

        # --- Case 1: Standard SQL Authentication ---
        if self.uid is not None and self.pwd is not None:
            log.info("SQL Authentication requested.")
            
            pwd_str = self.pwd.get_secret_value()
            connection_string = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.uid};"
                f"PWD={pwd_str};"
                "LongAsMax=yes;"
                "Connect Timeout=30;"
                "Encrypt=yes;"
                "TrustServerCertificate=Yes"
            )
            return {"connection_string": connection_string, "authentication": authentication}

        # --- Case 2: Entra ID Managed Identity (Manual Token Auth) ---
        log.info("Entra ID requested.")

        if self.entraid is not None and self.entraid.strip().lower() not in ["no", "false", ""]:
            log.info(f"Managed Identity Credential with client_id {self.entraid} requested.")
            authentication = "ManagedIdentityCredential"
        else:
            log.info("Azure CLI Credentials authentication requested.")
            authentication = "AzureCLICredential"

        connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            "LongAsMax=yes;"
            "Connect Timeout=30;"
            "Encrypt=yes;"
            "TrustServerCertificate=Yes"
        )
        
        # 4. Return both the string and the token attributes
        return {
            "connection_string": connection_string,
            "authentication": authentication,
            "principal": self.entraid
        }

    @validator("*")
    def not_empty_field(cls, v: any, field: any):
        if (
            field.name in cls.common_short_configs()
            or field.name in cls.common_long_configs()
            or field.name in ["uid", "pwd", "entraid"]
        ):
            return v
        if isinstance(v, str | SecretStr) and len(v) == 0:
            raise ValueError("Empty string!")
        return v


class MSSQLVectorIndexConfig(BaseModel, DBCaseConfig):
    metric_type: MetricType | None = None

    def parse_metric(self) -> str: 
        if self.metric_type == MetricType.L2:
            return "euclidean"
        elif self.metric_type == MetricType.IP:
            return "dot"
        elif self.metric_type == MetricType.COSINE:
            return "cosine"

        msg = f"Metric type {self.metric_type} is not supported!"
        raise ValueError(msg)

    @abstractmethod
    def index_param(self) -> dict: ...

    @abstractmethod
    def search_param(self) -> dict: ...
    
class MSSQLDISKANNVectorIndexConfig(MSSQLVectorIndexConfig):
    R: int 
    L: int 
    MAXDOP: int
    index: IndexType = IndexType.DISKANN

    def index_param(self) -> dict:
        return {
            "metric": self.parse_metric(),
            "index": self.index.value,
            "R": self.R,
            "L": self.L,
            "MAXDOP": self.MAXDOP,
        }

    def search_param(self) -> dict:
        return {
            "metric": self.parse_metric(),                        
            "index": self.index.value,
            "R": self.R,
            "L": self.L,
            "MAXDOP": self.MAXDOP,
        }

_mssql_case_config = {
    IndexType.DISKANN: MSSQLDISKANNVectorIndexConfig,
}
