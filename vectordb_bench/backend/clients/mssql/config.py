import logging
from pydantic import BaseModel, SecretStr, validator
from typing import Optional
from ..api import DBConfig, DBCaseConfig, MetricType

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
        if self.uid.strip() != "" and self.pwd is not None:
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
    efSearch: int | None = 48

    def parse_metric(self) -> str: 
        if self.metric_type == MetricType.L2:
            return "euclidean"
        elif self.metric_type == MetricType.IP:
            return "dot"
        return "cosine"
    
    def index_param(self) -> dict:
        return {
            "lists" : self.lists,
            "metric" : self.parse_metric()
        }
    
    def search_param(self) -> dict:
        return {
            "efSearch" : self.efSearch,
            "metric" : self.parse_metric()
        }
