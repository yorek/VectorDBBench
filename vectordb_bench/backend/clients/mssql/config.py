from pydantic import BaseModel, SecretStr
from ..api import DBConfig, DBCaseConfig, MetricType

MSSQL_CONNECTION_STRING_PLACEHOLDER="DRIVER={ODBC Driver 18 for SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s;Connect Timeout=30;TrustServerCertificate=Yes"

class MSSQLConfig(DBConfig):
    server: str
    database: str
    uid: str
    pwd: SecretStr    

    def to_dict(self) -> dict:
        pwd_str = self.pwd.get_secret_value()
        return {
            "connection_string" : MSSQL_CONNECTION_STRING_PLACEHOLDER%(self.server, self.database, self.uid, pwd_str)
        }


class MSSQLVectorIndexConfig(BaseModel, DBCaseConfig):
    metric_type: MetricType | None = None
    lists: int | None = 1000
    probes: int | None = 10

    def parse_metric(self) -> str: 
        if self.metric_type == MetricType.L2:
            return "euclidean"
        elif self.metric_type == MetricType.IP:
            return "dot"
        return "cosine"
    
    def parse_metric_fun_str(self) -> str: 
        return self.parse_metric()

    def index_param(self) -> dict:
        return {
            "lists" : self.lists,
            "metric" : self.parse_metric()
        }
    
    def search_param(self) -> dict:
        return {
            "probes" : self.probes,
            "metric_fun" : self.parse_metric_fun_str()
        }