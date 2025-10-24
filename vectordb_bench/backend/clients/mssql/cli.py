from typing import Annotated, Unpack

import click
from pydantic import SecretStr

from ....cli.cli import (
    CommonTypedDict,
    cli,
    click_parameter_decorators_from_typed_dict,
    run,
)
from .. import DB


class MSSQLTypedDict(CommonTypedDict):
    server: Annotated[
        str, click.option("--server", type=str, help="server url", required=True)
    ]
    database: Annotated[
        str,
        click.option("--database", type=str, help="database name", required=True),
    ]
    uid: Annotated[
        str,
        click.option("--uid", type=str, help="User id", required=False),
    ]
    pwd: Annotated[
        str,
        click.option("--pwd", type=str, help="user password", required=False),
    ]
    entraid: Annotated[
        str,
        click.option("--entraid", type=str, help="Entra Id Authentication", required=False),
    ]
    R: Annotated[
        int,
        click.option("--R", type=int, help="DISKANN R parameter", required=False),
    ]
    L: Annotated[
        int,
        click.option("--L", type=int, help="DISKANN L parameter", required=False),
    ]
    MAXDOP: Annotated[
        int,
        click.option("--MAXDOP", type=int, help="Maximum degree of parallelism", required=False),
    ]

@cli.command()
@click_parameter_decorators_from_typed_dict(MSSQLTypedDict)
def MSSQL(**parameters: Unpack[MSSQLTypedDict]):
    from .config import MSSQLConfig, MSSQLDISKANNVectorIndexConfig

    run(
        db=DB.MSSQL,
        db_config=MSSQLConfig(
            db_label=parameters["db_label"],
            server=parameters["server"],
            database=parameters["database"],
            uid=parameters["uid"],
            pwd=parameters["pwd"],
            entraid=parameters["entraid"]
        ),
        db_case_config=MSSQLDISKANNVectorIndexConfig(
            R=parameters.get("R", 0),
            L=parameters.get("L", 0),
            MAXDOP=parameters.get("MAXDOP", 0)
        ),
        **parameters,
    )
