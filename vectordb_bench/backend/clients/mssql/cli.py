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
        click.option("--uid", type=str, help="User id", required=True),
    ]
    pwd: Annotated[
        str,
        click.option("--pwd", type=str, help="user password", required=True),
    ]
    metric: Annotated[
        str,
        click.option("--metric", type=str, help="distance metric", required=True),
    ]


@cli.command()
@click_parameter_decorators_from_typed_dict(MSSQLTypedDict)
def MSSQL(**parameters: Unpack[MSSQLTypedDict]):
    from .config import MSSQLConfig, MSSQLVectorIndexConfig

    run(
        db=DB.MSSQL,
        db_config=MSSQLConfig(
            server=parameters["server"],
            database=parameters["database"],
            uid=parameters["uid"],
            pwd=parameters["pwd"],
            metric=parameters["metric"]
        ),
        db_case_config=MSSQLVectorIndexConfig(

        ),
        **parameters,
    )
