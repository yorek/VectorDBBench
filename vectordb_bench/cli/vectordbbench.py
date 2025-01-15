from ..backend.clients.pgvector.cli import PgVectorHNSW
from .cli import cli

cli.add_command(PgVectorHNSW)


if __name__ == "__main__":
    cli()
