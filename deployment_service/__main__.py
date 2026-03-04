"""CLI entry point for deployment-service."""

import click


@click.group()
@click.version_option(version="0.1.0")
def cli() -> None:
    """Deployment orchestration service."""
    pass


@cli.command()
def status() -> None:
    """Show deployment status."""
    click.echo("Deployment service — status stub")


if __name__ == "__main__":
    cli()
