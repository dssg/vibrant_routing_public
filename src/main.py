import click
from src.pipeline import call_level_pipeline, routing_level_pipeline


@click.command()
@click.option(
    "--run_call_level_pipeline",
    prompt="Would you like to run the call level pipeline?",
    default=False,
)
@click.option(
    "--run_routing_level_pipeline",
    prompt="Would you like to run the routing level pipeline?",
    default=False,
)
def run(run_call_level_pipeline, run_routing_level_pipeline):
    """Function that runs the pipeline.

    Keyword arguments:
        run_call_level_pipeline (bool) -- indicates whether to run call level pipeline.
        run_routing_level_pipeline (bool) -- indicates whether to run routing level pipeline.
    """

    if run_call_level_pipeline:
        call_level_pipeline()

    if run_routing_level_pipeline:
        routing_level_pipeline()


if __name__ == "__main__":
    run()
