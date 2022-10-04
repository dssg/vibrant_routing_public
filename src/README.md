Source code
======

This folder contains our source code.

* For an overview description of the pipeline, see [PIPELINE.md](PIPELINE.md).
* For an in-depth description of the pipeline, see the [Technical Report](https://docs.google.com/document/d/1uZWGOimoM0TWS17_uOM-ilzyMUIOpbWZ/edit#).
* For instructions on how to run the code, see the [main README](../README.md).

## Contents
---
It contains the following documents:

* `utils/`
    * `clear_disk.sh`: cleans the disk by deleting everything that was created during previous pipeline runs.
    * `generate_architecture.sh`: generates the architecture (e.g. creates necesary folders) needed to run the pipeline.
    * `logging_util.py`: utilities that aid in logging what happens at every increment of the pipeline run.
    * `metric_util.py`: utilities that aid in calculating how successful a model performs.
    * `pipeline_util.py`: utilties that aid in running this pipeline.
    * `plot_util.py`: utilties that aid in generating plots.
    * `sql_util.py`: utilties that aid in running sql queries.
    * `util.py`: general utilities.
* `prep/`
    * `data/`
        * `create_routing_id_mapping.sql`: creates center_calls id mapping table.
        * `data_to_database.py`: loads data into the database.
        * `pre_computed_features_creator.py`: generates the pre-computed features (see Technical Report for details about what these features are). This script takes ~6 hours to run.
        * `raw_to_processed.sql`: processes the raw data (e.g. does typecasting) and loads it into the database under the `processed` schema.
        * `run.sh`: runs the above four scripts.
    * `experiment/`
        * `setup_experiments_logging_call_level.sql`: sets up experiments logging for the call-level pipeline.
        * `setup_experiments_logging_routing_level.sql`: sets up experiments logging for the routing-level pipeline.
        * `run.sh`: runs the above two scripts.
* `pipeline/`
    * `call`: call-level pipeline.
    * `routing`: routing-level pipeline.
* `main.py`: main function of the package.
* `PIPELINE.md`: briefly overviews the pipeline.
* `README.md` (this file): contains info about the contents of this folder.