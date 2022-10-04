Config
======

This is the folder containing configuration docs.

None of this folder's contents are scripts that can be run.

## Contents
---
It contains the following documents:
* `data_to_database_config.yaml`: configuration of files to be loaded into the database.
* `data_for_table_generator.py`: data about states, call centers, and area codes used in the table generator.
* `modeling_config.yaml`: configuration of pipeline settings
* `project_constants.py`: delineates different settings for the dev vs normal project environment. 
* `pre_computed_features_config.yaml`: configuration file for the pre-computed features.
* `README.md` (this file): contains info about the contents of this folder.

## dev_ files
---

A subset of these documents have a corresponding `dev` version (indicated by those files that have `dev_` appended at the front).

* `dev_modeling_config.yaml`
* `dev_pre_computed_features_config`

These are abbreviated versions of the original. They were created for the purposes of testing new code during the development phase because this approach 1) reduces the runtime of the pipeline; and 2) preserves the functional components of the normal environment in the case that the development code is buggy.

When the [development mode](../README.md#activating-development-mode) is turned ON, the `dev` versions will automatically be used during runtime. When development mode is turned OFF, the normal versions will will automatically be used.