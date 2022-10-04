Docs
======

This is the folder containing docs.

## Contents
---
It contains the following documents:
* `CONVENTION.md`: code style documentation.
* `README.md` (this file): contains info about the contents of this folder.
* `pipeline_workflow_img/`
    * `call-level.png`: diagram depicting the call-level pipeline
    * `overall-flow.png`: diagram depicting both the call-level and routing-level pipelines
    * `routing-simulator.png`: diagram depicting the routing simulator
    * `routing-table-generator.png`: diagram depicting the routing table generator
* `network_visualizations_img/`
    * `generate_center_network_viz.json`: json that can be run through [graphviz](https://graphviz.org/) in order to generate `network_visualization.{svg,png}`.
    * `generate_center_network_viz.sql`: query to generate the `generate_center_network_viz.json`
    * `network_visualization.png`: graph diagram depicting the call-flow relationships between call centers for the one routing table that was provided by Vibrant.
    * `network_visualization.svg`: graph diagram depicting the call-flow relationships between call centers for the one routing table that was provided by Vibrant.