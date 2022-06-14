# Hacking into History Textract -> Zooniverse Scripts

These scripts were created by Tim Stallmann of [Research Action Design, LLC](https://rad.cat) as
part of the [Hacking into History project](https://hackingintohistory.org/) in 2020.

Usage:
* Ensure that panoptes-cli is installed on your system and in the PATH, and has credentials set up
* If https://github.com/zooniverse/panoptes-cli/pull/177 is not yet merged, you will need to patch your local version of panoptes to allow blank filenames for some items
* Run python postgis_to_zooniverse.py <path-to-postgis-csv> <base-output-name> <subject-set-id>

## Results processing & extraction from Zooniverse

Using the Python [Aggregation for Caesar](https://github.com/zooniverse/aggregation-for-caesar) package to reduce the raw Zooniverse
extracts into a usable dataset of classifications. This package is installed as a git submodule currently. It seems 
to work best via Docker to avoid pip issues when installing numpy.

Aggregation for caesar pulls data from the `data/` subdirectory

From the `aggregation-for-caesar` directory
* `docker-compose -f docker-compose.hacking-into-history.yml build local_scripts`
* From there, panoptes aggregation commands can be run via:
  * `docker-compose -f docker-compose.hacking-into-history.yml run --rm local_scripts panoptes_aggregation <...>`
  * `docker-compose -f docker-compose.hacking-into-history.yml run --rm local_scripts panoptes_aggregation reduce question_extractor_extractions.csv Reducer_config_workflow_16423_V25.156_question_extractor.yaml`
  * `docker-compose -f docker-compose.hacking-into-history.yml run --rm local_scripts panoptes_aggregation reduce text_extractor_extractions.csv Reducer_config_workflow_16423_V25.156_text_extractor.yaml`

Further info:
Contact tim@rad.cat