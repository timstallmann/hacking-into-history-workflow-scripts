# Hacking into History Textract -> Zooniverse Scripts

These scripts were created by Tim Stallmann of [Research Action Design, LLC](https://rad.cat) as
part of the [Hacking into History project](www.hackingintohistory.org/) in 2020.

Usage:
* Ensure that panoptes-cli is installed on your system and in the PATH, and has credentials set up
* If https://github.com/zooniverse/panoptes-cli/pull/177 is not yet merged, you will need to patch your local version of panoptes to allow blank filenames for some items
* Run python postgis_to_zooniverse.py <path-to-postgis-csv> <base-output-name> <subject-set-id>

Further info:
Contact tim@rad.cat
