#!/bin/bash

source /home/grid/miniconda2/bin/activate 16s-pipeline-env

touch /data/16S-Pipeline/test/sequencing_heartbeat.txt

python /opt/scripts/test/scripts/sequencing_automation.py >>/data/16S-Pipeline/test/stdout 2>&1

conda deactivate
