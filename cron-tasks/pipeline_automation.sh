#!/bin/bash

source /home/nanopore/miniconda2/bin/activate 16s-pipeline-env

touch /home/nanopore/Desktop/16S_Pipeline/test/pipeline_heartbeat.txt

python /home/nanopore/test/scripts/pipeline_automation.py >>/home/nanopore/Desktop/16S_Pipeline/test/stdout 2>&1

conda deactivate
