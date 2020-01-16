import argparse
from datetime import datetime
import glob
import json
import logging
import os
from pathlib import Path
from collections import defaultdict
import re
import shutil
from subprocess import Popen, PIPE
from data_transfer import checksum
from create_pdf import write_results_to_pdf
# note: "run_pipeline" function also requires installed module "cromwell"


def argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--prefix', action='store', required=True)
    parser.add_argument('--fastq', action='store', required=True)
    parser.add_argument('--sequencing_summary', action='store', required=True)
    parser.add_argument('--quality_stats', action='store', required=True)
    parser.add_argument('--seqmatch_ref', action='store')
    parser.add_argument('--centrifuge_tree', action='store')
    parser.add_argument('--centrifuge_names', action='store')
    parser.add_argument('--centrifuge_index', action='store')
    parser.add_argument('--threshold', action='store', default=0)
    parser.add_argument('--processes', action='store', default=1)
    return parser.parse_args()


def create_log_file(filename):
    """Creates logger object for writing to log file."""
    formatter = logging.Formatter(
        '%(levelname)s\t%(asctime)s\t%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler = logging.FileHandler(filename)
    handler.setFormatter(formatter)
    logger = logging.getLogger('main_logger')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger


def collate_std_files(std_files, filename):
    outputs = defaultdict(set)
    for f in std_files:
        task_regex = re.search(r'call-([a-zA-z]+)', f)
        if task_regex:
            task_name = task_regex.group(1)
            with open(f) as infile:
                content = infile.read()
            if not content:
                content = 'None\n'
            outputs[task_name].add(content)
    with open(filename, 'w') as outfile:
        for k, v in outputs.items():
            outfile.write('Task: %s\n' % k)
            for message in v:
                outfile.write('%s' % message)
            outfile.write('\n')


def tidy_up_pipeline_files(prefix, input_files=None, log_files=None):
    """
    """

    # create output directories
    os.mkdir(prefix)
    output_files_dir = os.path.join(prefix, '%s_classifications' % prefix)
    log_files_dir = os.path.join(prefix, '%s_logs' % prefix)
    input_files_dir = os.path.join(prefix, '%s_inputs' % prefix)
    nanoplot_dir = os.path.join(prefix, '%s_nanoplot' % prefix)
    intermediate_files_dir = os.path.join(prefix, '%s_intermediate_files' % prefix)
    for d in [output_files_dir, log_files_dir, input_files_dir, nanoplot_dir, intermediate_files_dir]:
        os.mkdir(d)

    pdf_file = list(Path('.').rglob('*.pdf'))
    for f in pdf_file:
        new_f = os.path.join(prefix, str(f).split('/')[-1])
        shutil.move(str(f), new_f)

    # move *collated*tsv files to output files dir and all other *tsv files to intermediate files dir
    output_files = list(Path('.').rglob('*.tsv'))
    for f in output_files:
        f = str(f)
        filename = f.split('/')[-1]
        if 'collated' in f:
            new_f = os.path.join(output_files_dir, filename)
        else:
            new_f = os.path.join(intermediate_files_dir, filename)
        shutil.move(f, new_f)

    nanoplot_files = list(Path('.').rglob('Nanoplot_*'))
    for f in nanoplot_files:
        f = str(f)
        filename = f.split('/')[-1]
        if 'report' in filename:
            new_f = os.path.join(prefix, filename.replace('Nanoplot_', ''))
        else:
            new_f = os.path.join(nanoplot_dir, filename)
        shutil.move(f, new_f)

    # extra png files for pdf
    for f in glob.glob('*.png'):
        shutil.move(f, os.path.join(intermediate_files_dir, f))

    # gather all stdout and stderr files and combine into one file for each and save to logs dir
    stdout_files = [str(f) for f in list(Path(prefix).rglob('*stdout'))]
    stderr_files = [str(f) for f in list(Path(prefix).rglob('*stderr'))]
    stdout_filename = os.path.join(log_files_dir, '%s.cromwell-tasks.stdout' % prefix)
    stderr_filename = os.path.join(log_files_dir, '%s.cromwell-tasks.stderr' % prefix)
    collate_std_files(stdout_files, stdout_filename)
    collate_std_files(stderr_files, stderr_filename)

    # move extra log files into logs dir
    for f in log_files:
        shutil.move(f, log_files_dir)

    # move pipeline input files to inputs dir
    for f in input_files:
        shutil.move(f, input_files_dir)

    # remove cromwell working files
    for d in glob.glob('cromwell-*'):
        shutil.rmtree(d)


def run_pipeline(prefix, fastq, summary, stats, threshold, processes, sqm_ref=None, cfg_idx=None, cfg_tree=None,
                 cfg_names=None):
    """
    """
    log_filename = 'Pipeline-%s-%s.log' % (datetime.now().strftime('%Y%m%d'), prefix)
    main_logger = create_log_file(log_filename)

    try:
        main_logger.info('Processing started.')
        inputs_file = '%s.inputs.json' % prefix
        pipeline_script = os.path.join(Path(__file__).resolve().parents[0], 'pipeline.wdl')

        # check fastq MD5
        md5_match = checksum(stats, fastq)
        if md5_match:
            main_logger.info('MD5 values match.')
        else:
            raise RuntimeError('MD5 values do not match.')

        # create cromwell inputs.json file
        wdl_parameters = {
            'PipelineWorkflow.fastq': fastq,
            'PipelineWorkflow.summary': summary,
            'PipelineWorkflow.quality_stats': stats,
            'PipelineWorkflow.threshold': threshold,
            'PipelineWorkflow.processes': processes,
        }
        if sqm_ref:
            wdl_parameters['PipelineWorkflow.seqmatch_ref_database'] = sqm_ref
        if cfg_idx:
            wdl_parameters['PipelineWorkflow.cfg_prefix'] = '%s*' % cfg_idx
        if cfg_tree:
            wdl_parameters['PipelineWorkflow.cfg_tree'] = cfg_tree
        if cfg_names:
            wdl_parameters['PipelineWorkflow.cfg_names'] = cfg_names
        with open(inputs_file, 'w') as inputs_json:
            json.dump(wdl_parameters, inputs_json)

        # run the pipeline using subprocess
        main_logger.info('Pipeline starting.')
        cromwell_cmd = ['cromwell', 'run', '-i', inputs_file, pipeline_script]
        sp = Popen(cromwell_cmd, stdout=PIPE, stderr=PIPE)
        stdout, stderr = sp.communicate()
        main_logger.info('Pipeline complete.')

        # save cromwell stdout to file
        pipeline_log_filename = '%s.cromwell.log' % prefix
        with open(pipeline_log_filename, 'wb') as pipeline_log:
            for std in [stdout, stderr]:
                pipeline_log.write(std)

        # create PDF report - unable to get ete3 tree to work properly inside docker container.
        pdf_report = '%s.pdf' % prefix
        sqm_outputs = list(Path('.').rglob('*collated-sqm-results.tsv'))
        cfg_outputs = list(Path('.').rglob('*collated-cfg-results.tsv'))
        if sqm_outputs and cfg_outputs:
            print(pdf_report, sqm_outputs[0], cfg_outputs[0], stats, threshold)
            write_results_to_pdf(outfile=pdf_report, seqmatch_output=sqm_outputs[0], centrifuge_output=cfg_outputs[0],
                                 stats_file=stats, threshold=threshold)

        # tidy up working files
        tidy_up_pipeline_files(
            prefix=prefix, input_files=[fastq, summary, stats, inputs_file], log_files=[pipeline_log_filename])

        main_logger.info('Processing completed.')

    except Exception:
        main_logger.error('Exception occurred.', exc_info=True)

    if os.path.isdir(prefix):
        for d in os.listdir(prefix):
            if 'logs' in d:
                shutil.move(log_filename, os.path.join(prefix, d))


if __name__ == '__main__':
    args = argument_parser()
    run_pipeline(
        prefix=args.prefix,
        fastq=args.fastq,
        summary=args.sequencing_summary,
        stats=args.quality_stats,
        threshold=args.threshold,
        processes=args.processes,
        sqm_ref=args.seqmatch_ref,
        cfg_idx=args.centrifuge_index,
        cfg_tree=args.centrifuge_tree,
        cfg_names=args.centrifuge_names
    )
