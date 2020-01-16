time_intervals = [2]
archive_hours = 72
setting = 'local'  # "local" or "cloud"
result_threshold = 1.0


class Gridion(object):
    username = 'grid'
    ip_address = '10.161.19.249'
    sshpass_file = '/data/passwd.txt'
    log_files = '/data/16S-Pipeline/test/log_files'
    sequencing_output = '/data'
    processed_files = '/data/16S-Pipeline/test/pipeline_input'
    output_files = '/data/16S-Pipeline/test/pipeline_output'
    run_stats = '/home/grid/Desktop/Flongle_QC_Stats.ods'


class Workstation(object):
    processes = 20
    username = 'nanopore'
    ip_address = '10.161.19.235'
    pipeline_input = '/home/nanopore/Desktop/16S_Pipeline/test/pipeline_input'
    pipeline_output = '/home/nanopore/Desktop/16S_Pipeline/test/pipeline_output'
    sshpass_file = '/home/nanopore/Desktop/16S_Pipeline/passwd.txt'


class Dnanexus(object):
    ua = '/home/grid/Desktop/dnanexus-upload-agent-1.5.30-linux/ua'
    project = 'FK8Xq5j0kXKZ531YGPbFqfzV'
    base_folder = 'Phase3_Pilot'
    api_token = 'xxZczMsFBmLfPr4IfoTcQb1K3AO40QlZ'  # expires 25/11/2020
