import re
from collections import defaultdict
import argparse


def argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--seqmatch', action='store', required=True)  # path to merged Seqmatch result
    parser.add_argument('--fasta', action='store', default='/opt/NCBI_RefSeq_16S.fasta')  # reference used for analysis
    return parser


class SeqmatchParser(object):
    """Parses seqmatch results into a dictionary of reads per species.

    Args:
        seqmatch_result_file (str): path to merged Seqmatch result
        refseq_fasta_file (str): reference fasta used for Seqmatch analysis

    Yields:
        ref_dict (dict): fasta file converted into dict where key is transcript id and value is species name.
        classified_reads (int): total number of classified reads
        reads_per_species (dict): key is species name and value is number of reads, e.g. {'Ecoli': 5}

    """
    def __init__(self, seqmatch_result_file, refseq_fasta_file):
        self.sqm_result = seqmatch_result_file
        self.ref = refseq_fasta_file
        self.ref_dict = defaultdict(list)
        self.classified_reads = 0
        self.reads_per_species = defaultdict(int)

    def parse_ref_headers(self):
        """Parses the headers of the reference fasta to populate ref_dict."""
        with open(self.ref, 'r') as infile:
            headers = [l for l in infile.readlines() if l.startswith('>')]
        for header in headers:
            regex = re.findall(r'>([A-Z{2}_[0-9.]+)\s(([A-Z\[][A-Za-z\]]+\s){1,2}[a-z]+)', header)
            if regex:
                for transcript in regex:
                    refseq_id = transcript[0]
                    species_name = transcript[1]
                    self.ref_dict[refseq_id].append(species_name)

    def collate_seqmatch_results(self):
        """Parses Seqmatch result to populate reads_per_species."""
        self.parse_ref_headers()
        with open(self.sqm_result, 'r') as infile:
            results = [l.strip().split('\t') for l in infile.readlines()][1:]
        self.classified_reads = len(results)
        for result in results:
            refseq_id = result[1]
            species_matches = set(self.ref_dict[refseq_id])
            for species in species_matches:
                self.reads_per_species[species] += 1


def print_results(read_count, result_dict):
    """Prints results to stdout."""
    print('Classified reads:\t%s' % read_count)
    for k, v in result_dict.items():
        fraction = float(v) / float(read_count)
        pct = '%.1f' % (fraction * 100)
        print('{species}\t{reads}\t{pct}'.format(species=k, reads=v, pct=pct))


if __name__ == '__main__':
    args = argument_parser().parse_args()
    sqm_parser = SeqmatchParser(args.seqmatch, args.fasta)
    sqm_parser.collate_seqmatch_results()
    print_results(sqm_parser.classified_reads, sqm_parser.reads_per_species)
