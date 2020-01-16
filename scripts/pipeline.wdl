workflow PipelineWorkflow {

        File fastq
        File summary
        File quality_stats
        File? seqmatch_ref_database
        String cfg_prefix = "!*"
        File? cfg_tree
        File? cfg_names
        Int threshold = 1
        Int processes = 1


        call glob_files {
                input: pattern=cfg_prefix
        }
        call convertFastqToFasta {
                input:
                        fastq=fastq
        }
        call splitFastaIntoChunks {
                input:
                        fasta=convertFastqToFasta.fasta,
                        chunks=processes
        }
        scatter (fasta in splitFastaIntoChunks.fastas) {
                call classifyWithSeqmatch {
                        input:
                                fasta=fasta,
                                ref=seqmatch_ref_database
                }
        }
        call mergeSeqmatchResults {
                input:
                        files=classifyWithSeqmatch.result
        }
        call parseSeqmatchResult {
                input:
                        seqmatch=mergeSeqmatchResults.result,
                        ref=seqmatch_ref_database
        }
        call classifyWithCentrifuge {
                input:
                        fastq=fastq,
                        ref_files=glob_files.files
        }
        call parseCentrifugeResult {
                input:
                        centrifuge=classifyWithCentrifuge.classifications,
                        tree=cfg_tree,
                        names=cfg_names
        }
        call checkQuality {
                input:
                        summary=summary,
                        fastq=fastq
        }
}

task glob_files {
        String pattern
        command {}
        output {Array[File] files = glob(pattern)}
}

task convertFastqToFasta {
        File fastq
        String name = basename(fastq, '.fastq')

        command {
                seqtk seq -a ${fastq} > ${name}.fasta
        }
        output {
                File fasta = "${name}.fasta"
        }
        runtime {
                docker: "test:0.0"
        }
}

task splitFastaIntoChunks {
        File fasta
        Int chunks
        String name = sub(fasta, '.fasta', '')

        command {
                python << CODE

                with open('${fasta}', 'r') as infile:
                        line_list = [line.strip() for line in infile.readlines()]
                lines_as_pairs = [line_list[i:i+2] for i in range(0, len(line_list), 2)]
                lines_by_file = [lines_as_pairs[i::${chunks}] for i in range(${chunks})]
                for ix, f in enumerate(lines_by_file):
                        filename = '${name}_%s.fasta' % ix
                        with open(filename, 'w+') as outfile:
                                for line in f:
                                        outfile.write('%s\n%s\n' % (line[0], line[1]))

                CODE
        }
        output {
                Array[File] fastas = glob("${name}_*.fasta")
        }
}

task classifyWithSeqmatch {
        File fasta
        File? ref
        String name = basename(fasta, '.fasta')

        command {
                SequenceMatch seqmatch -k 1 ${default="/usr/local/bin/NCBI_RefSeq_16S.fasta" ref} ${fasta} \
                > ${name}.sqm-results.txt
        }
        output {
                File result = "${name}.sqm-results.txt"
        }
        runtime {
                docker: "test:0.0"
        }
}

task mergeSeqmatchResults {
        Array[File] files
        String prefix = basename(files[0], '_0.sqm-results.txt')

        command {
                python << CODE

                files = '${sep="," files}'.split(',')
                all_results = []
                for ix, f in enumerate(files):
                    with open(f) as infile:
                        lines = infile.readlines()
                    if ix == 0:
                        all_results.extend(lines)
                    else:
                        all_results.extend(lines[1:])
                with open('${prefix}.merged-sqm-results.tsv', 'w') as outfile:
                    outfile.write(''.join(all_results))

                CODE
        }
        output {
                File result = "${prefix}.merged-sqm-results.tsv"
        }
}

task parseSeqmatchResult {
        File seqmatch
        File? ref
        String name = basename(seqmatch, '.merged-sqm-results.tsv')

        command {
                python /usr/local/bin/parse_seqmatch.py --seqmatch ${seqmatch} --fasta \
                ${default="/usr/local/bin/NCBI_RefSeq_16S.fasta" ref} > ${name}.collated-sqm-results.tsv
        }
        output {
                File result = "${name}.collated-sqm-results.tsv"
        }
        runtime {
                docker: "test:0.0"
        }
}

task classifyWithCentrifuge {
        File fastq
        Array[File?] ref_files
        Int array_length = length(ref_files)
        Boolean use_default_db = array_length == 0

        String ref = if use_default_db then "centrifuge_database/NCBI_RefSeq_16S" else basename(ref_files[0], '.1.cf')
        String dir = if use_default_db then "/usr/local/bin" else "$(dirname(ref_files[0]))"

        String name = basename(fastq, '.fastq')

        command {
                centrifuge -q -x ${dir}/${ref} -U ${fastq} -S ${name}.cfg-output.tsv \
                --report-file ${name}.cfg-summary.tsv
        }
        output {
                File classifications = "${name}.cfg-output.tsv"
                File summary = "${name}.cfg-summary.tsv"
        }
        runtime {docker: "test:0.0"}
}
task parseCentrifugeResult {
        File centrifuge
        File? tree
        File? names
        String name = basename(centrifuge, '.cfg-output.tsv')

        command {
                python /usr/local/bin/parse_centrifuge.py --centrifuge ${centrifuge} --tree \
                ${default="/usr/local/bin/NCBI_RefSeq_16S.tree" tree} --names \
                ${default="/usr/local/bin/NCBI_RefSeq_16S.names" names} > ${name}.collated-cfg-results.tsv
        }
        output {
                File result = "${name}.collated-cfg-results.tsv"
        }
        runtime {
                docker: "test:0.0"
        }
}
task checkQuality {
        File summary
        File fastq
        String name = basename(fastq, '.fastq')

        command {
               NanoPlot --summary ${summary} -p Nanoplot_${name}_
        }
        output {
              Array[File] images = glob("*.png")
              File stats = "Nanoplot_${name}_NanoStats.txt"
              Array[File] logs = glob("*.log")
              Array[File] htmls = glob(".html")
        }
        runtime {
              docker: "test:0.0"
        }
}
