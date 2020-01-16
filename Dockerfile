FROM continuumio/miniconda3

COPY parse_seqmatch.py /usr/local/bin/parse_seqmatch.py
COPY parse_centrifuge.py /usr/local/bin/parse_centrifuge.py

COPY NCBI_RefSeq_16S.names /usr/local/bin/NCBI_RefSeq_16S.names
COPY NCBI_RefSeq_16S.tree /usr/local/bin/NCBI_RefSeq_16S.tree
COPY NCBI_RefSeq_16S.fasta /usr/local/bin/NCBI_RefSeq_16S.fasta
COPY centrifuge_database /usr/local/bin/centrifuge_database

RUN conda create -n env -c bioconda python=3.6.9 seqtk=1.3 rdptools=2.0.2 centrifuge=1.0.4_beta nanoplot=1.27.0
RUN echo "source activate env" > ~/.bashrc
ENV PATH /opt/conda/envs/env/bin:$PATH

USER 1000:1000

WORKDIR /home
