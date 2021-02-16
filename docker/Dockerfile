FROM python:3.7-buster

RUN apt-get update  && \
      apt-get install -y --no-install-recommends apt-utils && \
      apt-get install -y \
        git \
        gcc \
        cmake \
        make \
        g++ \
        libgflags-dev \
        libsnappy-dev \
        zlib1g-dev \
        libbz2-dev \
        liblz4-dev \
        libzstd-dev && \
      apt-get autoremove -y && \
      apt-get clean

RUN git clone https://github.com/myrho/python-rocksdb.git --recursive -b sst_file_writer && \
      cd python-rocksdb && \
      python setup.py install


RUN pip install cassandra-driver==3.23.0
