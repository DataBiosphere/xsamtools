FROM ubuntu:18.04

RUN apt-get update --quiet \
    && apt-get install --assume-yes --no-install-recommends \
        \
        # Base
        build-essential \
        ca-certificates \
        make \
        cmake \
        moreutils \
        zlib1g-dev \
        gnupg \
        \
        # Utilities
        vim \
        bash-completion \
        git \
        httpie \
        jq \
        zip \
        unzip \
        screen \
        sudo \
        curl \
        wget \
        \
        # htslib deps
        libcurl4-openssl-dev \
        libbz2-dev \
        liblzma-dev \
        # samtools deps
        libncurses5-dev

# Python
RUN apt-get install --assume-yes --no-install-recommends \
        python3.8-dev \
        python3-pip \
    && python3.8 -m pip install --upgrade pip setuptools wheel \
    && update-alternatives --install /usr/bin/python3 python /usr/bin/python3.8 1

# Address locale problem, see "Python 3 Surrogate Handling":
# http://click.pocoo.org/5/python3/
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

# gcloud
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-sdk -y

# Downgrade setuptools for firecloud installation
# https://github.com/broadinstitute/fiss/issues/147
RUN pip3 install --upgrade setuptools==49.6.0

# xsamtools (includes htslib and bcftools)
COPY ${XSAMTOOLS_HOME} xsamtools_repo
RUN (cd xsamtools_repo ; pip3 install .)

# Create a user
ARG XSAMTOOLS_DOCKER_USER
RUN groupadd -g 999 ${XSAMTOOLS_DOCKER_USER} && useradd --home /home/${XSAMTOOLS_DOCKER_USER} -m -s /bin/bash -g ${XSAMTOOLS_DOCKER_USER} -G sudo ${XSAMTOOLS_DOCKER_USER}
RUN bash -c "echo '${XSAMTOOLS_DOCKER_USER} ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers"
USER ${XSAMTOOLS_DOCKER_USER}
WORKDIR /home/${XSAMTOOLS_DOCKER_USER}
ENV PATH /home/${XSAMTOOLS_DOCKER_USER}/bin:${PATH}
