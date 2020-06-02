# xsamtools
xsamtools makes the [samtools](https://samtools.github.io/) tooling from [htslib](https://github.com/samtools/htslib)
and [bcftools](https://github.com/samtools/bcftools) available through pypi packaging. These tools have been lightly
modified to allow merges on VCF streams without an index.

# Installation
```
pip install xsamtools
```
Installation requires a C toolchain. Typically Ubuntu/Debian systems should have the following packages installed:
  - `libbz2-dev`
  - `liblzma-dev`
  - `libcurl4-openssl-dev`

`libcurl4-openssl-dev` may be omitted at the cost of some cloud support features in htslib.

# Usage

After succesful installation, the following executables are available:

samtools:
  - htsfile
  - bgzip
  - tabix
  - bcftools

xsamtools:
  - merge_vcfs.py

xsamtools also provides Python tooling to create named (FIFO) pipes to Google Storage objects:
```
from xsamtools import pipes

reader = pipes.BlobReaderProcess("bucket-name", "read-key")
print("reader path", reader.filepath)  # local FIFO filepath

writer_key = pipes.BlobWriterProcess("bucket-name", "writ-key")
print("writer path", writer.filepath)  # local FIFO filepath
```
These streams appear as either readable or writable files on the filesystem. Such objects are not seekable.

A utility method is also provided to merge VCFs from GS objects:
```
from xsamtools import vcf

vcf.combine("src-bucket-name", ["first-src-vcf-key", "second-src-vcf-key"], "dst-bucket-name", "dst-vcf-key")
```
There is no formal limit on the number of VCF keys. Care should be taken that the VCF objects provided are aligned by
chromosome or the merge will fail.

# Docker

A Docker image with xsamtools installed is published at
[https://hub.docker.com/repository/docker/xbrianh/xsamtools](https://hub.docker.com/repository/docker/xbrianh/xsamtools)
