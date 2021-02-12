#!/usr/bin/env python3
import os
import sys
import time

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from xsamtools import cram  # noqa


cram_crai_pairs = [
    ('gs://lons-test/NWD938777.b38.irc.v1.cram', 'gs://lons-test/NWD938777.b38.irc.v1.cram.crai')
    # ('gs://lons-test/ce#5b.cram', 'gs://lons-test/ce#5b.cram.crai')
]

os.makedirs(os.path.join(pkg_root, 'test-cram-slicing'), exist_ok=True)

def output(filename):
    return os.path.join(pkg_root, f'test-cram-slicing/{os.path.basename(filename)}')


uris = []
for cram_gs_path, crai_gs_path in cram_crai_pairs:
    local_cram_path = output(cram_gs_path)
    if not os.path.exists(local_cram_path):
        print(f'Now downloading for local testing: {cram_gs_path}')
        cram.download_full_gs(gs_path=cram_gs_path, output_filename=output(cram_gs_path))
        print('Download complete!')

    local_crai_path = output(crai_gs_path)
    if not os.path.exists(local_crai_path):
        print(f'Now downloading for local testing: {crai_gs_path}')
        cram.download_full_gs(gs_path=crai_gs_path, output_filename=output(crai_gs_path))
        print('Download complete!')

    uris = (local_cram_path, local_crai_path), (cram_gs_path, crai_gs_path)
    print(f'Beginning tests for: {uris}')
    for cram_uri, crai_uri in uris:
        for slicing_bool in True, False:

            if cram_gs_path == 'gs://lons-test/ce#5b.cram':
                regions = 'CHROMOSOME_I', 'CHROMOSOME_II', 'CHROMOSOME_I:100,CHROMOSOME_II', 'CHROMOSOME_IV', None
            elif cram_gs_path == 'gs://lons-test/NWD938777.b38.irc.v1.cram':
                regions = 'chr1', 'chr2', 'chr1:100,chr2', 'chr23', None
            else:
                print(f'Add the regions for this cram: {cram_gs_path}.  Skipping... ')
                regions = []

            for region in regions:
                print(f'Now running: {cram_uri} {crai_uri} {region} w/slicing={slicing_bool}')
                start = time.time()
                cram_output = cram.view(cram=cram_uri, crai=crai_uri, regions=region, cram_format=True,
                                        slicing=slicing_bool)
                end = time.time()
                print(f'Timing was {(end - start) / 60} minutes.')
                print('=' * 40)