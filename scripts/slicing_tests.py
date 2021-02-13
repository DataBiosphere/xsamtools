#!/usr/bin/env python3
import os
import sys
import time

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from xsamtools import cram  # noqa


cram_crai_pairs = [
    ('gs://lons-test/NWD938777.b38.irc.v1.cram', 'gs://lons-test/NWD938777.b38.irc.v1.cram.crai'),
    ('gs://lons-test/ce#5b.cram', 'gs://lons-test/ce#5b.cram.crai')
]

uris = []
for cram_gs_path, crai_gs_path in cram_crai_pairs:
    for slicing_bool in [True]:
        if cram_gs_path == 'gs://lons-test/ce#5b.cram':
            regions = 'CHROMOSOME_I', 'CHROMOSOME_II', 'CHROMOSOME_I:100,CHROMOSOME_II', 'CHROMOSOME_IV', None
        elif cram_gs_path == 'gs://lons-test/NWD938777.b38.irc.v1.cram':
            regions = 'chr1', 'chr2', 'chr1:100,chr2', 'chr23', None
        else:
            print(f'Add the regions for this cram: {cram_gs_path}.  Skipping... ')
            regions = []

        for region in regions:
            print(f'Now running: {cram_gs_path} {crai_gs_path} {region} w/slicing={slicing_bool}')
            start = time.time()
            cram_output = cram.view(cram=cram_gs_path, crai=crai_gs_path, regions=region, cram_format=True,
                                    slicing=slicing_bool)
            end = time.time()
            print(f'Timing was {(end - start) / 60} minutes.')
            print('=' * 40)
