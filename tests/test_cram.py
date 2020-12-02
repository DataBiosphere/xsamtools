#!/usr/bin/env python
import os
import sys
import warnings
import unittest
import subprocess
import logging

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from xsamtools import cram  # noqa
from xsamtools.misc_utils import SubprocessErrorStdError

log = logging.getLogger(__name__)


class TestCram(unittest.TestCase):
    def setUp(self):
        # Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
        warnings.filterwarnings('ignore', 'Your application has authenticated using end user credentials')
        # Suppress unclosed socket warnings
        warnings.simplefilter('ignore', ResourceWarning)

    @classmethod
    def setUpClass(cls) -> None:
        # TODO: Add blob exists check to xsamtools.gs_utils and conditionally repopulate fixtures if missing
        cls.clean_up = []
        cls.cram_gs_path = 'gs://lons-test/ce#5b.cram'
        cls.cram_local_path = os.path.join(pkg_root, 'tests/fixtures/ce#5b.cram')
        cls.crai_gs_path = 'gs://lons-test/ce#5b.cram.crai'
        cls.crai_local_path = os.path.join(pkg_root, 'tests/fixtures/ce#5b.cram.crai')
        # basically the entire contents of ce#5b.cram
        cls.regions = {
            'CHROMOSOME_I': {
                'expected_output':
                    b'I\t16\tCHROMOSOME_I\t2\t1\t27M1D73M\t*\t0\t0\tCCTAGCCCTAACCCTAACCCTAAC'
                    b'CCTAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAA'
                    b'GCCTAA\t#############################@B?8B?BA@@DDBCDDCBC@CDCDCCCCCCCCC'
                    b'CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC\tXG:i:1\tXM:i:5\tXN:i:0\tXO:i:1'
                    b'\tXS:i:-18\tAS:i:-18\tYT:Z:UU\tMD:Z:4A0G5G5G5G3^A73\tNM:i:6\n'
            },
            'CHROMOSOME_II': {
                'expected_output':
                    b'II.14978392\t16\tCHROMOSOME_II\t2\t1\t27M1D73M\t*\t0\t0\tCCTAGCCCTAACC'
                    b'CTAACCCTAACCCTAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAG'
                    b'CCTAAGCCTAAGCCTAA\t#############################@B?8B?BA@@DDBCDDCBC@CD'
                    b'CDCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC\tXG:i:1\tXM:i:5\tXN:'
                    b'i:0\tXO:i:1\tXS:i:-18\tAS:i:-18\tYT:Z:UU\tMD:Z:1T0A4T0A1G2T0A1G2T0A1G2'
                    b'T0A0^A0G0C1T0A1G0C1T0A1G0C1T0A1G0C1T0A1G0C1T0A1G0C1T0A1G0C1T0A1G0C1T0A'
                    b'1G0C1T0A1G0C1T0A1G0C1T0A1G0C1T0A1G0\tNM:i:63\n'
            },
            'CHROMOSOME_III': {
                'expected_output':
                    b'III\t16\tCHROMOSOME_III\t2\t1\t27M1D73M\t*\t0\t0\tCCTAGCCCTAACCCTAACCC'
                    b'TAACCCTAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGC'
                    b'CTAAGCCTAA\t#############################@B?8B?BA@@DDBCDDCBC@CDCDCCCCC'
                    b'CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC\tXG:i:1\tXM:i:5\tXN:i:0\tXO'
                    b':i:1\tXS:i:-18\tAS:i:-18\tYT:Z:UU\tMD:Z:1T0A4T0A1G2T0A1G2T0A1G2T0A0^A0'
                    b'G0C1T0A1G0C1T0A1G0C1T0A1G0C1T0A1G0C1T0A1G0C1T0A1G0C1T0A1G0C1T0A1G0C1T0'
                    b'A1G0C1T0A1G0C1T0A1G0C1T0A1G0\tNM:i:63\n'
            },
            'CHROMOSOME_IV': {
                'expected_output':
                    b'IV\t16\tCHROMOSOME_IV\t2\t1\t27M1D73M\t*\t0\t0\tCCTAGCCCTAACCCTAACCCTA'
                    b'ACCCTAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCT'
                    b'AAGCCTAA\t#############################@B?8B?BA@@DDBCDDCBC@CDCDCCCCCCC'
                    b'CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC\tXG:i:1\tXM:i:5\tXN:i:0\tXO:i'
                    b':1\tXS:i:-18\tAS:i:-18\tYT:Z:UU\tMD:Z:1T0A4T0A1G2T0A1G2T0A1G2T0A0^A0G0'
                    b'C1T0A1G0C1T0A1G0C1T0A1G0C1T0A1G0C1T0A1G0C1T0A1G0C1T0A1G0C1T0A1G0C1T0A1'
                    b'G0C1T0A1G0C1T0A1G0C1T0A1G0\tNM:i:63\n'
            },
            'CHROMOSOME_V': {
                'expected_output':
                    b'V\t16\tCHROMOSOME_V\t2\t1\t27M1D73M\t*\t0\t0\tCCTAGCCCTAACCCTAACCCTAAC'
                    b'CCTAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAAGCCTAA'
                    b'GCCTAA\t#############################@B?8B?BA@@DDBCDDCBC@CDCDCCCCCCCCC'
                    b'CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC\tXG:i:1\tXM:i:5\tXN:i:0\tXO:i:1'
                    b'\tXS:i:-18\tAS:i:-18\tYT:Z:UU\tMD:Z:0A0A1T0C1T0A0A0G0C1T0A0A0G0C1T0A0A'
                    b'0G0C1T0A0A0^G0C0C0T0A0A0G0C0C0T0A0A0G0C0C0T0A0A0G0C0C0T0A0A0G0C0C0T0A0'
                    b'A0G0C0C0T0A0A0G0C0C0T0A0A0G0C0C0T0A0A0G0C0C0T0A0A0G0C0C0T0A0A0G0C0C0T0'
                    b'A0A0G0C0C0T0A0A0G0C0\tNM:i:96\n'
                    b''
                    b'VI\t0\tCHROMOSOME_V\t10\t1\t7S20M1D23M10I30M10S\t*\t0\t0\tAGCCTAAGCCTA'
                    b'AGCCTAAGCCTAAGCTAAGCCTAAGCCTAAGCCTAAGCTTTTTTTTTTCTAAGCCTAAGCCTAAGCCTAA'
                    b'GCCTAAGCCTAAGCCTAA\t*\tMD:Z:0A0G1C0T1A0G1C0T1A0G1C0T1A0G0^C0C0T1A0G1C0'
                    b'T1A0G1C0T1A0G1C0T1A0G1C0T1A0G1C0T1A0G1C0T1A0G1C0T1A0G1C0T1A0G0\tNM:i:6'
                    b'1\n'
                    b''
                    b'VI\t256\tCHROMOSOME_V\t10\t1\t7S20M1D23M10I30M10S\t*\t0\t0\tNNNNNNNAGC'
                    b'CTAAGCCTAAGCCTAAGCTAAGCCTAAGCCTAAGCCTAAGNNNNNNNNNNCCTAAGCCTAAGCCTAAGCC'
                    b'TAAGCCTAAGNNNNNNNNNN\t*\tMD:Z:20^C53\tNM:i:11\n'
            },
            # this one doesn't exist in the file
            'CHROMOSOME_VI': {
                'expected_output':
                    b''
            },
        }

    @classmethod
    def tearDownClass(cls) -> None:
        for file in cls.clean_up:
            if os.path.exists(file):
                os.remove(file)

    def assert_cram_view_with_no_regions_generates_identical_output(self, cram_uri, crai_uri):
        # use samtools to create a cram file from a cram file with no regions specified
        cram_output = cram.view(cram=cram_uri, crai=crai_uri, regions=None, cram_format=True)
        self.clean_up.append(cram_output)

        # view the INPUT cram as human readable
        cmd = f'samtools view {self.cram_local_path} -X {self.crai_local_path}'
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        expected_sam_stdout, _ = p.communicate()

        # view the OUTPUT cram as human readable
        cmd = f'samtools view {cram_output} -X {self.crai_local_path}'
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sam_stdout, _ = p.communicate()

        # check that they are the same and that the length is not zero
        self.assertTrue(expected_sam_stdout == sam_stdout)
        self.assertTrue(len(sam_stdout) > 2000)

        # NOTE: Cannot use: self.assertEqual(os.stat(cram_output).st_size, os.stat(self.test_cram).st_size)
        # The content of the cram files is the same, but the output cram is more deeply compressed.
        # The output file size may continue to change as the samtools version, and the cram spec changes.
        # This check allows us to change samtools versions without significant changes to the test.

    def cram_view_with_regions(self, cram_uri, crai_uri, regions):
        cram_output = cram.view(cram=cram_uri, crai=crai_uri, regions=regions, cram_format=True)
        self.clean_up.append(cram_output)

        # view the OUTPUT cram as human readable
        cmd = f'samtools view {cram_output} -X {self.crai_local_path}'
        log.info(f'Now running: {cmd}')
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sam_stdout, samtools_error = p.communicate()
        return sam_stdout, samtools_error

    def run_cram_view_api_with_regions(self, cram, crai):
        for region in self.regions:
            with self.subTest(f'xsamtools view cram file:// {region}'):
                stdout, stderr = self.cram_view_with_regions(cram, crai, regions=region)
                self.assertEqual(stdout, self.regions[region]['expected_output'])

            # these don't change the output with the normal samtools command?
            # we still need to test them but...
            # TODO: make a better test for these
            for subregion in ['1', '10', '3-100']:
                with self.subTest(f'xsamtools view cram file:// {region}:{subregion}'):
                    stdout, stderr = self.cram_view_with_regions(cram, crai, regions=f'{region}:{subregion}')
                    self.assertEqual(stdout, self.regions[region]['expected_output'])

        test_regions = 'CHROMOSOME_I,CHROMOSOME_V'
        with self.subTest(f'Test xsamtools view cram with regions: "{test_regions}"'):
            stdout, stderr = self.cram_view_with_regions(cram, crai, regions=test_regions)
            self.assertEqual(stdout, self.regions['CHROMOSOME_I']['expected_output'] +
                                     self.regions['CHROMOSOME_V']['expected_output'])

        test_regions = 'CHROMOSOME_I,CHROMOSOME_III,CHROMOSOME_IV'
        with self.subTest(f'Test xsamtools view cram with regions: "{test_regions}"'):
            stdout, stderr = self.cram_view_with_regions(cram, crai, regions=test_regions)
            self.assertEqual(stdout, self.regions['CHROMOSOME_I']['expected_output'] +
                                     self.regions['CHROMOSOME_III']['expected_output'] +
                                     self.regions['CHROMOSOME_IV']['expected_output'])

        test_regions = 'CHROMOSOME_I,CHROMOSOME_III:1,CHROMOSOME_IV'
        with self.subTest(f'Test xsamtools view cram with regions: "{test_regions}"'):
            stdout, stderr = self.cram_view_with_regions(cram, crai, regions=test_regions)
            self.assertEqual(stdout, self.regions['CHROMOSOME_I']['expected_output'] +
                                     self.regions['CHROMOSOME_III']['expected_output'] +
                                     self.regions['CHROMOSOME_IV']['expected_output'])

        test_regions = 'CHROMOSOME_I,CHROMOSOME_III,CHROMOSOME_IV:10-1000'
        with self.subTest(f'Test xsamtools view cram with regions: "{test_regions}"'):
            stdout, stderr = self.cram_view_with_regions(cram, crai, regions=test_regions)
            self.assertEqual(stdout, self.regions['CHROMOSOME_I']['expected_output'] +
                                     self.regions['CHROMOSOME_III']['expected_output'] +
                                     self.regions['CHROMOSOME_IV']['expected_output'])

    def test_cram_view_api_with_no_regions(self):
        with self.subTest('View cram for local files (no regions).'):
            self.assert_cram_view_with_no_regions_generates_identical_output(self.cram_local_path, self.crai_local_path)

        with self.subTest('View cram for gs:// files (no regions).'):
            self.assert_cram_view_with_no_regions_generates_identical_output(self.cram_gs_path, self.crai_gs_path)

    def test_cram_view_api_with_regions(self):
        with self.subTest('View cram for local files (regions).'):
            self.run_cram_view_api_with_regions(self.cram_local_path, self.crai_local_path)

        with self.subTest('View cram for gs:// files (regions).'):
            self.run_cram_view_api_with_regions(self.cram_gs_path, self.crai_gs_path)

    def test_samtools_prints_stderr_exception(self):
        nonexistent_output = 'nonexistent_output'
        self.clean_up.append(nonexistent_output)
        with self.subTest('Assert cram.write_final_file_with_samtools() raises SubprocessErrorStdError.'):
            with self.assertRaises(SubprocessErrorStdError) as e:
                cram.write_final_file_with_samtools('nonexistent_cram', None, None, True, nonexistent_output)
            self.assertEqual(e.exception.returncode, 1)

        with self.subTest('Assert cram.write_final_file_with_samtools() prints useful stderr.'):
            exc = ''
            try:
                cram.write_final_file_with_samtools('nonexistent_cram', None, None, True, nonexistent_output)
            except:
                import traceback
                exc = traceback.format_exc()
            for error_msg in [
                '[E::hts_open_format] Failed to open file "nonexistent_cram" : No such file or directory',
                'samtools view: failed to open "nonexistent_cram" for reading: No such file or directory'
            ]:
                self.assertIn(error_msg, exc, exc)

if __name__ == '__main__':
    unittest.main()
