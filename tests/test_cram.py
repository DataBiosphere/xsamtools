#!/usr/bin/env python
import io
import os
import sys
import unittest
import subprocess
import logging

from uuid import uuid4
from typing import List

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.infra import SuppressWarningsMixin  # noqa
from xsamtools import cram  # noqa

log = logging.getLogger(__name__)


class TestCram(SuppressWarningsMixin, unittest.TestCase):
    # TODO: Add blob exists check to xsamtools.gs_utils and conditionally repopulate fixtures if missing
    clean_up: List[str] = []
    cram_gs_path = 'gs://lons-test/ce#5b.cram'
    cram_local_path = os.path.join(pkg_root, 'tests/fixtures/ce#5b.cram')
    cram_v3_local_path = os.path.join(pkg_root, 'tests/fixtures/ce#5b_v3.cram')
    crai_gs_path = 'gs://lons-test/ce#5b.cram.crai'
    crai_local_path = os.path.join(pkg_root, 'tests/fixtures/ce#5b.cram.crai')
    # basically the entire contents of ce#5b.cram
    regions = {
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

    def cram_cli(self, cram_uri, crai_uri):
        output_file = f'{uuid4()}.cram'
        self.clean_up.append(output_file)
        cmd = f'xsamtools cram view --cram {cram_uri} --crai {crai_uri} -C --output {output_file}'
        log.info(f'Now running: {cmd}')
        p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        self.clean_up.append(p.stdout)

        # samtools can't handle gs uris
        if cram_uri == self.cram_gs_path:
            cram_uri = self.cram_local_path
        if crai_uri == self.crai_gs_path:
            crai_uri = self.crai_local_path

        # view the INPUT cram as human readable
        cmd = f'samtools view {cram_uri} -X {crai_uri}'
        log.info(f'Now running: {cmd}')
        p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        input_contents = p.stdout

        # view the OUTPUT cram as human readable
        cmd = f'samtools view {output_file} -X {crai_uri}'
        log.info(f'Now running: {cmd}')
        p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        output_contents = p.stdout
        assert input_contents == output_contents

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
            self.assertEqual(stdout, (self.regions['CHROMOSOME_I']['expected_output'] +
                                      self.regions['CHROMOSOME_V']['expected_output']))

        test_regions = 'CHROMOSOME_I,CHROMOSOME_III,CHROMOSOME_IV'
        with self.subTest(f'Test xsamtools view cram with regions: "{test_regions}"'):
            stdout, stderr = self.cram_view_with_regions(cram, crai, regions=test_regions)
            self.assertEqual(stdout, (self.regions['CHROMOSOME_I']['expected_output'] +
                                      self.regions['CHROMOSOME_III']['expected_output'] +
                                      self.regions['CHROMOSOME_IV']['expected_output']))

        test_regions = 'CHROMOSOME_I,CHROMOSOME_III:1,CHROMOSOME_IV'
        with self.subTest(f'Test xsamtools view cram with regions: "{test_regions}"'):
            stdout, stderr = self.cram_view_with_regions(cram, crai, regions=test_regions)
            self.assertEqual(stdout, (self.regions['CHROMOSOME_I']['expected_output'] +
                                      self.regions['CHROMOSOME_III']['expected_output'] +
                                      self.regions['CHROMOSOME_IV']['expected_output']))

        test_regions = 'CHROMOSOME_I,CHROMOSOME_III,CHROMOSOME_IV:10-1000'
        with self.subTest(f'Test xsamtools view cram with regions: "{test_regions}"'):
            stdout, stderr = self.cram_view_with_regions(cram, crai, regions=test_regions)
            self.assertEqual(stdout, (self.regions['CHROMOSOME_I']['expected_output'] +
                                      self.regions['CHROMOSOME_III']['expected_output'] +
                                      self.regions['CHROMOSOME_IV']['expected_output']))

    def test_cram_view_cli_with_no_regions(self):
        with self.subTest('[CLI] View cram for local files (no regions).'):
            self.cram_cli(self.cram_local_path, self.crai_local_path)

        with self.subTest('[CLI] View cram for gs:// files (no regions).'):
            self.cram_cli(self.cram_gs_path, self.crai_gs_path)

    def test_cram_view_api_with_no_regions(self):
        with self.subTest('[API] View cram for local files (no regions).'):
            self.assert_cram_view_with_no_regions_generates_identical_output(self.cram_local_path, self.crai_local_path)

        with self.subTest('[API] View cram for gs:// files (no regions).'):
            self.assert_cram_view_with_no_regions_generates_identical_output(self.cram_gs_path, self.crai_gs_path)

    def test_cram_view_api_with_regions(self):
        with self.subTest('[API] View cram for local files (regions).'):
            self.run_cram_view_api_with_regions(self.cram_local_path, self.crai_local_path)

        with self.subTest('[API] View cram for gs:// files (regions).'):
            self.run_cram_view_api_with_regions(self.cram_gs_path, self.crai_gs_path)

    def test_read_crai(self):
        self.assertEqual(len(cram.get_crai_indices(self.crai_local_path)), 5)

    def test_decode_itf8_array(self):
        number_of_items_in_the_array = b'\x04'
        # these should be the bytes representations of: [1, 128, 268435456, 2 ** 32 - 1]
        array_items = b'\x01' + b'\x80\x80' + b'\xf1\x00\x00\x00\x00' + b'\xff\xff\xff\xff\xff'

        with self.subTest('Test decoding an itf8 array where "size" is the first byte.'):
            itf8_array_input_stream = io.BytesIO(number_of_items_in_the_array + array_items)
            results = cram.decode_itf8_array(itf8_array_input_stream)
            self.assertEqual(results, [1, 128, 268435456, 2 ** 32 - 1])

        with self.subTest('Test decoding an itf8 array where "size" is explicitly provided.'):
            itf8_array_input_stream = io.BytesIO(array_items)
            results = cram.decode_itf8_array(itf8_array_input_stream, size=4)
            self.assertEqual(results, [1, 128, 268435456, 2 ** 32 - 1])

    def test_encode_decode_itf8(self):
        """
        Tests ITF-8 encoding and decoding functions.

        Encodes and decodes all binary numbers: 0, 1, 2**1, 2**2, 2**3, ... , up until the highest 32-bit unsigned
        integer: 2**32 - 1 (and ensures a few larger numbers error).

        This ensures that the encoding and decoding functions are at least internally consistent, and checks the
        accuracy of a select number of known cases (like that "2" produces b'\x02' produces "2" again).
        """
        for n in range(32):
            for adjust_number in (-1, 0, 1):
                original_integer = (2 ** n) + adjust_number

                # convert the integer to a bytestring, as would be written into a cram file
                num_as_bytes = cram.encode_itf8(original_integer)
                # then wrap it as a ByteIO object to mimic an open file handle to that bytestring
                readable_bytes_as_handle = io.BytesIO(num_as_bytes)
                # ensure that the decoder returns the same number we started with
                decoded_integer = cram.decode_itf8(readable_bytes_as_handle)
                self.assertEqual(original_integer, decoded_integer)

                if original_integer == 1:
                    self.assertEqual(num_as_bytes, b'\x01')
                elif original_integer == 2:
                    self.assertEqual(num_as_bytes, b'\x02')
                elif original_integer == 16:
                    self.assertEqual(num_as_bytes, b'\x10')
                elif original_integer == 128:
                    self.assertEqual(num_as_bytes, b'\x80\x80')
                elif original_integer == 16384:
                    self.assertEqual(num_as_bytes, b'\xc0@\x00')
                elif original_integer == 4194304:
                    self.assertEqual(num_as_bytes, b'\xe0@\x00\x00')
                elif original_integer == 268435456:
                    self.assertEqual(num_as_bytes, b'\xf1\x00\x00\x00\x00')
                elif original_integer == 2147483648:
                    self.assertEqual(num_as_bytes, b'\xf8\x00\x00\x00\x00')

        for i in (2**32, 2**32 + 1, 2**40):
            with self.assertRaises(ValueError):
                cram.encode_itf8(i)
        num_as_bytes = cram.encode_itf8(2 ** 32 - 1)  # this should be the highest 32-bit unsigned int allowed
        self.assertEqual(num_as_bytes, b'\xff\xff\xff\xff\xff')

    def test_encode_decode_ltf8(self):
        """
        Tests LTF-8 encoding and decoding functions.

        Encodes and decodes all binary numbers: 0, 1, 2**1, 2**2, 2**3, ... , up until the highest 64-bit unsigned
        integer: 2**64 - 1 (and ensures a few larger numbers error).

        This ensures that the encoding and decoding functions are at least internally consistent, and checks the
        accuracy of a select number of known cases (like that "2" produces b'\x02' produces "2" again).
        """
        for n in range(63):
            for adjust_number in (-1, 0, 1):
                original_integer = (2 ** n) + adjust_number

                # convert the integer to a bytestring, as would be written into a cram file
                num_as_bytes = cram.encode_ltf8(original_integer)
                # then wrap it as a ByteIO object to mimic an open file handle to that bytestring
                readable_bytes_as_handle = io.BytesIO(num_as_bytes)
                # ensure that the decoder returns the same number we started with
                decoded_integer = cram.decode_ltf8(readable_bytes_as_handle)
                self.assertEqual(original_integer, decoded_integer)

                if original_integer == 1:
                    self.assertEqual(num_as_bytes, b'\x01')
                elif original_integer == 2:
                    self.assertEqual(num_as_bytes, b'\x02')
                elif original_integer == 16:
                    self.assertEqual(num_as_bytes, b'\x10')
                elif original_integer == 128:
                    self.assertEqual(num_as_bytes, b'\x80\x80')
                elif original_integer == 16384:
                    self.assertEqual(num_as_bytes, b'\xc0@\x00')
                elif original_integer == 4194304:
                    self.assertEqual(num_as_bytes, b'\xe0@\x00\x00')
                elif original_integer == 268435456:
                    self.assertEqual(num_as_bytes, b'\xf0\x10\x00\x00\x00')  # different than itf8
                elif original_integer == 2147483648:
                    self.assertEqual(num_as_bytes, b'\xf0\x80\x00\x00\x00')  # different than itf8

        for i in (2**64, 2**64 + 1, 2**80):
            with self.assertRaises(ValueError):
                cram.encode_ltf8(i)
        num_as_bytes = cram.encode_ltf8(2 ** 64 - 1)  # this should be the highest 64-bit unsigned int allowed
        self.assertEqual(num_as_bytes, b'\xff\xff\xff\xff\xff\xff\xff\xff\xff')

    def test_read_cram_file(self):
        """
        Test parsing the file description and first container header for CRAM versions 2.0 and 3.0 respectively.
        """
        self.read_v2_cram_file()
        self.read_v3_cram_file()

    def read_v2_cram_file(self):
        with open(self.cram_local_path, 'rb') as f:
            with self.subTest('Read CRAM file definition.'):
                expected_file_definition = {
                    'cram': 'CRAM',
                    'major_version': 2,
                    'minor_version': 0,
                    'file_id': '-\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
                }
                cram_file_definition = cram.read_fixed_length_cram_file_definition(f)
                self.assertEqual(cram_file_definition, expected_file_definition,
                                 f'{cram_file_definition} is not: {expected_file_definition}')
            # this should immediately follow the CRAM file definition.
            with self.subTest('Read CRAM container header.'):
                expected_container_header = {
                    'length': 10000,
                    'reference_sequence_id': 0,
                    'starting_position': 0,
                    'alignment_span': 0,
                    'number_of_records': 0,
                    'record_counter': 0,
                    'bases': 0,
                    'number_of_blocks': 1,
                    'landmark': [0],
                    'crc_hash': b'\x00\x00\x00\xa7'
                }
                cram_container_header = cram.read_cram_container_header(f)
                self.assertEqual(cram_container_header, expected_container_header,
                                 f'{cram_container_header} is not: {expected_container_header}')

    def read_v3_cram_file(self):
        with open(self.cram_v3_local_path, 'rb') as f:
            with self.subTest('Read CRAM file definition.'):
                expected_file_definition = {
                    'cram': 'CRAM',
                    'major_version': 3,
                    'minor_version': 0,
                    'file_id': '-\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
                }
                cram_file_definition = cram.read_fixed_length_cram_file_definition(f)
                self.assertEqual(cram_file_definition, expected_file_definition,
                                 f'{cram_file_definition} is not: {expected_file_definition}')
            # this should immediately follow the CRAM file definition.
            with self.subTest('Read CRAM container header.'):
                expected_container_header = {
                    'length': 441,
                    'reference_sequence_id': 0,
                    'starting_position': 0,
                    'alignment_span': 0,
                    'number_of_records': 0,
                    'record_counter': 0,
                    'bases': 0,
                    'number_of_blocks': 2,
                    'landmark': [0, 287],
                    'crc_hash': b'\x80\x9b\xd7\xc1'
                }
                cram_container_header = cram.read_cram_container_header(f)
                self.assertEqual(cram_container_header, expected_container_header,
                                 f'{cram_container_header} is not: {expected_container_header}')

if __name__ == '__main__':
    unittest.main()
