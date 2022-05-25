import os

from ingest import parseArgs

def test_argparse():
    """
    Test that the custom argparse is working as expected
    """
    dummy_file_path = 'my/fake/path'
    dummy_output_dir = 'my/fake/output_dir/'

    dummy_arguments = ['--input', dummy_file_path,
                       '--output', dummy_output_dir]
    event = parseArgs(dummy_arguments)

    assert os.environ['OUTPUT_DIR'] == dummy_output_dir
    assert event['files'][0] == dummy_file_path
