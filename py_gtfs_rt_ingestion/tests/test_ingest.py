from ingest import parseArgs

def test_argparse():
    """
    Test that the custom argparse is working as expected
    """
    dummy_file_path = 'my/fake/path'
    dummy_output_dir = 'my/fake/output_dir/'

    dummy_arguments = ['--input', dummy_file_path,
                       '--output', dummy_output_dir]
    args = parseArgs(dummy_arguments)

    assert args.input_file == dummy_file_path
    assert args.output_dir == dummy_output_dir

def test_file_conversion():
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    assert True
