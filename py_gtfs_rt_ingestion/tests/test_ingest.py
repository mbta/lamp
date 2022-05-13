from ingest import parseArgs, run

def test_argparse():
    """
    Test that the custom argparse is working as expected
    """
    dummy_file_path = 'my/fake/path'

    dummy_arguments = ['--fname', dummy_file_path]
    args = parseArgs(dummy_arguments)

    assert args.fname == dummy_file_path

def test_file_conversion():
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    assert True
