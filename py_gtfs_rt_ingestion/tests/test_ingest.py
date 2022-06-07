import os

from ingest import parseArgs

def test_argparse():
    """
    Test that the custom argparse is working as expected
    """
    filepath = 'my/fake/path.json.gz'

    export_bucket = 'fake_dataplatform_springboard'
    error_bucket = 'fake_dataplatform_error'
    archive_bucket = 'fake_dataplatform_archive'

    arguments = ['--input', filepath,
                 '--export', export_bucket,
                 '--archive', archive_bucket,
                 '--error', error_bucket]

    event = parseArgs(arguments)

    # check that argparse assigns env vars correctly
    assert os.environ['EXPORT_BUCKET'] == export_bucket
    assert os.environ['ARCHIVE_BUCKET'] == archive_bucket
    assert os.environ['ERROR_BUCKET'] == error_bucket

    # check that argparse creates the correct event
    assert len(event['files']) == 1
    assert event['files'][0] == filepath
