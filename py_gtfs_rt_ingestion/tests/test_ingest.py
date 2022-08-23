from ingest import parse_args


def test_argparse() -> None:
    """
    Test that the custom argparse is working as expected
    """
    filepath = "my/fake/path.json.gz"

    arguments = [
        "--input",
        filepath,
    ]

    event = parse_args(arguments)

    # check that argparse creates a single event correctly
    assert isinstance(event, dict)
    assert len(event["files"]) == 1
    assert event["files"][0] == filepath
