from sds_data_model.metadata import Metadata

expected = Metadata(
    title="Digital Geological Map Data of Great Britain - 625k",
)


def test_from_files(datadir) -> None:
    # Arrange
    metadata_path = datadir / "title.xml"
    # Act
    recieved = Metadata.from_file(
        xml_path=str(metadata_path),
    )
    # Assert
    assert recieved == expected
