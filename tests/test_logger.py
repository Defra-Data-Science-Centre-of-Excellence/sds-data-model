from sds_data_model.vector import VectorLayer 


# def test_graph(shared_datadir) -> None:
# _data_path = shared_datadir / "Countries_(December_2020)_UK_BUC.zip"

test = VectorLayer.from_files(
    data_path="tests/data/Countries_(December_2020)_UK_BUC.zip",
    data_kwargs={},
    name="test",
)

test