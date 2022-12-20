"""Tests for the `DataFrameWrapper` pipeline."""

# def test_pipeline(
#     gpdf: GeoDataFrame,
#     tmp_path: Path,
#     expected_HL_array: NDArray[int8],
#     expected_HM_array: NDArray[int8],
# ) -> None:
#     """Returns expected values for grid references HL and HM."""
#     gpkg_path = tmp_path / "test.gpkg"
#     zarr_path = tmp_path / "test.zarr"

#     gpdf.to_file(gpkg_path)

#     pipeline = (
#         VectorLayer.from_files(
#             data_path=gpkg_path,
#             name="test",
#             convert_to_categorical=["category"],
#         )
#         .to_tiles()
#         .to_dataset_as_raster(columns=["category"])
#         .to_zarr(
#             store=zarr_path,
#             mode="w",
#             compute=False,
#         )
#     )
#     with ProgressBar():
#         pipeline.compute()

#     dataset = open_dataset(
#         zarr_path,
#         chunks={
#             "northings": 10_000,
#             "eastings": 10_000,
#         },
#         engine="zarr",
#     )

#     received_HL_array = dataset["category"][range(0, 10_000), range(0, 10_000)].values

#     assert array_equal(
#         a1=received_HL_array,
#         a2=expected_HL_array,
#     )

#     received_HM_array = dataset["category"][
#         range(0, 10_000), range(10_000, 20_000)
#     ].values

#     assert array_equal(
#         a1=received_HM_array,
#         a2=expected_HM_array,
#     )
