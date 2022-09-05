"""Tests for Pyspark Wrapper."""
from pathlib import Path

from sds_data_model.metadata import Wrapper
from chispa.dataframe_comparer import *

expected_data = read_vector_files(
            path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_national_parks/format_SHP_national_parks/LATEST_national_parks/", 
    suffix = ".shp"
        )


actual_data = Wrapper.from_files(name = "National_parks",data_path = "/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_national_parks/format_SHP_national_parks/LATEST_national_parks/",
                                 suffix_glob = ".shp",
                                   metadata_path =  "https://ckan.publishing.service.gov.uk/harvest/object/656c07d1-67b3-4bdb-8ab3-75e118a7cf14")

#check sparks dataframes are the same
assert_df_equality(actual_data.data, expected_data)

# check names are the same
expected_name = "National Parks (England)"
def test_name():
    assert expected_name == actual_data.metadata.title, "Names do not match"
    
test_name()

# read_vector_file does not retain crs information, can either use workaround or wait until that is fixed
#Check Projections is same in gemini metadata as gdal interpretation
#gdal_metadata = "not sure"
#gemini_metadata_crs = ('http://www.opengis.net/def/crs/EPSG/0/27700',)
#actual_crs = actual_data.meta.spatial_reference_system

#def test_crs():
 #   assert actual_crs == gemini_metadata_crs, "Gemini metadata crs does not match"
    
#test_crs()
