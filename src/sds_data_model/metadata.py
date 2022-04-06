from abc import ABC
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TypeVar, Type, Tuple, Union
from xml.etree.ElementTree import Element, fromstring
from re import compile
from requests import get
from functools import singledispatch
from typing import List, Union, Tuple
from xml.etree.ElementTree import Element


from sds_data_model.constants import GCO, GMD, GML, GMX, CHARACTER_STRING


@singledispatch
def _qualify_tag(tag) -> str:
    pass


@_qualify_tag.register
def _(tag: tuple) -> str:
    namespace, _tag = tag
    return f"/{namespace}" + _tag


@_qualify_tag.register
def _(tag: str) -> str:
    return f"/{GMD}" + tag


def _create_xpath(tags: List[Union[str, Tuple[str, str]]]) -> str:
    return "." + "".join([_qualify_tag(tag) for tag in tags])


def get_value(
    tree: Element,
    tags: List[Union[str, Tuple[str, str]]],
    final_tag: Union[str, Tuple[str, str]] = None,
) -> str:
    _final_tag = _qualify_tag(final_tag) if final_tag else CHARACTER_STRING
    xpath = _create_xpath(tags)
    return tree.find(xpath).findtext(_final_tag)


def get_tuple_of_values(
    tree: Element,
    tags: List[Union[str, Tuple[str, str]]],
    final_tag: Union[str, Tuple[str, str]] = None,
) -> Tuple[str]:
    _final_tag = _qualify_tag(final_tag).lstrip("/") if final_tag else CHARACTER_STRING
    xpath = _create_xpath(tags)
    return tuple(element.findtext(_final_tag) for element in tree.findall(xpath))


def _remove_namespace(tag: str) -> str:
    if "}" in tag:
        return tag.split("}")[1]
    else:
        return tag


def _element_to_dic(element: Element) -> Dict[str, str]:
    element_name = _remove_namespace(element.tag)
    initial_dictionary = {
        element_name: {},
    }

    if element.attrib:
        for key, value in element.attrib.items():
            initial_dictionary[element_name][_remove_namespace(key)] = value

    empty_element_text = compile(r"^\s+$")
    if not empty_element_text.search(element.text):
        if len(initial_dictionary[element_name]) > 0:
            initial_dictionary[element_name]["text"] = element.text
        else:
            initial_dictionary[element_name] = element.text

    element_children = (
        [_element_to_dic(children) for children in list(element)]
        if len(element) > 0
        else None
    )
    if element_children:
        for child in element_children:
            for key, value in child.items():
                initial_dictionary[element_name][_remove_namespace(key)] = value

    return initial_dictionary


def get_dictionary(
    tree: Element,
    tags: List[Union[str, Tuple[str, str]]],
    final_tag: Union[str, Tuple[str, str]] = None,
) -> Dict[str, Any]:
    tags.append(final_tag)
    xpath = _create_xpath(tags)
    element = tree.find(xpath)
    return _element_to_dic(element)


def _str_from_dict(dictionary: Dict[str, Any], list_of_keys: List[str]) -> str:
    for key in list_of_keys:
        dictionary = dictionary[key]

    return dictionary


MetadataType = TypeVar("MetadataType", bound="Metadata")


@dataclass
class Metadata:
    title: str
    dataset_language: Tuple[str]
    abstract: str
    topic_category: Tuple[str]
    keyword: Tuple[str]
    # temporal_extent: Dict[str, Any]

    @classmethod
    def from_file(cls: MetadataType, path: str) -> MetadataType:
        response = get(path)

        xml = fromstring(response.content)

        title = get_value(
            tree=xml,
            tags=[
                "identificationInfo",
                "MD_DataIdentification",
                "citation",
                "CI_Citation",
                "title",
            ],
        )

        dataset_language = get_tuple_of_values(
            tree=xml,
            tags=[
                "identificationInfo",
                "MD_DataIdentification",
                "language",
            ],
            final_tag="LanguageCode",
        )

        abstract = get_value(
            tree=xml,
            tags=[
                "identificationInfo",
                "MD_DataIdentification",
                "abstract",
            ],
        )

        topic_category = get_tuple_of_values(
            tree=xml,
            tags=[
                "identificationInfo",
                "MD_DataIdentification",
                "topicCategory",
            ],
            final_tag="MD_TopicCategoryCode",
        )

        # TODO INSPIRE theme
        # TODO keywords from 2 controlled vocabularies
        keyword = get_tuple_of_values(
            tree=xml,
            tags=[
                "identificationInfo",
                "MD_DataIdentification",
                "descriptiveKeywords",
                "MD_Keywords",
                "keyword",
            ],
        )

        # temporal_extent = get_dictionary(
        #     tree=xml,
        #     tags=[
        #         "identificationInfo",
        #         "MD_DataIdentification",
        #         "extent",
        #         "EX_Extent",
        #         "temporalElement",
        #         "EX_TemporalExtent",
        #         "extent",
        #     ],
        #     final_tag=(GML, "TimePeriod"),
        # )

        return cls(
            title=title,
            dataset_language=dataset_language,
            abstract=abstract,
            topic_category=topic_category,
            keyword=keyword,
            # temporal_extent=temporal_extent,
        )


# dataset_reference_date: List[str]
# lineage: str
# spatial_reference_system: List[str]
# data_format: List[str]
# responsible_organisation: List[str]
# limitations_on_public_access: List[str]
# use_constraints: List[str]
# metadata_date: str
# metadata_language: str
# metadata_point_of_contact: List[str]
# spatial_data_service_type: str
# resource_type: str
# conformity: List[str]
# bounding_box: List[str]
# file_identifier: str
# quality_scope: List[str]
# spatial_representation_type: List[str]
# additional_information: Optional[str]
# hierarchy_level_name: Optional[str]  # Conditional
# parent_identifier: Optional[str]
# maintenance_information: Optional[str]
# metadata_standard_name: Optional[str]
# metadata_standard_version: Optional[str]
# alternative_title: Optional[List[str]] = field(default_factory=list)
# topic_category: Optional[List[str]] = field(default_factory=list)  # Conditional
# temporal_extent: Optional[List[str]] = field(default_factory=list)
# extent: Optional[List[str]] = field(default_factory=list)
# vertical_extent_information: Optional[List[str]] = field(default_factory=list)
# spatial_resolution: Optional[List[str]] = field(default_factory=list)  # Conditional
# resource_locator: Optional[List[str]] = field(default_factory=list)  # Conditional
# resource_identifier: Optional[List[str]] = field(default_factory=list)  # Conditional
# coupled_resource: Optional[List[str]] = field(default_factory=list)  # Conditional
# equivalent_scale: Optional[List[str]] = field(default_factory=list)  # Conditional
# character_encoding: Optional[List[str]] = field(default_factory=list)  # Conditional
# data_quality: Optional[List[str]] = field(default_factory=list)  # Conditional

# @classmethod
# def from_path(cls: GeminiMetadataType, path: str) -> GeminiMetadataType:

#     response = get(path)

#     xml = fromstring(response.content)

# dictionary = _element_to_dic(tree)

# title = _str_from_dict(
#     dictionary=dictionary,
#     list_of_keys=[
#         "MD_Metadata",
#         "identificationInfo",
#         "MD_DataIdentification",
#         "citation",
#         "CI_Citation",
#         "title",
#         "CharacterString",
#     ],
# )

# #TODO Example two
# alternative_title = get_tuple_of_values(
#     tree=_xml,
#     tags=[
#         "identificationInfo",
#         "MD_DataIdentification",
#         "citation",
#         "CI_Citation",
#         "alternateTitle",
#     ],
# )


# #TODO gml:TimeInstant
# #TODO unknown dates
# #TODO other unknown or unspecified dates
# temporal_extent_xpath = _create_xpath(
#     [
#         "identificationInfo",
#         "MD_DataIdentification",
#         "extent",
#         "EX_Extent",
#         "temporalElement",
#         "EX_TemporalExtent",
#         "extent",
#         (GML, "TimePeriod"),
#         (GML, "*"),
#     ]
# )
# temporal_extent = [
#     (time_period.tag.replace(GML, ""), time_period.text)
#     for time_period in _xml.findall(temporal_extent_xpath)
# ]

# #TODO date and time
# dataset_reference_date_xpath = _create_xpath(
#     [
#         "identificationInfo",
#         "MD_DataIdentification",
#         "citation",
#         "CI_Citation",
#         "date",
#         "CI_Date",
#     ]
# )

# dataset_reference_date = [
#     (
#         element.find(f"{GMD}date").findtext(f"{GCO}Date"),
#         element.find(f"{GMD}dateType").findtext(f"{GMD}CI_DateTypeCode"),
#     )
#     for element in _xml.findall(dataset_reference_date_xpath)
# ]

# lineage = get_value(
#     tree=_xml,
#     tags=[
#         "dataQualityInfo",
#         "DQ_DataQuality",
#         "lineage",
#         "LI_Lineage",
#         "statement",
#     ],
# )

# #TODO Extent encoding example with authority
# extent = get_tuple_of_values(
#     tree=_xml,
#     tags=[
#         "identificationInfo",
#         "MD_DataIdentification",
#         "extent",
#         "EX_Extent",
#         "geographicElement",
#         "EX_GeographicDescription",
#         "geographicIdentifier",
#         "MD_Identifier",
#         "code",
#     ],
#     final_tag=(GMX, "FileName"),  #! Surely some mistake...
# )

# #TODO <gmd:verticalCRS xlink:href='http://www.opengis.net/def/crs/EPSG/0/5701'/>
# #TODO Vertical CRS by value
# #TODO Vertical CRS unknown
# vertical_extent_information_xpath = _create_xpath(
#     [
#         "identificationInfo",
#         "MD_DataIdentification",
#         "extent",
#         "EX_Extent",
#         "verticalElement",
#         "EX_VerticalExtent",
#     ],
# )
# vertical_extent_information = [
#     (vertical_extent_information.tag.replace(GMD, ""), vertical_extent_information.findtext(f"{GCO}Real"))
#     for vertical_extent_information in _xml.findall(vertical_extent_information_xpath)
# ]

# #TODO using gmx:Anchor for a default Coordinate Reference System (as defined in Annex D.4 of the INSPIRE metadata technical guidance v.2)
# #TODO using gmx:Anchor for a non default CRS.
# #TODO encoding example with authority
# #TODO encoding example for spatial reference systems using geographic identifiers
# #! SAC doesn't appear to use any of these examples
# spatial_reference_system = get_tuple_of_values(
#     tree=_xml,
#     tags=[
#         "referenceSystemInfo",
#         "MD_ReferenceSystem",
#         "referenceSystemIdentifier",
#         "RS_Identifier",
#         "code",
#     ],
# )

# spatial_resolution = get_tuple_of_values(
#     tree=_xml,
#     tags=[
#         "identificationInfo",
#         "MD_DataIdentification",
#         "spatialResolution",
#         "MD_Resolution",
#         "distance",
#     ],
#     final_tag=(GCO, "Distance")
# )

# online_resources = _create_xpath(
#     [
#         "distributionInfo",
#         "MD_Distribution",
#         "transferOptions",
#         "MD_DigitalTransferOptions",
#         "onLine",
#         "CI_OnlineResource",
#     ]
# )
# resource_locator = [{child.tag.split("}")[1]: list(child)[0].text for child in list(element)} for element in _xml.findall(online_resources)]

# data_format = [
#     element.find(GMD + "name").findtext(CHARACTER_STRING)
#     for element in _xml.iter(GMD + "MD_Format")
# ]

# return cls(
#     xml=xml,
# title=title,
# alternative_title=alternative_title,
# dataset_language=dataset_language,
# abstract=abstract,
# topic_category=topic_category,
# keyword=keyword,
# temporal_extent=temporal_extent,
# dataset_reference_date=dataset_reference_date,
# lineage=lineage,
# extent=extent,
# vertical_extent_information=vertical_extent_information,
# spatial_reference_system=spatial_reference_system,
# spatial_resolution=spatial_resolution,
# resource_locator=resource_locator,
# data_format=data_format,
# )
