from dataclasses import dataclass
from typing import Dict, List, TypeVar, Union

from lxml.etree import Element, parse

from sds_data_model.constants import TITLE_XPATH, ABSTRACT_XPATH, LINEAGE_XPATH, METADATA_DATE_XPATH, METADATA_LANGUAGE_XPATH, RESOURCE_TYPE_XPATH, FILE_IDENTIFIER_XPATH

MetadataType = TypeVar("MetadataType", bound="Metadata")


def _get_value(root_element: Element, xpath: Union[str, List], namespaces: Dict) -> str:
    if isinstance(xpath, str):
        _xpath = xpath
    elif isinstance(xpath, list):
        _xpath = "/".join(xpath)
    title_element = root_element.xpath(_xpath, namespaces=namespaces)
    return title_element[0].text


@dataclass
class Metadata:
    title: str
    # alternative_title: Optional[List[str]] = field(default_factory=list) #! Optional
    # dataset_language: Tuple[str]
    abstract: str
    # topic_category: Tuple[str]
    # keyword: Tuple[str]
    # temporal_extent: Dict[str, Any]
    # dataset_reference_date: List[str]
    lineage: str
    # extent: Tuple[str] #! Optional
    # vertical_extent_information: Optional[List[str]] = field(default_factory=list) #! Optional
    # spatial_reference_system: List[str]
    # resource_locator: Optional[List[str]] = field(default_factory=list)  #! Conditional
    # data_format: List[str]
    # responsible_organisation: List[str]
    # limitations_on_public_access: List[str]
    # use_constraints: List[str]
    # additional_information: Optional[str] #! Optional
    metadata_date: str
    metadata_language: str
    # metadata_point_of_contact: List[str]
    # resource_identifier: Optional[List[str]] = field(default_factory=list)  #! Conditional
    # spatial_data_service_type: str
    # coupled_resource: Optional[List[str]] = field(default_factory=list)  #! Conditional
    resource_type: str
    # conformity: List[str]
    # equivalent_scale: Optional[List[str]] = field(default_factory=list)  #! Optional
    # bounding_box: List[str]
    file_identifier: str
    # hierarchy_level_name: Optional[str]  #! Conditional
    # quality_scope: List[str]
    # parent_identifier: Optional[str] #! Optional
    # spatial_representation_type: List[str]
    # character_encoding: Optional[List[str]] = field(default_factory=list)  #! Conditional
    # data_quality: Optional[List[str]] = field(default_factory=list)  #! Conditional
    # maintenance_information: Optional[str] #! Optional
    # metadata_standard_name: Optional[str] #! Optional
    # metadata_standard_version: Optional[str] #! Optional

    @classmethod
    def from_file(cls: MetadataType, xml_path: str) -> MetadataType:

        xml = parse(xml_path)

        root_element = xml.getroot()

        namespaces = root_element.nsmap

        title = _get_value(
            root_element=root_element,
            namespaces=namespaces,
            xpath=TITLE_XPATH,
        )

        abstract = _get_value(
            root_element=root_element,
            namespaces=namespaces,
            xpath=ABSTRACT_XPATH,
        )

        lineage = _get_value(
            root_element=root_element,
            namespaces=namespaces,
            xpath=LINEAGE_XPATH,
        )

        metadata_date = _get_value(
            root_element=root_element,
            namespaces=namespaces,
            xpath=METADATA_DATE_XPATH,
        )

        metadata_language = _get_value(
            root_element=root_element,
            namespaces=namespaces,
            xpath=METADATA_LANGUAGE_XPATH,
        )

        resource_type = _get_value(
            root_element=root_element,
            namespaces=namespaces,
            xpath=RESOURCE_TYPE_XPATH,
        )

        file_identifier = _get_value(
            root_element=root_element,
            namespaces=namespaces,
            xpath=FILE_IDENTIFIER_XPATH,
        )

        return cls(
            title=title,
            abstract=abstract,
            lineage=lineage,
            metadata_date=metadata_date,
            metadata_language=metadata_language,
            resource_type=resource_type,
            file_identifier=file_identifier
        )