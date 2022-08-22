from dataclasses import dataclass
from typing import Dict, List, Tuple, Type, TypeVar, Union

from lxml.etree import Element, parse

from src.sds_data_model.constants import (
    ABSTRACT_XPATH,
    DATASET_LANGUAGE_XPATH,
    FILE_IDENTIFIER_XPATH,
    KEYWORD_XPATH,
    LINEAGE_XPATH,
    METADATA_DATE_XPATH,
    METADATA_LANGUAGE_XPATH,
    QUALITY_SCOPE_XPATH,
    RESOURCE_TYPE_XPATH,
    SPATIAL_REPRESENTATION_TYPE_XPATH,
    TITLE_XPATH,
    TOPIC_CATEGORY_XPATH,
)

MetadataType = TypeVar("MetadataType", bound="Metadata")


def _get_xpath(xpath: Union[str, List[str]]) -> str:
    """Construct an xpath query from a string or list of strings."""
    if isinstance(xpath, str):
        _xpath = xpath
    elif isinstance(xpath, list):
        _xpath = "/".join(xpath)
    return _xpath


def _get_target_elements(
    root_element: Element,
    xpath: Union[str, List[str]],
    namespaces: Dict[str, str],
) -> List[Element]:
    """Get a list of Elements using a xpath query."""
    _xpath = _get_xpath(xpath)
    target_elements: List[Element] = root_element.xpath(_xpath, namespaces=namespaces)
    return target_elements


def _get_value(
    root_element: Element,
    xpath: Union[str, List[str]],
    namespaces: Dict[str, str],
) -> str:
    """Get a single text value from a xpath query."""
    target_elements = _get_target_elements(
        root_element,
        xpath=xpath,
        namespaces=namespaces,
    )
    target_element: str = target_elements[0].strip()
    return target_element


def _get_values(
    root_element: Element,
    xpath: Union[str, List[str]],
    namespaces: Dict[str, str],
) -> Tuple[str, ...]:
    """Get a sequence of text values from a xpath query."""
    target_elements = _get_target_elements(
        root_element,
        xpath=xpath,
        namespaces=namespaces,
    )
    return tuple(target_element.strip() for target_element in target_elements)


@dataclass
class Metadata:
    title: str
    # alternative_title: Optional[List[str]] = field(default_factory=list) #! Optional
    dataset_language: Tuple[str, ...]
    abstract: str
    topic_category: Tuple[str, ...]
    keyword: Tuple[str, ...]
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
    # coupled_resource: Optional[List[str]] = field(default_factory=list)  #! Conditional
    resource_type: str
    # conformity: List[str]
    # equivalent_scale: Optional[List[str]] = field(default_factory=list)  #! Optional
    # bounding_box: List[str]
    file_identifier: str
    # hierarchy_level_name: Optional[str]  #! Conditional
    quality_scope: Tuple[str, ...]
    # parent_identifier: Optional[str] #! Optional
    spatial_representation_type: Tuple[str, ...]
    # character_encoding: Optional[List[str]] = field(default_factory=list)  #! Conditional
    # data_quality: Optional[List[str]] = field(default_factory=list)  #! Conditional
    # maintenance_information: Optional[str] #! Optional
    # metadata_standard_name: Optional[str] #! Optional
    # metadata_standard_version: Optional[str] #! Optional

    @classmethod
    def from_file(cls: Type[MetadataType], xml_path: str) -> MetadataType:

        xml = parse(xml_path)

        root_element = xml.getroot()

        namespaces = root_element.nsmap

        title = _get_value(
            root_element=root_element,
            namespaces=namespaces,
            xpath=TITLE_XPATH,
        )

        dataset_language = _get_values(
            root_element=root_element,
            namespaces=namespaces,
            xpath=DATASET_LANGUAGE_XPATH,
        )

        abstract = _get_value(
            root_element=root_element,
            namespaces=namespaces,
            xpath=ABSTRACT_XPATH,
        )

        topic_category = _get_values(
            root_element=root_element,
            namespaces=namespaces,
            xpath=TOPIC_CATEGORY_XPATH,
        )

        keyword = _get_values(
            root_element=root_element,
            namespaces=namespaces,
            xpath=KEYWORD_XPATH,
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

        quality_scope = _get_values(
            root_element=root_element,
            namespaces=namespaces,
            xpath=QUALITY_SCOPE_XPATH,
        )

        spatial_representation_type = _get_values(
            root_element=root_element,
            namespaces=namespaces,
            xpath=SPATIAL_REPRESENTATION_TYPE_XPATH,
        )

        return cls(
            title=title,
            dataset_language=dataset_language,
            abstract=abstract,
            topic_category=topic_category,
            keyword=keyword,
            lineage=lineage,
            metadata_date=metadata_date,
            metadata_language=metadata_language,
            resource_type=resource_type,
            file_identifier=file_identifier,
            quality_scope=quality_scope,
            spatial_representation_type=spatial_representation_type,
        )
