from dataclasses import dataclass
from typing import Dict, List, Tuple, TypeVar, Union

from lxml.etree import Element, parse

from sds_data_model.constants import (
    TITLE_XPATH,
    DATASET_LANGUAGE_XPATH,
    TOPIC_CATEGORY_XPATH,
    KEYWORD_XPATH,
    QUALITY_SCOPE_XPATH,
    SPATIAL_REPRESENTATION_TYPE_XPATH,
)

MetadataType = TypeVar("MetadataType", bound="Metadata")


def _get_xpath(xpath: Union[str, List]) -> str:
    if isinstance(xpath, str):
        _xpath = xpath
    elif isinstance(xpath, list):
        _xpath = "/".join(xpath)
    return _xpath


def _get_target_elements(
    root_element: Element,
    xpath: Union[str, List],
    namespaces: Dict,
) -> str:
    _xpath = _get_xpath(xpath)
    return root_element.xpath(_xpath, namespaces=namespaces)


def _get_text_value(
    root_element: Element,
    xpath: Union[str, List],
    namespaces: Dict,
) -> str:
    target_elements = _get_target_elements(
        root_element,
        xpath=xpath,
        namespaces=namespaces,
    )
    return target_elements[0].text.strip()


def _get_text_values(
    root_element: Element,
    xpath: Union[str, List],
    namespaces: Dict,
) -> Tuple[str, ...]:
    target_elements = _get_target_elements(
        root_element,
        xpath=xpath,
        namespaces=namespaces,
    )
    return tuple(target_element.text.strip() for target_element in target_elements)


def _get_attribute_values(
    root_element: Element,
    xpath: Union[str, List],
    namespaces: Dict,
    attribute: str,
) -> Tuple[str, ...]:
    target_elements = _get_target_elements(
        root_element,
        xpath=xpath,
        namespaces=namespaces,
    )
    return tuple(target_element.get(attribute) for target_element in target_elements)


@dataclass
class Metadata:
    title: str
    # alternative_title: Optional[List[str]] = field(default_factory=list) #! Optional
    dataset_language: Tuple[str]
    # abstract: str
    topic_category: Tuple[str]
    keyword: Tuple[str]
    # temporal_extent: Dict[str, Any]
    # dataset_reference_date: List[str]
    # lineage: str
    # extent: Tuple[str] #! Optional
    # vertical_extent_information: Optional[List[str]] = field(default_factory=list) #! Optional
    # spatial_reference_system: List[str]
    # resource_locator: Optional[List[str]] = field(default_factory=list)  #! Conditional
    # data_format: List[str]
    # responsible_organisation: List[str]
    # limitations_on_public_access: List[str]
    # use_constraints: List[str]
    # additional_information: Optional[str] #! Optional
    # metadata_date: str
    # metadata_language: str
    # metadata_point_of_contact: List[str]
    # resource_identifier: Optional[List[str]] = field(default_factory=list)  #! Conditional
    # spatial_data_service_type: str
    # coupled_resource: Optional[List[str]] = field(default_factory=list)  #! Conditional
    # resource_type: str
    # conformity: List[str]
    # equivalent_scale: Optional[List[str]] = field(default_factory=list)  #! Optional
    # bounding_box: List[str]
    # file_identifier: str
    # hierarchy_level_name: Optional[str]  #! Conditional
    quality_scope: Tuple[str]
    # parent_identifier: Optional[str] #! Optional
    spatial_representation_type: Tuple[str]
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

        title = _get_text_value(
            root_element=root_element,
            namespaces=namespaces,
            xpath=TITLE_XPATH,
        )

        dataset_language = _get_text_values(
            root_element=root_element,
            namespaces=namespaces,
            xpath=DATASET_LANGUAGE_XPATH,
        )

        topic_category = _get_text_values(
            root_element=root_element,
            namespaces=namespaces,
            xpath=TOPIC_CATEGORY_XPATH,
        )

        keyword = _get_text_values(
            root_element=root_element,
            namespaces=namespaces,
            xpath=KEYWORD_XPATH,
        )

        quality_scope = _get_text_values(
            root_element=root_element,
            namespaces=namespaces,
            xpath=QUALITY_SCOPE_XPATH,
        )

        spatial_representation_type =_get_attribute_values(
            root_element=root_element,
            namespaces=namespaces,
            xpath=SPATIAL_REPRESENTATION_TYPE_XPATH,
            attribute="codeListValue",
        )

        return cls(
            title=title,
            dataset_language=dataset_language,
            topic_category=topic_category,
            keyword=keyword,
            quality_scope=quality_scope,
            spatial_representation_type=spatial_representation_type,
        )
