"""Metadata parsing module."""
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, TypeVar, Union

from lxml.etree import (  # noqa: S410 - assuming we can trust the XML
    Element,
    ElementTree,
    parse,
)
from requests import get


from sds_data_model.constants import (
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


def _get_xpath(xpath: Union[str, Iterable[str], Iterable[Iterable[str]]]) -> str:
    """Construct an `XPath`_ query from a string, list of strings, or list of lists of strings.

    Examples:
        A list of strings will be concatenated with "/" as the separator. This should produce an XPath query:
        >>> TITLE_XPATH = [
            "gmd:identificationInfo",
            "gmd:MD_DataIdentification",
            "gmd:citation",
            "gmd:CI_Citation",
            "gmd:title",
            "gco:CharacterString",
            "/text()",
        ]
        >>> _get_xpath(TITLE_XPATH)
        'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString//text()'

        Each list in a list of lists of strings will be concatenated with "/" as the separator, then the lists will be concatenated
        with "|" as a separator. This should produce an OR XPath query:
        >>> METADATA_DATE_XPATH = [
            [
                "gmd:dateStamp",
                "gco:Date",
                "/text()",
            ],
            [
                "gmd:dateStamp",
                "gco:DateTime",
                "/text()",
            ],
        ]
        >>> _get_xpath(METADATA_DATE_XPATH)
        'gmd:dateStamp/gco:Date//text()|gmd:dateStamp/gco:DateTime//text()'

        For a string, this is a `no-op`_.
        >>> TITLE_XPATH = 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString//text()'
        >>> _get_xpath(TITLE_XPATH)
        'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString//text()'

    Args:
        xpath (Union[str, List[str]]): A string or list of strings that represent
            an XPath query.

    Returns:
        str: An XPath query.

    .. _XPath:
        https://www.w3.org/TR/xpath-3/

    .. _no-op:
        https://en.wikipedia.org/wiki/NOP_(code)

    """  # noqa: B950 - XPath strings cannot be wrapped
    if isinstance(xpath, str):
        return xpath
    elif isinstance(xpath, list) and isinstance(xpath[0], str):
        return "/".join(xpath)
    else:
        return "|".join("/".join(list_of_str) for list_of_str in xpath)


def _get_target_elements(
    root_element: Element,
    xpath: Union[str, Iterable[str], Iterable[Iterable[str]]],
    namespaces: Dict[str, str],
) -> List[Element]:
    """Get a list of Elements using a `XPath`_ query.

    Examples:
        >>> from lxml.etree import parse
        >>> from sds_data_model.constants import TITLE_XPATH
        >>> xml = parse("tests/test_metadata/ramsar.xml")
        >>> root_element = xml.getroot()
        >>> namespaces = root_element.nsmap
        >>> _get_target_elements(
            root_element=root_element,
            xpath=TITLE_XPATH,
            namespaces=namespaces,
        )
        ['Ramsar (England)']

    Args:
        root_element (Element): The starting point of the XPath query.
        xpath (Union[str, List[str]]): A string or list of strings that represent
            an XPath query.
        namespaces (Dict[str, str]): A dictionary of XML `namespaces`_.

    Returns:
        List[Element]: A list of XML elements returned by the XPath query.

    .. _XPath:
        https://www.w3.org/TR/xpath-3/

    .. _namespaces:
        https://www.w3.org/TR/xml-names/

    """
    _xpath = _get_xpath(xpath)
    target_elements: List[Element] = root_element.xpath(_xpath, namespaces=namespaces)
    return target_elements


def _get_value(
    root_element: Element,
    xpath: Union[str, Iterable[str], Iterable[Iterable[str]]],
    namespaces: Dict[str, str],
) -> str:
    """Get a single text value from a `XPath`_ query.

    Examples:
        >>> from lxml.etree import parse
        >>> from sds_data_model.constants import TITLE_XPATH
        >>> xml = parse("tests/test_metadata/ramsar.xml")
        >>> root_element = xml.getroot()
        >>> namespaces = root_element.nsmap
        >>> _get_value(
            root_element=root_element,
            xpath=TITLE_XPATH,
            namespaces=namespaces,
        )
        'Ramsar (England)'

    Args:
        root_element (Element): The starting point of the XPath query.
        xpath (Union[str, List[str]]): A string or list of strings that represent
            an XPath query.
        namespaces (Dict[str, str]): A dictionary of XML `namespaces`_.

    Returns:
        str: The text value of the first element returned by the XPath query.

    .. _XPath:
        https://www.w3.org/TR/xpath-3/

    .. _namespaces:
        https://www.w3.org/TR/xml-names/

    """
    target_elements = _get_target_elements(
        root_element,
        xpath=xpath,
        namespaces=namespaces,
    )
    target_element: str = target_elements[0].strip()
    return target_element


def _get_values(
    root_element: Element,
    xpath: Union[str, Iterable[str], Iterable[Iterable[str]]],
    namespaces: Dict[str, str],
) -> Tuple[str, ...]:
    """Get a tuple of text values from a `XPath`_ query.

    Examples:
        >>> from lxml.etree import parse
        >>> from sds_data_model.constants import KEYWORD_XPATH
        >>> xml = parse("tests/test_metadata/ramsar.xml")
        >>> root_element = xml.getroot()
        >>> namespaces = root_element.nsmap
        >>> _get_values(
            root_element=root_element,
            xpath=KEYWORD_XPATH,
            namespaces=namespaces,
        )
        ('OpenData', 'NEbatch4', 'Protected sites')

    Args:
        root_element (Element): The starting point of the XPath query.
        xpath (Union[str, List[str]]): A string or list of strings that represent
            an XPath query.
        namespaces (Dict[str, str]): A dictionary of XML `namespaces`_.

    Returns:
        str: A tuple of text values returned by the XPath query.

    .. _XPath:
        https://www.w3.org/TR/xpath-3/

    .. _namespaces:
        https://www.w3.org/TR/xml-names/

    """
    target_elements = _get_target_elements(
        root_element,
        xpath=xpath,
        namespaces=namespaces,
    )
    return tuple(target_element.strip() for target_element in target_elements)


def _get_xml(path: str, metadata_kwargs: Dict[str, Any]) -> ElementTree:
    """Parses XML from remote URL or local file path.

    Examples:
        Parse XML from a remote URL:
        >>> xml = _get_xml("https://ckan.publishing.service.gov.uk/harvest/object/715bc6a9-1008-4061-8783-d12e9e7f38a9")
        >>> xml.docinfo.root_name
        'MD_Metadata'

        Parse XML from a local file path:
        >>> xml = _get_xml("../../tests/test_metadata/ramsar.xml")
        >>> xml.docinfo.root_name
        'MD_Metadata'

    Args:
        path (str): A remote URL or local file path.
        metadata_kwargs (Dict[str, Any]): Key word arguments to be passed to the
            requests `get`_ method when reading xml metadata from a URL.

    Returns:
        ElementTree: an ElementTree representation of the XML.

    .. _get:
        https://requests.readthedocs.io/en/latest/api/#requests.request
    """  # noqa: B950 - URL
    if path.startswith("http"):
        response = get(path, **metadata_kwargs)
        buffered_content = BytesIO(response.content)
        return parse(buffered_content)  # noqa: S320 - assuming we can trust the XML
    else:
        return parse(path)  # noqa: S320 - assuming we can trust the XML


@dataclass
class Metadata:
    """# TODO.

    Returns:
        _type_: # TODO
    """

    title: str
    # alternative_title: Optional[Tuple[str, ...]] #! Optional
    dataset_language: Tuple[str, ...]
    abstract: str
    topic_category: Tuple[str, ...]
    keyword: Tuple[str, ...]
    # temporal_extent: Dict[str, Any]
    # dataset_reference_date: List[str]
    lineage: str
    # extent: Optional[Tuple[str, ...]] #! Optional
    # vertical_extent_information: Optional[List[str]] #! Optional
    # spatial_reference_system: List[str]
    # resource_locator: Optional[List[str]]  #! Conditional
    # data_format: List[str]
    # responsible_organisation: List[str]
    # limitations_on_public_access: List[str]
    # use_constraints: List[str]
    # additional_information: Optional[str] #! Optional
    metadata_date: str
    metadata_language: str
    # metadata_point_of_contact: List[str]
    # resource_identifier: Optional[List[str]]  #! Conditional
    # coupled_resource: Optional[List[str]]  #! Conditional
    resource_type: str
    # conformity: List[str]
    # equivalent_scale: Optional[List[str]] = field(default_factory=list)  #! Optional
    # bounding_box: List[str]
    file_identifier: str
    # hierarchy_level_name: Optional[str]  #! Conditional
    quality_scope: Tuple[str, ...]
    # parent_identifier: Optional[str]  #! Optional
    spatial_representation_type: Tuple[str, ...]
    # character_encoding: Optional[List[str]]  #! Conditional
    # data_quality: Optional[List[str]]  #! Conditional
    # maintenance_information: Optional[str]  #! Optional
    # metadata_standard_name: Optional[str]  #! Optional
    # metadata_standard_version: Optional[str]  #! Optional

    @classmethod
    def from_file(
        cls: Type[MetadataType],
        xml_path: str,
        metadata_kwargs: Optional[Dict[str, Any]] = None,
    ) -> MetadataType:
        """# TODO.

        Args:
            xml_path (str): A remote URL or local file path.
            metadata_kwargs (Dict[str, Any]): Key word arguments to be passed to the
                requests `get`_ method when reading xml metadata from a URL.

        Returns:
            MetadataType: # TODO

        .. _get:
            https://requests.readthedocs.io/en/latest/api/#requests.request
        """
        _metadata_kwargs = metadata_kwargs if metadata_kwargs else {"verify": False}

        xml = _get_xml(
            xml_path, _metadata_kwargs
        )  # noqa: S410, S320 - assuming we can trust the XML

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
