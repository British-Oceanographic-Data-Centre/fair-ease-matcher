import xml.etree.ElementTree as ET

from src.string_functions import (
    quack_analyser,
    get_http_https_variant,
    get_trailing_slash_variant,
)

XMLNS_XLINK = "{http://www.w3.org/1999/xlink}"


def extract_full_xml(xml_string):
    root = ET.fromstring(xml_string)
    all_text = extract_data(root)
    types_to_plural = {  # TODO refactor out this is unnecessary
        "URI": "uris",
        "identifier": "identifiers",
        "string": "strings",
    }
    types_to_text = [
        {"guessed_type": types_to_plural[quack_analyser(text)], "text": text}
        for text in all_text
    ]
    variants = []
    for gt2t in types_to_text:
        if gt2t["guessed_type"] == "uris":
            http_https_variant = get_http_https_variant(gt2t["text"])
            if http_https_variant:
                variants.append({"guessed_type": "uris", "text": http_https_variant})
                trailing_slash_on_http_variant = get_trailing_slash_variant(
                    http_https_variant
                )
                if trailing_slash_on_http_variant:
                    variants.append(
                        {"guessed_type": "uris", "text": trailing_slash_on_http_variant}
                    )
            trailing_slash_variant = get_trailing_slash_variant(gt2t["text"])
            if trailing_slash_variant:
                variants.append({"guessed_type": "uris", "text": trailing_slash_variant})
    extended = list(types_to_text)
    extended.extend(variants)
    return types_to_text, extended


def extract_data(element):
    # Base case: if element is None, return an empty list
    if element is None:
        return []

    # Extract text and tail (if present) for the current element
    data = []
    if element.text and element.text.strip():
        data.append(element.text.strip().replace("\n", ""))
    if element.tail and element.tail.strip():
        data.append(element.tail.strip().replace("\n", ""))

    # Extract xlink:href attribute if it exists
    xlink_href = element.attrib.get(f"{XMLNS_XLINK}href")
    if xlink_href:
        data.append(xlink_href)

    for attrib_val in element.attrib.values():
        if attrib_val:
            if " " in attrib_val:
                attrib_vals = attrib_val.split(" ")
                data.extend(attrib_vals)
            else:
                data.append(attrib_val)

    # Recursively extract data from child elements
    for child in element:
        data.extend(extract_data(child))

    return data
