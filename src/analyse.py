import asyncio
import itertools
import logging
import os
import re
from typing import List

import xml.etree.ElementTree as ET
from pathlib import Path

import httpx
from flask import current_app as app
from httpx import AsyncClient
from jinja2 import Template
from netCDF4 import Dataset
from rdflib import URIRef

from src.model_functions import merge_dicts
from src.sparql_queries import tabular_query_to_dict
from src.xml_extract_all import extract_full_xml
from src.xml_extraction import (
    extract_from_descriptiveKeywords,
    extract_from_topic_categories,
    extract_from_content_info,
    extract_instruments_platforms_from_acquisition_info,
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sparql_endpoint = os.getenv("SPARQL_ENDPOINT")
user = os.getenv("SPARQL_USERNAME", "")
passwd = os.getenv("SPARQL_PASSWORD", "")

namespaces = {
    "gmd": "http://www.isotc211.org/2005/gmd",
    "gco": "http://www.isotc211.org/2005/gco",
    "gmi": "http://www.isotc211.org/2005/gmi",
    "xlink": "http://www.w3.org/1999/xlink",
}

themes_map = {
    "param": URIRef("http://vocab.nerc.ac.uk/collection/L19/current/SDNKG03/"),
    "plat": URIRef("http://vocab.nerc.ac.uk/collection/L19/current/SDNKG04/"),
    "inst": URIRef("http://vocab.nerc.ac.uk/collection/L19/current/SDNKG01/"),
}


def analyse_from_full_xml(xml_string, restrict_to_themes, exclude_deprecated = False, match_properties=None):        
    types_to_text_original, types_to_text_with_variants = extract_full_xml(xml_string)
    mapping = {"all": [("uris", None)]}
    collected_t2t_with_variants = collect_types(types_to_text_with_variants)
    query_args = get_query_args(collected_t2t_with_variants, mapping, restrict_to_themes)
    all_queries = generate_queries(query_args, exclude_deprecated=exclude_deprecated, match_properties=match_properties)
    all_bindings, head = run_all_queries(all_queries)

    # {'guessed_type': 'uris', 'text': 'http://www.isotc211.org/2005/gmi'}

    original_set = set([orig['text'] for orig in types_to_text_original if [orig['guessed_type'] == 'uris']])
    variant_set = set([orig['text'] for orig in types_to_text_with_variants if [orig['guessed_type'] == 'uris']])

    variants = variant_set - original_set

    # Map MatchProperty URIs to readable labels
    all_bindings = map_match_property_to_label(all_bindings)

    all_search_terms = (set(query_args['all_uris']['terms']) - variants) #-


    search_terms_found = set(d["SearchTerm"]["value"] for d in all_bindings)
    search_terms_not_found = list(all_search_terms - search_terms_found)

    results = {
        "head": head,
        "results": {"bindings": all_bindings},
        "all_search_elements": collected_t2t_with_variants,
        "search_terms_not_found": search_terms_not_found,
        "stats": {"found": len(search_terms_found), "total": len(all_search_terms)},
    }

    # Collect the URI matches to pass to the structured XML analysis to avoid further analysis
    uri_matches: List[str] = []
    for binding in all_bindings:
        if binding["Method"]["value"] == "URI Match":
            uri_matches.append(binding["MatchURI"]["value"])
    return results, uri_matches


def collect_types(types_to_text: dict):
    # Using a dictionary to group texts by their guessed_type
    result_dict = {"uris": [], "strings": [], "identifiers": []}

    for item in types_to_text:
        guessed_type = item["guessed_type"]
        if guessed_type == "uris":
            contains_whitespace = any(char.isspace() for char in item["text"])
            if contains_whitespace:
                guessed_type = "strings"
        result_dict[guessed_type].append(item["text"])

    # Creating the final list of dictionaries
    result = {"All": result_dict}
    return result


def analyse_from_netcdf(file_bytes, exclude_deprecated=False, restrict_to_themes=None, match_properties=None):
    # Use the memory argument to read from file_bytes
    rootgrp = Dataset(filename=None, mode="r", memory=file_bytes, format="NETCDF3")
    # try get URNs
    var_urns = extract_urns_from_netcdf(rootgrp, "sdn_parameter_urn")
    uom_urns = extract_urns_from_netcdf(rootgrp, "sdn_uom_urn")

    var_names = list(set(rootgrp.variables.keys()))
    var_names_long = list(
        set(
            [
                getattr(rootgrp.variables[var_name], "long_name", None)
                for var_name in var_names
                if hasattr(rootgrp.variables[var_name], "long_name")
            ]
        )
    )
    var_names_standard = list(
        set(
            [
                getattr(rootgrp.variables[var_name], "standard_name", None)
                for var_name in var_names
                if hasattr(rootgrp.variables[var_name], "standard_name")
            ]
        )
    )
    all_var_strings = var_names + var_names_long + var_names_standard

    source = getattr(
        rootgrp, "source", ""
    )  # example values appear to be platforms e.g. 'drifting surface float'
    platform_name = getattr(rootgrp, "platform_name", "")
    platform_names = []
    if source:
        platform_names.append(source)
    if platform_name:
        platform_names.append(platform_name)

    platform_code = getattr(rootgrp, "platform_code", "")
    platform_codes = []
    if platform_code:
        platform_codes.append(platform_code)

    # add convetnions to keywords - not a great fit though.
    conventions = getattr(rootgrp, "Conventions", "").split()

    all_metadata_elems = {
        "Keywords": {"identifiers": [], "strings": conventions, "uris": []},
        "Variable": {"identifiers": var_urns, "strings": all_var_strings, "uris": []},
        "Platform": {
            "identifiers": platform_codes,
            "strings": platform_names,
            "uris": [],
        },
    }
    mapping = {
        "variable": [
            ("strings", None),
            ("identifiers", "dcterms:identifier"),
            ("uris", None),
        ]
    }

    query_args = get_query_args(all_metadata_elems, mapping, restrict_to_themes)
    all_queries = generate_queries(query_args, exclude_deprecated=exclude_deprecated, match_properties=match_properties)
    all_bindings, head = run_all_queries(all_queries)

    # Map MatchProperty URIs to readable labels
    all_bindings = map_match_property_to_label(all_bindings)

    _all_search_terms = [list(i.values()) for i in all_metadata_elems.values()]
    all_search_terms = set(
        itertools.chain.from_iterable(itertools.chain.from_iterable(_all_search_terms))
    )
    search_terms_found = set(d["SearchTerm"]["value"] for d in all_bindings)
    search_terms_not_found = list(all_search_terms - search_terms_found)

    results = {
        "head": head,
        "results": {"bindings": all_bindings},
        "all_search_elements": all_metadata_elems,
        "search_terms_not_found": search_terms_not_found,
        "stats": {"found": len(search_terms_found), "total": len(all_search_terms)},
    }
    return results

def map_geodab_meta_to_sparql_meta(term):
    mapping = {
        'instrument': ['inst'],
        'platform': ['plat'],
        'parameter': ['param'],
        'keyword': None
    }
    return mapping.get(term, None)

def get_terms_elements(terms, restrict_to_theme):
    all_terms_elements = {
        'Instrument': {'uris': [], 'identifiers': [], 'strings': []},
        'Variable': {'uris': [], 'identifiers': [], 'strings': []},
        'Keywords': {'uris': [], 'identifiers': [], 'strings': []},
        'Platform': {'uris': [], 'identifiers': [], 'strings': []}
    }

    dab_term_map = {
        'instrument': 'Instrument',
        'parameter': 'Variable',
        'keyword': 'Keywords',
        'platform': 'Platform'
    }
    
    theme_key = dab_term_map.get(restrict_to_theme, 'Keywords')
    if theme_key:
        all_terms_elements[theme_key]['strings'] = terms    
    return all_terms_elements

def analyse_from_geodab_terms(terms, restrict_to_theme, exclude_deprecated=False, restrict_to_vocabs = None, match_properties=None) -> dict:                
  
    terms_clean = [el.replace('"', "\'") for el in terms]
    all_metadata_elems = get_terms_elements(terms_clean, restrict_to_theme)                
    restrict_to_theme = map_geodab_meta_to_sparql_meta(restrict_to_theme)
                                           
    mapping = {
        "keywords": [("strings", None)],
        "instrument": [
            ("strings", None),
            ("identifiers", "dcterms:identifier"),
            ("uris", None),
        ],
        "variable": [
            ("strings", None),
            ("identifiers", "dcterms:identifier"),
            ("uris", None),
        ],
        "platform": [
            ("strings", None),
            ("identifiers", "dcterms:identifier"),
            ("uris", None),
        ],
    }    
    

    query_args = get_query_args(all_metadata_elems, mapping, restrict_to_theme, restrict_to_vocabs=restrict_to_vocabs)    
    all_queries = generate_queries(query_args, exclude_deprecated=exclude_deprecated, match_properties=match_properties)                                    

    all_bindings, head = run_all_queries(all_queries)            
    exact_or_uri_matches = {k: False for k in all_metadata_elems}
    
    remove_uri_matches_from_other_matches(all_bindings)
    remove_exact_and_uri_matches(all_bindings, all_metadata_elems)
    
# If there are no Exact or URI matches for a metadata element, also run a proximity search on those metadata elements
    proximity_preds = "skos:prefLabel dcterms:description skos:altLabel dcterms:identifier rdfs:label"  # https://github.com/Kurrawong/fair-ease-matcher/issues/37
    for metadata_element, has_exact_or_uri_match in exact_or_uri_matches.items():
        if has_exact_or_uri_match:
            mapping.pop(metadata_element.lower())
            all_metadata_elems.pop(metadata_element)
        else:
            lower_metadata_element = metadata_element.lower()
            mapping_tuples = mapping.get(lower_metadata_element, [])

            for i, (metadata_type, value) in enumerate(mapping_tuples):
                if value is None:
                    mapping_tuples[i] = (metadata_type, proximity_preds)

    proximity_query_args = get_query_args(
        all_metadata_elems, mapping, restrict_to_theme, restrict_to_vocabs=restrict_to_vocabs
    )
    proximity_queries = generate_queries(proximity_query_args, exclude_deprecated=exclude_deprecated, proximity=True, match_properties=match_properties)
    if proximity_queries:
        proximity_bindings, _ = run_all_queries(proximity_queries)
        all_bindings.extend(proximity_bindings)
        # Map MatchProperty URIs to readable labels
        all_bindings = map_match_property_to_label(all_bindings)

    _all_search_terms = [list(i.values()) for i in all_metadata_elems.values()]
    all_search_terms = set(
        itertools.chain.from_iterable(itertools.chain.from_iterable(_all_search_terms))
    )
    search_terms_found = set(d["SearchTerm"]["value"] for d in all_bindings)
    search_terms_not_found = list(all_search_terms - search_terms_found)
    results = {
        "head": head,
        "results": {"bindings": all_bindings},
        "all_search_elements": all_metadata_elems,
        "search_terms_not_found": search_terms_not_found,
        "stats": {"found": len(search_terms_found), "total": len(all_search_terms)},
    }    
    
    return results

def analyse_from_xml_structure(xml, threshold, restrict_to_themes, exclude_deprecated=False, match_properties=None, uri_matches=None) -> dict:
    root = ET.fromstring(xml)
    logger.info("Obtained root from remote XML.")
    all_metadata_elems = extract_from_all(root)
    logger.info(f"Info extracted:\n{dict(all_metadata_elems)}".replace("}, '", "},\n'"))

    # Prevent URI matches from being analysed again if they were previously matched
    if uri_matches:
        for key in all_metadata_elems.keys():
            all_metadata_elems[key]['uris'] = [uri for uri in all_metadata_elems[key]['uris'] if uri not in uri_matches]

    # tuple 1: config for text search
    # tuple 2: for identifiers (only search in dcterms:identifier predicate)
    # tuple 3: config for uri search
    mapping = {
        "keywords": [("strings", None)],
        "instrument": [
            ("strings", None),
            ("identifiers", "dcterms:identifier"),
            ("uris", None),
        ],
        "variable": [
            ("strings", None),
            ("identifiers", "dcterms:identifier"),
            ("uris", None),
        ],
        "platform": [
            ("strings", None),
            ("identifiers", "dcterms:identifier"),
            ("uris", None),
        ],
    }

    query_args = get_query_args(all_metadata_elems, mapping, restrict_to_themes)
    all_queries = generate_queries(query_args, exclude_deprecated=exclude_deprecated, match_properties=match_properties)            
    all_bindings, head = run_all_queries(all_queries)
    # Map MatchProperty URIs to readable labels
    all_bindings = map_match_property_to_label(all_bindings)
    exact_or_uri_matches = {k: False for k in all_metadata_elems}

    remove_uri_matches_from_other_matches(all_bindings)
    remove_exact_and_uri_matches(all_bindings, all_metadata_elems)

    # If there are no Exact or URI matches for a metadata element, also run a proximity search on those metadata elements
    proximity_preds = "skos:prefLabel dcterms:description skos:altLabel dcterms:identifier rdfs:label"  # https://github.com/Kurrawong/fair-ease-matcher/issues/37
    for metadata_element, has_exact_or_uri_match in exact_or_uri_matches.items():
        if has_exact_or_uri_match:
            mapping.pop(metadata_element.lower())
            all_metadata_elems.pop(metadata_element)
        else:
            lower_metadata_element = metadata_element.lower()
            mapping_tuples = mapping.get(lower_metadata_element, [])

            for i, (metadata_type, value) in enumerate(mapping_tuples):
                if value is None:
                    mapping_tuples[i] = (metadata_type, proximity_preds)

    proximity_query_args = get_query_args(
        all_metadata_elems, mapping, restrict_to_themes
    )
    proximity_queries = generate_queries(proximity_query_args, proximity=True, exclude_deprecated=exclude_deprecated, match_properties=match_properties)
    if proximity_queries:
        proximity_bindings, _ = run_all_queries(proximity_queries)
        all_bindings.extend(proximity_bindings)
        # Map MatchProperty URIs to readable labels
        all_bindings = map_match_property_to_label(all_bindings)

    _all_search_terms = [list(i.values()) for i in all_metadata_elems.values()]
    all_search_terms = set(
        itertools.chain.from_iterable(itertools.chain.from_iterable(_all_search_terms))
    )
    search_terms_found = set(d["SearchTerm"]["value"] for d in all_bindings)
    search_terms_not_found = list(all_search_terms - search_terms_found)
    results = {
        "head": head,
        "results": {"bindings": all_bindings},
        "all_search_elements": all_metadata_elems,
        "search_terms_not_found": search_terms_not_found,
        "stats": {"found": len(search_terms_found), "total": len(all_search_terms)},
    }

    return results


def run_all_queries(all_queries):
    all_bindings = []
    head = {}
    all_results = asyncio.run(run_queries(all_queries))

    for query_type, result in all_results:
        head, bindings = flatten_results(result, query_type)
        all_bindings.extend(bindings)
        
    return all_bindings, head


def generate_queries(query_args, proximity=False, exclude_deprecated=False, match_properties=None):            
    all_queries = []
    for query_type, kwargs in query_args.items():
        if kwargs["terms"]:
            if ("uris" in query_type or "identifiers" in query_type) and proximity:
                pass  # don't need proximity search on
            else:
                queries = create_query(
                    **kwargs, query_type=query_type, proximity=proximity, exclude_deprecated=exclude_deprecated, match_properties=match_properties
                )                
                all_queries.extend([(query_type, query) for query in queries])                                
    return all_queries


def get_query_args(all_metadata_elems, mapping, theme_names=None, restrict_to_vocabs = None):
    """vocabs: the vocabularies the query should be restricted to"""
    query_args = {
        f"{prefix}_{key}": {
            "predicate": predicate,
            "terms": all_metadata_elems[prefix.capitalize()][key],
        }
        for prefix, configs in mapping.items()
        for (key, predicate) in configs
    }

    if theme_names:
        for prefix_key in query_args:
            query_args[prefix_key]["theme_uris"] = [
                themes_map[theme_name] for theme_name in theme_names
            ]

    if restrict_to_vocabs:
        for prefix_key in query_args:
            query_args[prefix_key]["allowed_vocabs"] = restrict_to_vocabs
    
    return query_args
        
def remove_uri_matches_from_other_matches(all_bindings):
    """If a search term matches a URI, remove it from the other matches."""
    # Collect URIs that have a MethodSubType of 'URI Match'
    uri_match_uris = [
        result["MatchURI"]["value"]
        for result in all_bindings
        if result.get("MethodSubType", {}).get("value") == "URI Match"
    ]

    # Rebuild the list, excluding non-URI matches found in the collected URIs
    all_bindings[:] = [
        result
        for result in all_bindings
        if result.get("MethodSubType", {}).get("value") == "URI Match"
        or result["MatchURI"]["value"] not in uri_match_uris
    ]



def remove_exact_and_uri_matches(all_bindings, all_metadata_elems):
    """Remove exact and URI matches from the results."""
    exact_matches_uris = [
        result["MatchURI"]["value"]
        for result in all_bindings
        if result.get("MethodSubType", {}).get("value") == "Exact Match"
    ]



    # Rebuild the list excluding the exact matches and URI matches that should be removed
    all_bindings[:] = [
        result
        for result in all_bindings
        if result.get("MethodSubType", {}).get("value") == "Exact Match"
        or result["MatchURI"]["value"] not in exact_matches_uris
    ]

    uri_matches = [
        result["MatchURI"]["value"]
        for result in all_bindings
        if result.get("MethodSubType", {}).get("value") == "URI Match"
    ]


    all_bindings[:] = [
        result
        for result in all_bindings
        if result.get("MethodSubType", {}).get("value") in ["Exact Match", "URI Match"]
        or result["MatchURI"]["value"] not in uri_matches
    ]



async def run_queries(queries):
    async with AsyncClient(auth=(user, passwd) if user else None, timeout=30) as client:
        return await asyncio.gather(
            *[
                tabular_query_to_dict(query, query_type, client)
                for query_type, query in queries
            ]
        )

def execute_async_func(func, *args, **kwargs):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(func(*args, **kwargs))


def flatten_results(json_doc, method):
    method_labels = {
        "all_uris": ("All", "URI Match"),
        "all_identifiers": ("All", "Identifiers Match"),
        "keywords_strings": ("Keywords", "Text Match"),
        "instrument_strings": ("Instrument", "Text Match"),
        "instrument_identifiers": ("Instrument", "Identifiers Match"),
        "instrument_uris": ("Instrument", "URI Match"),
        "variable_strings": ("Variable", "Text Match"),
        "variable_identifiers": ("Variable", "Identifiers Match"),
        "variable_uris": ("Variable", "URI Match"),
        "platform_strings": ("Platform", "Text Match"),
        "platform_identifiers": ("Platform", "Identifiers Match"),
        "platform_uris": ("Platform", "URI Match"),
    }

    new_head = {
        "vars": [
            head for head in json_doc["head"]["vars"] + ["Method", "TargetElement"]
        ]
    }

    target_element, method_label = method_labels[method]
    label_dict = {
        "Method": {"type": "literal", "value": method_label},
        "TargetElement": {"type": "literal", "value": target_element},
    }

    new_bindings = [
        {**label_dict, **binding} for binding in json_doc["results"]["bindings"]
    ]
    return new_head, new_bindings



def create_query(predicate, terms, query_type, theme_uris=None, proximity=False, exclude_deprecated=False, allowed_vocabs = [], match_properties=None):
    queries = []
    if "uri" in query_type:
        template = Template(Path("src/sparql/uri_query_template.sparql").read_text())
    else:
        template = Template(Path("src/sparql/query_template.sparql").read_text())
        # escape the terms for Lucene
        if terms:
            terms = [escape_for_lucene_and_sparql(term) for term in terms]
    # Render the template with the necessary parameters

    query = template.render(
        predicate=predicate, terms=terms, proximity=proximity, theme_uris=theme_uris, exclude_deprecated=exclude_deprecated,
        allowed_vocabs=[f"<{x}>" for x in allowed_vocabs], match_properties=match_properties
    )  # template imported at module level.
    queries.append(query)        
    return queries


def escape_for_lucene_and_sparql(query):
    # First, escape the Lucene special characters.
    chars_to_escape = re.compile(r'([\+\-!\(\)\{\}\[\]\^"~\*\?:\\/])')
    lucene_escaped = chars_to_escape.sub(r"\\\1", query)

    # Then, double escape the backslashes for SPARQL.
    sparql_escaped = lucene_escaped.replace("\\", "\\\\")
    return sparql_escaped


def get_root_from_remote(xml_url):
    try:
        response = httpx.get(xml_url, timeout=10)  # 10 seconds timeout
        logger.info(f"Response from {xml_url}: {response.text}")
        # Check if the request was successful
        if response.status_code != 200:
            logger.error(
                f"Failed to fetch XML from {xml_url}. Status code: {response.status_code}. Response: {response.text}"
            )
            raise Exception(f"HTTP error {response.status_code} when fetching XML.")

        root = ET.fromstring(response.text)
        return root

    except httpx.HTTPError as e:
        logger.error(
            f"HTTP error occurred when fetching XML from {xml_url}. Error: {str(e)}"
        )
        raise
    except ET.ParseError as e:
        logger.error(f"Failed to parse XML from {xml_url}. Error: {str(e)}")
        raise


def extract_from_all(root):
    all_dicts = []
    all_dicts.append(
        {"Keywords": {}, "Instrument": {}, "Variable": {}, "Platform": {}}
    )  # default empty dicts
    all_dicts.append(extract_from_descriptiveKeywords(root))
    all_dicts.append(extract_from_topic_categories(root))
    all_dicts.append(extract_from_content_info(root))
    all_dicts.append(extract_instruments_platforms_from_acquisition_info(root))
    merged = merge_dicts(all_dicts)
    return merged


def run_method_dab_terms(doc_name, results, terms, restrict_to_theme, exclude_deprecated = False, restrict_to_vocabs = None, match_properties=None):
    results[doc_name] = {}     
    results[doc_name][
        app.config["Methods"]["terms"]["source"]
    ] = analyse_from_geodab_terms(terms, restrict_to_theme, exclude_deprecated=exclude_deprecated, restrict_to_vocabs=restrict_to_vocabs, match_properties=match_properties)
    
def run_methods(
        doc_name, methods, results, threshold, xml_string, restrict_to_themes, method_type, exclude_deprecated=False, match_properties=None
):
    results[doc_name] = {}
    if method_type == "XML":  # run specified xml methods
        uri_matches = None
        if "full" in methods:
            full_results, uri_matches = analyse_from_full_xml(
                xml_string, 
                restrict_to_themes, 
                exclude_deprecated=exclude_deprecated, 
                match_properties=match_properties
            )
            results[doc_name][
                    app.config["Methods"]["metadata"]["full"]
                ] = full_results
        if "xml" in methods:
            results[doc_name][
                app.config["Methods"]["metadata"]["xml"]
            ] = analyse_from_xml_structure(
                xml_string,
                threshold,
                restrict_to_themes,
                exclude_deprecated=exclude_deprecated,
                match_properties=match_properties,
                uri_matches=uri_matches
            )

    if method_type == "NETCDF":  # run netCDF methods - currently just the one
        results[doc_name][
            app.config["Methods"]["netcdf"]["netcdf"]
        ] = analyse_from_netcdf(xml_string, restrict_to_themes=restrict_to_themes, exclude_deprecated=exclude_deprecated)

def extract_urns_from_netcdf(rootgrp: Dataset, var_name: str):
    var_urns = []
    for var in rootgrp.variables.items():
        urn_or_none = var[1].__dict__.get(var_name)
        if urn_or_none:
            var_urns.append(urn_or_none)
    return var_urns


def extract_text_from_net_cdf(rootgrp: Dataset, var_name: str):
    var_text = []
    for var in rootgrp.variables.items():
        text_or_none = var[1].__dict__.get(var_name)
        if text_or_none:
            var_text.append(text_or_none)
    return var_text

def map_match_property_to_label(bindings): # A function to map the match property URIs to the readable labels
    PROP_LABEL_MAP = {
    "http://www.w3.org/2004/02/skos/core#prefLabel": "Preferred Label",
    "http://www.w3.org/2000/01/rdf-schema#label": "Preferred Label",
    "http://purl.org/dc/terms/title": "Preferred Label",
    "https://schema.org/name": "Preferred Label",
    "http://www.w3.org/2004/02/skos/core#altLabel": "Alternate Label",
    "http://www.w3.org/2004/02/skos/core#definition": "Definition",
    "http://purl.org/dc/terms/description": "Definition",
    "http://purl.org/dc/terms/identifier": "Identifier",
    }
    for binding in bindings:
        match_property_uri = binding.get("MatchProperty", {}).get("value")
        if match_property_uri in PROP_LABEL_MAP:
            binding["MatchProperty"]["value"] = PROP_LABEL_MAP[match_property_uri]
    return bindings
