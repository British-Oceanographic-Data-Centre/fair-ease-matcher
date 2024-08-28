import asyncio
import itertools
import logging
import os
import re
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


def analyse_from_full_xml(xml_string, restrict_to_themes, exclude_deprecated):        
    types_to_text = extract_full_xml(xml_string)            
    mapping = {"all": [("uris", None)]}
    collected_t2t = collect_types(types_to_text)                
    query_args = get_query_args(collected_t2t, mapping, restrict_to_themes)            
    all_queries = generate_queries(query_args, exclude_deprecated=exclude_deprecated)
    all_bindings, head = run_all_queries(all_queries)

    _all_search_terms = [list(i.values()) for i in collected_t2t.values()]
    all_search_terms = set(
        itertools.chain.from_iterable(itertools.chain.from_iterable(_all_search_terms))
    )
    search_terms_found = set(d["SearchTerm"]["value"] for d in all_bindings)
    search_terms_not_found = list(all_search_terms - search_terms_found)

    results = {
        "head": head,
        "results": {"bindings": all_bindings},
        "all_search_elements": collected_t2t,
        "search_terms_not_found": search_terms_not_found,
        "stats": {"found": len(search_terms_found), "total": len(all_search_terms)},
    }
    return results


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


def analyse_from_netcdf(file_bytes, exclude_deprecated, restrict_to_themes=None):
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
    all_queries = generate_queries(query_args, exclude_deprecated=exclude_deprecated)
    all_bindings, head = run_all_queries(all_queries)

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

def analyse_from_geodab_terms(terms, restrict_to_theme, exclude_deprecated=False) -> dict:                
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
    
    query_args = get_query_args(all_metadata_elems, mapping, restrict_to_theme)    
    all_queries = generate_queries(query_args, exclude_deprecated=exclude_deprecated)                                    
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
        all_metadata_elems, mapping, restrict_to_theme
    )
    proximity_queries = generate_queries(proximity_query_args, exclude_deprecated=False, proximity=True)
    if proximity_queries:
        proximity_bindings, _ = run_all_queries(proximity_queries)
        all_bindings.extend(proximity_bindings)

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

def analyse_from_xml_structure(xml, threshold, restrict_to_themes, exclude_deprecated=False) -> dict:
    root = ET.fromstring(xml)
    logger.info("Obtained root from remote XML.")
    
    all_metadata_elems = extract_from_all(root)
    logger.info(f"Info extracted:\n{dict(all_metadata_elems)}".replace("}, '", "},\n'"))
    
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
    all_queries = generate_queries(query_args, exclude_deprecated=exclude_deprecated)            
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
        all_metadata_elems, mapping, restrict_to_themes
    )
    proximity_queries = generate_queries(proximity_query_args, proximity=True, exclude_deprecated=exclude_deprecated)
    if proximity_queries:
        proximity_bindings, _ = run_all_queries(proximity_queries)
        all_bindings.extend(proximity_bindings)

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


def generate_queries(query_args, proximity=False, exclude_deprecated=False):            
    all_queries = []
    for query_type, kwargs in query_args.items():
        if kwargs["terms"]:
            if ("uris" in query_type or "identifiers" in query_type) and proximity:
                pass  # don't need proximity search on
            else:
                queries = create_query(
                    **kwargs, query_type=query_type, proximity=proximity, exclude_deprecated=exclude_deprecated
                )                
                all_queries.extend([(query_type, query) for query in queries])                                
    return all_queries


def get_query_args(all_metadata_elems, mapping, theme_names=None):
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
    
    return query_args
        
def remove_uri_matches_from_other_matches(all_bindings):
    """If a search term matches a URI, remove it from the other matches"""
    uri_match_uris = [result["MatchURI"]["value"] for result in all_bindings if
                      result["MethodSubType"]["value"] == "URI Match"]

    to_remove = [result for result in all_bindings if
                 result["MatchURI"]["value"] in uri_match_uris and
                 result["MethodSubType"]["value"] != "URI Match"]
    logger.info(msg=f"Deduplicating {len(to_remove)} matches where the URI has already been exactly matched.")

    # Then, rebuild the list excluding the non-URI matches not found in the collected URIs
    all_bindings[:] = [result for result in all_bindings if
                       result["MethodSubType"]["value"] == "URI Match" or
                       result["MatchURI"]["value"] not in uri_match_uris]


def remove_exact_and_uri_matches(all_bindings, all_metadata_elems):
    for result in all_bindings:
        if result["MethodSubType"]["value"] in ["Exact Match"]:
            target_element = result["TargetElement"]["value"]
            for search_type, search_list in all_metadata_elems[target_element].items():
                search_term = result["SearchTerm"]["value"]
                if search_term in search_list:
                    all_metadata_elems[target_element][search_type].remove(search_term)


async def run_queries(queries):
    async with AsyncClient(auth=(user, passwd) if user else None, timeout=280) as client:
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


def create_query(predicate, terms, query_type, theme_uris=None, proximity=False, exclude_deprecated=False):
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
        predicate=predicate, terms=terms, proximity=proximity, theme_uris=theme_uris, exclude_deprecated=exclude_deprecated
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

def run_method_dab_terms(doc_name, results, terms, restrict_to_theme):
    results[doc_name] = {}     
    results[doc_name][
        app.config["Methods"]["terms"]["source"]
    ] = analyse_from_geodab_terms(terms, restrict_to_theme)
    
def run_methods(
        doc_name, methods, results, threshold, xml_string, restrict_to_themes, method_type, exclude_deprecated=False
):
    results[doc_name] = {}
    if method_type == "XML":  # run specified xml methods
        for method in methods:
            if method == "xml":
                results[doc_name][
                    app.config["Methods"]["metadata"][method]
                ] = analyse_from_xml_structure(
                    xml_string, threshold, restrict_to_themes, exclude_deprecated=exclude_deprecated
                )
            elif method == "full":
                results[doc_name][
                    app.config["Methods"]["metadata"][method]
                ] = analyse_from_full_xml(xml_string, restrict_to_themes, exclude_deprecated=exclude_deprecated)
    if method_type == "NETCDF":  # run netCDF methods - currently just the one
        results[doc_name][
            app.config["Methods"]["netcdf"]["netcdf"]
        ] = analyse_from_netcdf(xml_string, restrict_to_themes, exclude_deprecated=exclude_deprecated)

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
