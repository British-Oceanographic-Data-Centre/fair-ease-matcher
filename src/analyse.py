import asyncio
import logging
import os
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import httpx
from httpx import AsyncClient
from jinja2 import Template

from src.model_functions import merge_dicts
from src.sparql_queries import tabular_query_to_dict
from src.xml_extraction import extract_from_descriptiveKeywords, extract_from_topic_categories, \
    extract_from_content_info, extract_instruments_platforms_from_acquisition_info

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sparql_endpoint = os.getenv("SPARQL_ENDPOINT")
user = os.getenv("SPARQL_USERNAME", "")
passwd = os.getenv("SPARQL_PASSWORD", "")

namespaces = {
    'gmd': 'http://www.isotc211.org/2005/gmd',
    'gco': 'http://www.isotc211.org/2005/gco',
    'gmi': 'http://www.isotc211.org/2005/gmi',
    'xlink': 'http://www.w3.org/1999/xlink'
}


def analyse_from_xml(xml, threshold) -> dict:
    root = ET.fromstring(xml)
    logger.info("Obtained root from remote XML.")

    all_metadata_elems = extract_from_all(root)
    logger.info(f"Info extracted:\n{dict(all_metadata_elems)}".replace("}, '", "},\n'"))

    # tuple 1: config for text search
    # tuple 2: for identifiers (only search in dcterms:identifier predicate)
    # tuple 3: config for uri search
    mapping = {
        'keywords': [('strings', None)],
        'instrument': [('strings', None),
                       ('identifiers', 'dcterms:identifier'),
                       ('uris', None),],
        'variable': [('strings', None),
                     ('identifiers', 'dcterms:identifier'),
                     ('uris', None)],
        'platform': [('strings', None),
                     ('identifiers', 'dcterms:identifier'),
                     ('uris', None)]
    }

    query_args = get_query_args(all_metadata_elems, mapping)

    # all_queries = [(query_type, create_query(**kwargs, query_type=query_type)) for query_type, kwargs in query_args.items()]
    all_queries = generate_queries(query_args)
    all_bindings, head = run_all_queries(all_queries)

    exact_or_uri_matches = {k: False for k in all_metadata_elems}
    remove_exact_and_uri_matches(all_bindings, all_metadata_elems, exact_or_uri_matches)

    # If there are no Exact or URI matches for a metadata element, also run a fuzzy search on those metadata elements
    for metadata_element, has_exact_or_uri_match in exact_or_uri_matches.items():
        if has_exact_or_uri_match:
            mapping.pop(metadata_element.lower())
            all_metadata_elems.pop(metadata_element)
    fuzzy_query_args = get_query_args(all_metadata_elems, mapping)
    fuzzy_queries = generate_queries(fuzzy_query_args, fuzzy=True)
    if fuzzy_queries:
        fuzzy_bindings, _ = run_all_queries(fuzzy_queries)
        all_bindings.extend(fuzzy_bindings)

    results = {'head': head, 'results': {'bindings': all_bindings}, 'all_search_elements': all_metadata_elems}
    return results


def run_all_queries(all_queries):
    all_bindings = []
    all_results = asyncio.run(run_queries(all_queries))
    for query_type, result in all_results:
        head, bindings = flatten_results(result, query_type)
        all_bindings.extend(bindings)
    return all_bindings, head


def generate_queries(query_args, fuzzy=False):
    all_queries = []
    for query_type, kwargs in query_args.items():
        if ("uris" in query_type or "identifiers" in query_type) and fuzzy:
            pass  # don't need fuzzy search on
        else:
            queries = create_query(**kwargs, query_type=query_type, fuzzy=fuzzy)
            all_queries.extend([(query_type, query) for query in queries])
    return all_queries


def get_query_args(all_metadata_elems, mapping):
    query_args = {
        f"{prefix}_{key}": {
            "predicate": predicate,
            "terms": all_metadata_elems[prefix.capitalize()][key]
        }
        for prefix, configs in mapping.items()
        for (key, predicate) in configs
    }
    return query_args


def remove_exact_and_uri_matches(all_bindings, all_metadata_elems, exact_or_uri_matches):
    for result in all_bindings:
        if result["MethodSubType"]["value"] in ["Exact Match"]:
            target_element = result["TargetElement"]["value"]
            for search_type, search_list in all_metadata_elems[target_element].items():
                search_term = result["SearchTerm"]["value"]
                if search_term in search_list:
                    all_metadata_elems[target_element][search_type].remove(search_term)



async def run_queries(queries):
    async with AsyncClient(auth=(user, passwd) if user else None, timeout=30) as client:
        return await asyncio.gather(*[tabular_query_to_dict(query, query_type, client) for query_type, query in queries])


def execute_async_func(func, *args, **kwargs):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(func(*args, **kwargs))


def flatten_results(json_doc, method):
    method_labels = {
        'keywords_strings': ('Keywords', 'Text Match'),
        'instrument_strings': ('Instrument', 'Text Match'),
        'instrument_identifiers': ('Instrument', 'Identifiers Match'),
        'instrument_uris': ('Instrument', 'URI Match'),
        'variable_strings': ('Variable', 'Text Match'),
        'variable_identifiers': ('Variable', 'Identifiers Match'),
        'variable_uris': ('Variable', 'URI Match'),
        'platform_strings': ('Platform', 'Text Match'),
        'platform_identifiers': ('Platform', 'Identifiers Match'),
        'platform_uris': ('Platform', 'URI Match'),
    }


    new_head = {'vars': [head for head in json_doc['head']['vars'] + ["Method", "TargetElement"]]}

    target_element, method_label = method_labels[method]
    label_dict = {
        "Method": {"type": "literal", "value": method_label},
        "TargetElement": {"type": "literal", "value": target_element}
    }

    new_bindings = [{**label_dict, **binding} for binding in json_doc['results']['bindings']]

    return new_head, new_bindings


def create_query(predicate, terms, query_type, fuzzy=False):
    queries = []
    if "uri" in query_type:
        template = Template(Path("src/sparql/uri_query_template.sparql").read_text())
    else:
        template = Template(Path("src/sparql/query_template.sparql").read_text())
        # escape the terms for Lucene
        if terms:
            terms = [escape_for_lucene_and_sparql(term) for term in terms]
    # Render the template with the necessary parameters
    query = template.render(predicate=predicate, terms=terms, fuzzy=fuzzy)  # template imported at module level.
    queries.append(query)
    return queries


def escape_for_lucene_and_sparql(query):
    # First, escape the Lucene special characters.
    chars_to_escape = re.compile(r'([\+\-!\(\)\{\}\[\]\^"~\*\?:\\/])')
    lucene_escaped = chars_to_escape.sub(r'\\\1', query)

    # Then, double escape the backslashes for SPARQL.
    sparql_escaped = lucene_escaped.replace('\\', '\\\\')
    return sparql_escaped


def get_root_from_remote(xml_url):
    try:
        response = httpx.get(xml_url, timeout=10)  # 10 seconds timeout
        logger.info(f"Response from {xml_url}: {response.text}")
        # Check if the request was successful
        if response.status_code != 200:
            logger.error(
                f"Failed to fetch XML from {xml_url}. Status code: {response.status_code}. Response: {response.text}")
            raise Exception(f"HTTP error {response.status_code} when fetching XML.")

        root = ET.fromstring(response.text)
        return root

    except httpx.HTTPError as e:
        logger.error(f"HTTP error occurred when fetching XML from {xml_url}. Error: {str(e)}")
        raise
    except ET.ParseError as e:
        logger.error(f"Failed to parse XML from {xml_url}. Error: {str(e)}")
        raise


def extract_from_all(root):
    all_dicts = []
    all_dicts.append({"Keywords": {}, "Instrument": {}, "Variable": {}, "Platform": {}})  # default empty dicts
    all_dicts.append(extract_from_descriptiveKeywords(root))
    all_dicts.append(extract_from_topic_categories(root))
    all_dicts.append(extract_from_content_info(root))
    all_dicts.append(extract_instruments_platforms_from_acquisition_info(root))
    merged = merge_dicts(all_dicts)
    return merged
