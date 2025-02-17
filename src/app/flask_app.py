import json
import logging
import time
from pathlib import Path
from urllib.parse import urlparse

import traceback

from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from httpx import AsyncClient

from collections import defaultdict

from src.analyse import run_methods, run_method_dab_terms
from src.sparql_queries import get_vocabs_from_sparql_endpoint, send_query

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


config = None
with open(Path(__file__).parent / "config.json", "r", encoding="utf-8") as file:
    config = json.load(file)

app = Flask(__name__)
for k, v in config.items():
    app.config[k] = v


# Allow requests from your UI
CORS(app)

# Map requested match properties to equivalent geodab
# See api/matchproperties
match_properties_map = {
    'identifier': 'id',
    'prefLabel': 'pref',
    'altLabel': 'alt',
    'definition': 'def'
}

# Map requested categories to equivalent geodab
# See /api/categories
categories_map = {
    'parameter': 'parameter',
    'platform': 'platform',
    'instrument': 'instrument',
    'theme': 'keyword',
    'all': ''
}

# Map results match type to equivalent geodab
# See /api/matchType
match_type_map = {
    'exactMatch': 'Exact Match',
    'wildcardMatch': 'Wildcard Match',
    'proximityMatch': 'Proximity Match',    
}


@app.route("/analyse", methods=["POST"])
def get_analysis_results():
    """Analyses terms. Based on the endpoint /process-geodab-terms but simplifies the
    structure of the json response.
    
    Fields in json payload of request:
    
    {
        "category" [optional]               Restricted values: see ~/api/categories 
                                            Cardinality      : 0:1
                                            Default value    : "" 
                           
        "vocabularies" [optional]           Cardinality      : 0:Many  
                                            Default value    : []
        
        "terms":                            Cardinality      : 1:Many
        
        "exclude_deprecated": [optional]    Cardinality      : 0
                                            Default value    : "false"
        
        "matchType": [optional]            Restricted values: see ~/api/matchType
                                            Cardinality      : 0:Many
                                            Default value    : ["exactMatch"]
                
        "matchProperties": [optional]       Restricted values: see ~/api/matchproperties
                                            Cardinality      : 0:Many
                                            Default value    : ["altLabel", "definition", "preflabel", "identifier"]                
    }
    
    example:
    
        {
            "category": "parameter",            
            "terms": ["SALINITY", "AMETEK", "Base current of pH sensor"],              
            "matchType": ['exactMatch','proximityMatch'],            
            "matchProperties": ["altLabel", "definition"]
        }
     
    """
    #
    # Verify we have some json
    #
    sa_data = request.get_json(silent=True) or {}

    if not sa_data:
        return make_response("Error JSON: No data provided or invalid JSON", 400)

    #
    # category json field
    #
    category = sa_data.get("category", "all")
    
    if isinstance(category, list):
        return make_response("Error JSON value: category should be a string not array", 400)
        
    
    if category not in categories_map:
        return make_response(f"Error JSON value: Invalid '{category}' category", 400)

    category = categories_map[category]
        
    #
    # vocabularies json field
    #
    vocabularies = sa_data.get("vocabularies", [])
    if not isinstance(vocabularies, list):
        return make_response("Error JSON value: vocabularies should be an array list", 400)

    #
    # terms json field
    #
    if not sa_data.get("terms"):
        return make_response(f"Error JSON value: {'No terms provided'}", 400)

    max_terms = config["max_terms_limit"]
    terms = sa_data["terms"]
    if len(terms) > max_terms:
        return make_response(f"Error JSON value: {'Number of terms cannot exceed'} {max_terms}", 400)

    #
    # matchType json field
    #
    match_type_required = (
        ["exactMatch"]
        if sa_data.get('matchType') is None
        else sa_data["matchType"]
    )    
    if not match_type_required:
        match_type_required = ["Exact Match"]
    else:
        match_type_required = [
            match_type_map.get(item) 
            for item in match_type_required 
                if item in match_type_map
        ]
        if not match_type_required:
            return make_response(f"Error JSON value: {'Invalid match type'}", 400)

    #
    # exclude deprecated json field
    #
    exclude_deprecated = str(sa_data.get("exclude_deprecated", "false")).lower() == "true"

    #
    # matchProperties json field
    #
    match_properties = (
        list(match_properties_map.keys())
        if sa_data.get('matchProperties') is None
        else sa_data["matchProperties"]
    )
    if not match_properties:
        match_properties = list(match_properties_map.values())
    else:
        match_properties = [
            match_properties_map.get(item)
            for item in match_properties
                if item in match_properties_map
        ]
        if not match_properties:
            return make_response(f"Error JSON value: {'Invalid match property'}", 400)    

    responses = {}
    try:
        run_method_dab_terms(
            "SAterms",
            responses,
            terms,
            category,
            exclude_deprecated=exclude_deprecated,
            restrict_to_vocabs=vocabularies,
            match_properties=match_properties
        )
    except Exception as e:
        traceback.print_exc()
        return make_response(f"Exception from Python: {str(e)}", 500)
    
    response = jsonify(responses)
    json_data = response.json
        
    # Extract the results bindings
    bindings = json_data["SAterms"]["geoDABterms"]["results"]["bindings"]
    
    terms_not_found = json_data["SAterms"]["geoDABterms"]["search_terms_not_found"]

    # Create the simplified json response structure
    results = {
        "@context": [
        "https://schema.org/",
        {
            "skos": "http://www.w3.org/2004/02/skos/core#"
        }
        ],
        "@graph": []
    }

    grouped_json = {}    
    stats = { "total_number_terms_found": 0 }
    match_count = 0

    for item in bindings:            
        matching_type = item.get('MethodSubType', {}).get('value')

        if matching_type not in match_type_required:
            continue

        additional_type = "" if not item.get('Categories') else item['Categories']['value']
        add_list = [x.strip().lower() for x in additional_type.split(',')]

        category = category.strip()
        
        category = "Theme" if category == "keyword" else category

        if not (not category or category.lower() in add_list):
            continue
        
        match_count = match_count + 1
        
        match_property = item['MatchProperty']['value']
        
        query = item['SearchTerm']['value']        
        url = item['MatchURI']['value']
        parsed_url = urlparse(url)
        path_parts = parsed_url.path.strip("/").split("/")
        term_code = path_parts[-1]

        matching_type = item['MethodSubType']['value'].replace(" ","")

        if "vocab.nerc.ac.uk" in parsed_url.netloc:
            in_defined_term_set = f"{parsed_url.scheme}://{parsed_url.netloc}/{'/'.join(path_parts[:-1])}/"
        else:
            sparql_query = f"""select distinct ?g where {{graph ?g {{<{url}> ?b ?n .}} }} limit 100"""
            vocabjson = get_vocabs_from_sparql_endpoint(sparql_query)
            in_defined_term_set = vocabjson["results"]["bindings"][0]["g"]["value"]

        graph_item = {
            "query": query,            
            "@type": "SearchAction",
            "result": [
                {
                  "@type": ["DefinedTerm", "skos:Concept","CreativeWork" ],
                  "@id": url,
                  "name": item['MatchTerm']['value'],
                  "additionalType": additional_type,
                  "inDefinedTermSet": in_defined_term_set,
                  "url": url,
                  "termCode": term_code,
                  "skos:deprecated": "false" if item["Status"]["value"] in "Accepted" else "true",
                  "matchType": matching_type,
                  "matchProperty": match_property
                }
            ]
        }

        stats = { "total_number_terms_found": match_count }

        results["@graph"].append(graph_item)     

    # Restructure the json by grouping results by query field
    grouped_results = defaultdict(list)

    for item in results["@graph"]:
        query = item["query"]
        grouped_results[query].extend(item["result"])

    grouped_json = {
        "@context": results["@context"],
        "@graph": [
            {"@type": "SearchAction", "query": query, "result": results}
            for query, results in grouped_results.items()
        ]
    }

    grouped_json["stats"] = stats
    grouped_json["search_terms_not_found"] = terms_not_found

    if len(bindings) > 0:
        return grouped_json

    results["stats"] = stats
    results["search_terms_not_found"] = terms_not_found

    return results


def parse_categories(data: dict) -> dict:
    """Parse the categories JSON into the correct format."""
    new_data = {"@context": {
    "@vocab": "https://schema.org/"

    },
    "@graph": [
        {
            "query": "categories",
            "@type": "SearchAction",
            "result": [
            ]

            }
        ]
    }
    for result in (data["results"]["bindings"]):
        raw_url = urlparse(result["c"]["value"])
        term_code = raw_url.path.strip("/").split("/")[-1]
        in_defined_term_set = f"{raw_url.scheme}://{raw_url.netloc}/{'/'.join(raw_url.path.strip('/').split('/')[:-1])}/"
        item =  {
            "@id": result["c"]["value"],
            "@type": "DefinedTerm",
            "name": result["prefLabel"]["value"].lower(),
            "inDefinedTermSet": in_defined_term_set,
            "url": result["c"]["value"],
            "termCode" :term_code
            }
        new_data["@graph"][0]["result"].append(item)
    return new_data

@app.route("/process-geodab-terms", methods=["POST"])
def process_metadata_geodab():
    start_time = time.time()
    data = request.json
    restrict_to_theme = data['metadata']
    terms = data['terms']
    vocabs = data['vocabs']
    responses = {}
    doc_name = 'geoDabTerms'
    match_properties = request.args.get("match_props")
    if match_properties:
        match_properties = match_properties.split(",")
    
    exclude_deprecated = request.args.get("exclude_deprecated", "false").lower() == "true"
    
    try:
        run_method_dab_terms(
            doc_name,
            responses,     
            terms,
            restrict_to_theme,
            exclude_deprecated=exclude_deprecated,
            restrict_to_vocabs=vocabs,
            match_properties=match_properties
        )
    except Exception as e:
        # Handle exceptions and send a 500 response
        traceback.print_exc()
        return make_response(f"Exception from Python: {str(e)}", 500)    
    
    response = jsonify(responses)
    response.headers["Access-Control-Allow-Origin"] = "*"
    logger.info(f"Time taken: {time.time() - start_time}")        
    return response    

@app.route("/process-metadata", methods=["GET", "POST"])
def process_metadata():
    start_time = time.time()

    analysis_methods = request.args.get("Methods")
    if analysis_methods:
        analysis_methods = analysis_methods.split(",")

    restrict_to_themes = request.args.get("Restrict to Themes")
    if restrict_to_themes:
        restrict_to_themes = restrict_to_themes.split(",")

    match_properties = request.args.get("match_props")
    if match_properties:
        match_properties = match_properties.split(",")

    exclude_deprecated = request.args.get("exclude_deprecated", "false").lower() == "true"

    if analysis_methods != ["netcdf"]:
        data = request.json
    else:
        data = request.files
    threshold = data.get("threshold")
    responses = {}
    available_methods = ["xml", "full", "netcdf"]
    if not analysis_methods:
        analysis_methods = available_methods

    # run XML methods
    if ("xml" in analysis_methods) or ("full" in analysis_methods):
        for doc_name, xml in data.get("xml").items():
            try:
                run_methods(
                    doc_name,
                    analysis_methods,
                    responses,
                    threshold,
                    xml,
                    restrict_to_themes,
                    "XML",
                    exclude_deprecated=exclude_deprecated, match_properties=match_properties
                )
            except Exception as e:
                # Handle exceptions and send a 500 response
                traceback.print_exc()
                return make_response(f"Exception from Python: {str(e)}", 500)

    if "netcdf" in analysis_methods:
        for doc_name in data:
            doc_data = data[doc_name].read()
            try:
                run_methods(
                    doc_name,
                    analysis_methods,
                    responses,
                    threshold,
                    doc_data,
                    restrict_to_themes,
                    "NETCDF",
                    exclude_deprecated=exclude_deprecated, match_properties=match_properties
                )
            except Exception as e:
                # Handle exceptions and send a 500 response
                return make_response(f"Exception from Python: {str(e)}", 500)

    response = jsonify(responses)
    response.headers["Access-Control-Allow-Origin"] = "*"
    logger.info(f"Time taken: {time.time() - start_time}")        
    return response

@app.route("/config", methods=["GET"])
def get_config():
    response = jsonify(config)
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


@app.route("/vocab-list", methods=["GET"])
async def get_vocab_list():
    category = request.args.get("category")
    query = """
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

    SELECT distinct ?collection ?title where {{
        graph <https://themes> {{ ?collection ?b ?c . }}
        ?c skos:prefLabel ?p .
        filter regex(str(?p), "{0}") .
        OPTIONAL {{
            ?collection skos:prefLabel ?title
        }}
    }} limit 100
    """
    async_client = AsyncClient()
    response = await send_query(query.format(category), mediatype="application/json", client=async_client)
    await response.aread()
    return response.json()

@app.route("/categories", methods=["GET"])
async def get_categories():  
    query = """
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    select distinct ?c ?prefLabel ?l where { graph <https://themes> {?a <http://www.w3.org/ns/dcat#theme> ?c .
        ?c <http://www.w3.org/2000/01/rdf-schema#label>  ?prefLabel
    } } limit 100"""
    async_client = AsyncClient()
    response = await send_query(query, mediatype="application/json", client=async_client)
    await response.aread()
    return parse_categories(response.json())


def get_match_properties_ld(sparql_json: dict):
    """Return json-ld form of match properties.
    
    Args:
        sparql_json (dict): Match properties json response directly from the sparql endpoint.
    Returns:
        dict: Formatted json-ld. 
    """
    property_urls = [ binding['b']['value'] for binding in sparql_json['results']['bindings']]    
    
    properties_ld = []
    for url in property_urls:
        split_url = url.split("/")
        name = split_url[-1] if '#' not in split_url[-1] else split_url[-1].split('#')[-1]
        defined_set = url.replace(name, '')
        properties_ld.append({
                        "@id": url,
                        "@type": "DefinedTerm",
                        "name": name,
                        "inDefinedTermSet": defined_set,
                        "url": url,
                        "termCode": name
                    })
    
    return {
        "@context": {
            "@vocab": "https://schema.org/"
        },
        "@graph": [
            {
                "query": "matchProperties",
                "@type": "SearchAction",
                "result": properties_ld
            }
        ]
    }


@app.route("/matchproperties", methods=["GET"])
async def get_match_properties():
    """Return all possible match properties from the knowledge base."""
   # Query to get match properties.
    query = ('select distinct ?b where { <http://vocab.nerc.ac.uk/collection/R22/current/FLOAT_COASTAL/> ?b ?c . '
             'FILTER (CONTAINS(str(?b), "prefLabel") || CONTAINS(str(?b),"altLabel") ||  '
             'CONTAINS(str(?b),"/terms/identifier") ||  CONTAINS(str(?b),"definition")) }'
            )
    async_client = AsyncClient()
    response = await send_query(query, mediatype="application/json", client=async_client)
    await response.aread()
    return get_match_properties_ld(response.json())

@app.route("/matchType", methods=["get"])
async def get_match_types():
    """Return all possible match types."""
    return {
    "@context": "https://schema.org",
    "@type": "ItemList",
    "itemListElement": [
        "exactMatch",
        "proximityMatch",
        "wildcardMatch"
    ],
    "name": "SA matches"
    }


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8004)
