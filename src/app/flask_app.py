import json
import logging
import time
from pathlib import Path
from urllib.parse import urlparse

import traceback

from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from httpx import AsyncClient

from src.analyse import run_methods, run_method_dab_terms
from src.sparql_queries import send_query

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
    query = ('select distinct ?b where { graph <http://vocab.nerc.ac.uk/collection/R22/current/> '
    '{?a ?b ?c . filter (contains(str(?b), "skos") || contains(str(?b), "identifier") )} } limit 100')
    async_client = AsyncClient()
    response = await send_query(query, mediatype="application/json", client=async_client)
    await response.aread()
    return get_match_properties_ld(response.json())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8004)
