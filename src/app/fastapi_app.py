"""Endpoints for the Semantic Analyser."""
import csv
import io
import logging
import os
import time
from typing import Any, Dict
from urllib.parse import urlparse
from collections import defaultdict

from pydantic import BaseModel

import traceback
import requests

from fastapi import FastAPI, APIRouter, Request, Query, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from httpx import AsyncClient

from src.analyse import run_methods, run_method_dab_terms
from src.sparql_queries import get_vocabs_from_sparql_endpoint, send_query
from src.csv2ttl import csv2sssom_ttl
from src.config_loader import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

user = os.getenv("SPARQL_USERNAME", "")
passwd = os.getenv("SPARQL_PASSWORD", "")
fuseki_endpoint = os.getenv("FUSEKI_ENDPOINT", "")

app = FastAPI(
    title="Semantic Analyser API",
    docs_url="/api/docs",    
    openapi_url="/api/openapi.json"
)

router = APIRouter()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

match_properties_map = {
    'identifier': 'id',
    'prefLabel': 'pref',
    'altLabel': 'alt',
    'definition': 'def'
}

categories_map = {
    'parameter': 'parameter',
    'platform': 'platform',
    'instrument': 'instrument',
    'theme': 'keyword',
    'all': ''
}

match_type_map = {
    'exactMatch': 'Exact Match',
    'wildcardMatch': 'Wildcard Match',
    'proximityMatch': 'Proximity Match',    
}

# -------------------------
# Helper functions
# -------------------------

def parse_categories(data: dict) -> dict:
    new_data = {
        "@context": {"@vocab": "https://schema.org/"},
        "@graph": [{"query": "categories", "@type": "SearchAction", "result": []}]
    }
    for result in data["results"]["bindings"]:
        raw_url = urlparse(result["c"]["value"])
        term_code = raw_url.path.strip("/").split("/")[-1]
        in_defined_term_set = f"{raw_url.scheme}://{raw_url.netloc}/{'/'.join(raw_url.path.strip('/').split('/')[:-1])}/"
        item = {
            "@id": result["c"]["value"],
            "@type": "DefinedTerm",
            "name": result["prefLabel"]["value"].lower(),
            "inDefinedTermSet": in_defined_term_set,
            "url": result["c"]["value"],
            "termCode": term_code
        }
        new_data["@graph"][0]["result"].append(item)
    return new_data

def get_match_properties_ld(sparql_json: dict):
    property_urls = [binding['b']['value'] for binding in sparql_json['results']['bindings']]
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
        "@context": {"@vocab": "https://schema.org/"},
        "@graph": [{"query": "matchProperties", "@type": "SearchAction", "result": properties_ld}]
    }

def populate_json_template(category_name: str, json_results: dict) -> dict:
    json_template = {
        "@context": {"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                     "skos": "http://www.w3.org/2004/02/skos/core#"},
        "@graph": [{"query": f"/categories/{category_name}/vocabularies",
                    "@type": "SearchAction",
                    "result": []}]
    }
    for res in json_results["results"]["bindings"]:
        result = {
            "@type": "DefinedTermSet",
            "@id": res["collection"]["value"],
            "name": res.get("title", {}).get("value", ""),
            "about": res.get("aboutList", {}).get("value", "")
        }
        json_template["@graph"][0]["result"].append(result)
    return json_template

# -------------------------
# Endpoints
# -------------------------

class ErrorResponse(BaseModel):
    detail: str

@router.post("/analyse", status_code=200,

responses={
        400: {
            "model": ErrorResponse,
            "description": "Bad Request – The input data is invalid (e.g., missing terms, invalid category, or wrong types).",
            "content": {
                "application/json": {
                    "examples": {
                        "no_data": {"summary": "Missing input", "value": {"detail": "No data provided or invalid JSON"}},
                        "invalid_category": {"summary": "Bad category", "value": {"detail": "Invalid category 'x', see ~/api/categories for valid categories"}},
                        "no_terms": {"summary": "Empty terms", "value": {"detail": "No terms provided"}},
                        "too_many_terms": {"summary": "Too many terms", "value": {"detail": "Number of terms cannot exceed 300"}},
                        "invalid_match_type": {"summary": "Bad match type", "value": {"detail": "Invalid match type, see ~/api/matchType for valid matchTypes"}},
                        "invalid_match_property": {"summary": "Bad match property", "value": {"detail": "Invalid match property, see ~/api/matchproperties for valid match properties"}},
                    }
                }
            },
        },
        500: {
            "model": ErrorResponse,
            "description": "Internal Server Error - Unexpected failure during analysis.",
            "content": {
                "application/json": {
                    "example": {"detail": "Internal error occurred while processing request"}
                }
            },
        },
    },             

)
async def analyse(sa_data: Dict[str, Any] = Body(
         ...,
         example={
             "terms": ["DYFAMED","EuroSITES","MOORING","Observatory","SmartBay"],
             "matchType": ["exactMatch"],
             "matchProperties": ["altLabel", "prefLabel", "definition"],
             "vocabularies": ["http://vocab.nerc.ac.uk/collection/L06/current/"],
             "category": "platform"
         }        
     )
 ):
    """ Analyses terms based on the provided match criteria.

    Fields in JSON request body:

    - **category** *(optional)*  list, restricted values (default all categories from ~/api/categories)
    - **vocabularies** *(optional)*  list, default []
    - **terms** *(required)*  list of terms
    - **exclude_deprecated** *(optional)*  boolean, default false
    - **matchType** *(optional)*  list, (see ~/api/matchType) default ["exactMatch"]
    - **matchProperties** *(optional)*  list, restricted values (default all properties from ~/api/matchproperties)
     
    """        
    if not sa_data:
        raise HTTPException(status_code=400, detail="No data provided or invalid JSON")

    category = sa_data.get("category") or "all"

    if isinstance(category, list) or category not in categories_map:
        raise HTTPException(status_code=400, detail=f"Invalid category '{category}'")
    category = categories_map[category]

    vocabularies = sa_data.get("vocabularies", [])
    if not isinstance(vocabularies, list):
        raise HTTPException(status_code=400, detail="vocabularies should be a list")

    terms = sa_data.get("terms")
    if not terms:
        raise HTTPException(status_code=400, detail="No terms provided")
    if len(terms) > config["max_terms_limit"]:
        raise HTTPException(status_code=400, detail=f"Number of terms cannot exceed {config['max_terms_limit']}")

    match_type_required = sa_data.get("matchType") or ["exactMatch"]
    match_type_required = [match_type_map.get(item) for item in match_type_required if item in match_type_map]
    if not match_type_required:
        raise HTTPException(status_code=400, detail="Invalid match type")

    exclude_deprecated = str(sa_data.get("exclude_deprecated", "false")).lower() == "true"

    match_properties = sa_data.get('matchProperties') or list(match_properties_map.keys())
    match_properties = [match_properties_map.get(item) for item in match_properties if item in match_properties_map]
    if not match_properties:
        raise HTTPException(status_code=400, detail="Invalid match property")

    results_data = {}
    try:
        await run_method_dab_terms(
            "SAterms",
            results_data,
            terms,
            category,
            exclude_deprecated=exclude_deprecated,
            restrict_to_vocabs=vocabularies,
            match_properties=match_properties
        )
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    
    bindings = results_data["SAterms"]["geoDABterms"]["results"]["bindings"]
    terms_not_found = results_data["SAterms"]["geoDABterms"]["search_terms_not_found"]

    final_results = {"@context": ["https://schema.org", {"skos": "http://www.w3.org/2004/02/skos/core#"}], "@graph": []}
    match_count = 0

    for item in bindings:
        matching_type = item.get('MethodSubType', {}).get('value')
        if matching_type not in match_type_required:
            continue
        additional_type = item.get('Categories', {}).get('value', "")
        add_list = [x.strip().lower() for x in additional_type.split(',')]
        cat = category.strip()
        cat = "Theme" if cat == "keyword" else cat
        if not (not cat or cat.lower() in add_list):
            continue
        match_count += 1

        match_property = item['MatchProperty']['value']
        query_str = item['SearchTerm']['value']
        url = item['MatchURI']['value']
        parsed_url = urlparse(url)
        path_parts = parsed_url.path.strip("/").split("/")
        term_code = path_parts[-1]

        if "vocab.nerc.ac.uk" in parsed_url.netloc:
            in_defined_term_set = f"{parsed_url.scheme}://{parsed_url.netloc}/{'/'.join(path_parts[:-1])}/"
        else:
            sparql_query = f"select distinct ?g where {{graph ?g {{<{url}> ?b ?n .}} }} limit 100"
            vocabjson = get_vocabs_from_sparql_endpoint(sparql_query)
            in_defined_term_set = vocabjson["results"]["bindings"][0]["g"]["value"]

        final_results["@graph"].append({
            "query": query_str,
            "@type": "SearchAction",
            "result": [{
                "@type": ["DefinedTerm", "skos:Concept", "CreativeWork"],
                "@id": url,
                "name": item['MatchTerm']['value'],
                "additionalType": additional_type,
                "inDefinedTermSet": in_defined_term_set,
                "url": url,
                "termCode": term_code,
                "skos:deprecated": "false" if item["Status"]["value"] in "Accepted" else "true",
                "matchType": matching_type,
                "matchProperty": match_property
            }]
        })

    grouped_results = defaultdict(list)
    for item in final_results["@graph"]:
        grouped_results[item["query"]].extend(item["result"])

    grouped_json = {
        "@context": final_results["@context"],
        "@graph": [{"@type": "SearchAction", "query": q, "result": r} for q, r in grouped_results.items()],
        "stats": {"total_number_terms_found": match_count},
        "search_terms_not_found": terms_not_found
    }

    return grouped_json


@router.post("/process-geodab-terms", status_code=200, include_in_schema=False)
async def process_geodab_terms(request: Request,
                               match_props: str = Query(None),
                               exclude_deprecated: str = Query("false")):
    data = await request.json()
    restrict_to_theme = data.get('metadata')
    terms = data.get('terms')
    vocabs = data.get('vocabs')
    responses = {}
    doc_name = 'geoDabTerms'
    match_properties = match_props.split(",") if match_props else None
    exclude_deprecated = exclude_deprecated.lower() == "true"

    try:
        await run_method_dab_terms(
            doc_name,
            responses,
            terms,
            restrict_to_theme,
            exclude_deprecated=exclude_deprecated,
            restrict_to_vocabs=vocabs,
            match_properties=match_properties
        )
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    return responses

@router.post("/process-metadata", status_code=200, include_in_schema=False)
async def process_metadata(request: Request,
                           Methods: str = Query(None),
                           Restrict_to_Themes: str = Query(None),
                           match_props: str = Query(None),
                           exclude_deprecated: str = Query("false")):
    start_time = time.time()
    data = await request.json()
    analysis_methods = Methods.split(",") if Methods else ["xml", "full", "netcdf"]
    restrict_to_themes = Restrict_to_Themes.split(",") if Restrict_to_Themes else None
    match_properties = match_props.split(",") if match_props else None
    exclude_deprecated = exclude_deprecated.lower() == "true"
    responses = {}
    threshold = data.get("threshold")
        
    # run XML methods
    if ("xml" in analysis_methods) or ("full" in analysis_methods):
        for doc_name, xml in data.get("xml").items():
            try:
                await run_methods(
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
                traceback.print_exc()                
                raise HTTPException(status_code=500, detail=str(e))                


    for doc_name, file in (data.get("netcdf") or {}).items():
        doc_data = file.read()
        try:
            await run_methods(
                doc_name,
                analysis_methods,
                responses,
                threshold,
                doc_data,
                restrict_to_themes,
                "NETCDF",
                exclude_deprecated=exclude_deprecated,
                match_properties=match_properties
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    logger.info(f"Time taken: {time.time() - start_time}")
    return responses


@router.get("/config", status_code=200, include_in_schema=False)
async def get_config():
    return config


@router.post("/upload_mappings", status_code=200, include_in_schema=False)
async def upload_mappings(request: Request):
    try:
        data = await request.json()
        raw_csv = data.get("csv")
        if not raw_csv:
            raise HTTPException(status_code=400, detail="Missing 'csv' field")
        csv_reader = csv.DictReader(io.StringIO(raw_csv))
        ttl_data = csv2sssom_ttl(csv_reader)
        response = requests.post(
            fuseki_endpoint,
            params={"graph": "https://mappings"},
            data=ttl_data.encode("utf-8"),
            headers={"Content-Type": "text/turtle"},
            auth=(user, passwd)
        )
        if response.status_code not in (200, 201):
            raise HTTPException(status_code=500, detail=f"Fuseki error: {response.status_code} - {response.text}")
        return {"message": "Success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/vocab-list", status_code=200, include_in_schema=False)
async def get_vocab_list(category: str = Query(...)):
    query = f"""
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    SELECT distinct ?collection ?title where {{
        graph <https://themes> {{ ?collection ?b ?c . }}
        ?c skos:prefLabel ?p .
        filter regex(str(?p), "{category}") .
        OPTIONAL {{ ?collection skos:prefLabel ?title }}
    }} limit 100
    """
    async with AsyncClient() as client:
        resp = await send_query(query, mediatype="application/json", client=client)
        await resp.aread()
        return resp.json()


@router.get("/categories", status_code=200)
async def get_categories():
    """Fetches a list of all available categories."""
    query = """
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    select distinct ?c ?prefLabel ?l where { graph <https://themes> {?a <http://www.w3.org/ns/dcat#theme> ?c .
        ?c <http://www.w3.org/2000/01/rdf-schema#label>  ?prefLabel
    } } limit 100"""
    async with AsyncClient() as client:
        resp = await send_query(query, mediatype="application/json", client=client)
        await resp.aread()
        return parse_categories(resp.json())


@router.get("/matchproperties", status_code=200)
async def get_match_properties():
    """Fetches available properties used for term matching."""
    query = ('select distinct ?b where { <http://vocab.nerc.ac.uk/collection/R22/current/FLOAT_COASTAL/> ?b ?c . '
             'FILTER (CONTAINS(str(?b), "prefLabel") || CONTAINS(str(?b),"altLabel") ||  '
             'CONTAINS(str(?b),"/terms/identifier") ||  CONTAINS(str(?b),"definition")) }')
    async with AsyncClient() as client:
        resp = await send_query(query, mediatype="application/json", client=client)
        await resp.aread()
        return get_match_properties_ld(resp.json())


@router.get("/matchType", status_code=200)
async def get_match_types():
    """Returns available match types for term analysis."""
    return {
        "@context": "https://schema.org",
        "@type": "ItemList",
        "itemListElement": ["exactMatch", "proximityMatch", "wildcardMatch"],
        "name": "SA matches"
    }


@router.get("/categories/{categoryName}/vocabularies", status_code=200)
async def get_vocabs_by_category(categoryName: str):
    """Retrieves vocabularies based on a specific category.
    
    Available categories

    - instrument
    - platform
    - theme
    - all (Retrieves all vocabularies across categories)
    
    """
    async with AsyncClient() as client:
        if categoryName.lower() == "all":
            query = f"""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX dc: <http://purl.org/dc/elements/1.1/>
            PREFIX dct: <http://purl.org/dc/terms/>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            SELECT  ?collection ?title (GROUP_CONCAT(DISTINCT ?about; SEPARATOR=", ") AS ?aboutList) WHERE {{
                {{ GRAPH <https://themes> {{ ?collection ?b ?c . }} }}
                UNION 
                {{ GRAPH <https://w3id.org/semanticanalyser/system-graph> {{ 
                    ?collection a <https://w3id.org/semanticanalyser/Vocabulary> .
                    ?collection ?b ?c .
                }} }}
                ?c skos:prefLabel ?about .
                OPTIONAL {{ ?collection skos:prefLabel ?title }}
                OPTIONAL {{ ?collection dc:title ?title }}
                OPTIONAL {{ ?collection rdfs:label ?title }}
            }} 
            GROUP BY ?collection ?title
            """
        else:
            query = f"""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX dc: <http://purl.org/dc/elements/1.1/>
            PREFIX dct: <http://purl.org/dc/terms/>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            SELECT  ?collection ?title (GROUP_CONCAT(DISTINCT ?about; SEPARATOR=", ") AS ?aboutList) WHERE {{
                {{ GRAPH <https://themes> {{ ?collection ?b ?c . }} }}
                UNION 
                {{ GRAPH <https://w3id.org/semanticanalyser/system-graph> {{ 
                    ?collection a <https://w3id.org/semanticanalyser/Vocabulary> .
                    ?collection ?b ?c .
                }} }}
                ?c skos:prefLabel ?about .
                FILTER REGEX(STR(?about), '{categoryName}', "i") .
                OPTIONAL {{ ?collection skos:prefLabel ?title }}
                OPTIONAL {{ ?collection dc:title ?title }}
                OPTIONAL {{ ?collection rdfs:label ?title }}
            }} 
            GROUP BY ?collection ?title
            """
        
        resp = await send_query(query, mediatype="application/json", client=client)
        await resp.aread()
        return populate_json_template(categoryName, resp.json())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8007, reload=True)

app.include_router(router, prefix="/api")    