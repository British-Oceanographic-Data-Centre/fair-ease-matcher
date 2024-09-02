import logging
import os

from SPARQLWrapper import SPARQLWrapper, JSON
from httpx import AsyncClient
from rdflib import URIRef

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sparql_endpoint = os.getenv("SPARQL_ENDPOINT")
user = os.getenv("SPARQL_USERNAME", "")
passwd = os.getenv("SPARQL_PASSWORD", "")


def find_vocabs_sparql(urns):
    return f"""
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX dcterms: <http://purl.org/dc/terms/>

    SELECT ?collection {{
      {{
        ?concept dcterms:identifier ?urn .
        ?collection skos:member ?concept .
      }}
      UNION
      {{
        ?concept dcterms:identifier ?urn .
        ?concept skos:inScheme ?collection .
      }}
      FILTER(?urn IN (
          {", ".join(f'"{urn}"' for urn in urns)} 
      ))
    }} 
    GROUP BY ?collection
    """


# TODO get rid of and use httpx as below
def get_vocabs_from_sparql_endpoint(query):
    if not sparql_endpoint:
        raise Exception("SPARQL_ENDPOINT not set")
    sparql = SPARQLWrapper(endpoint=sparql_endpoint)
    sparql.setCredentials(
        user=user,
        passwd=passwd,
    )
    sparql.setReturnFormat(JSON)
    sparql.setQuery(query)
    try:
        response = sparql.queryAndConvert()
        return response  # return as tuple to make it hashable and usable with asyncio.gather
    except Exception:
        raise


async_client = AsyncClient(
    auth=(user, passwd) if user else None,
    timeout=30,
)


async def send_query(query: str, mediatype="text/turtle", client=None):
    """Sends a SPARQL query asynchronously."""
    query_rq = client.build_request(
        "POST",
        url=sparql_endpoint,
        headers={"Accept": mediatype},
        data={"query": query},
    )
    response = await client.send(query_rq, stream=True)
    return response


async def tabular_query_to_dict(query: str, context: URIRef = None, client=None):
    """Sends a SPARQL query asynchronously and parses the response into a table format."""
    response = await send_query(query, "application/sparql-results+json", client)
    await response.aread()
    
    return context, response.json()
