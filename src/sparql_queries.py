import os

from SPARQLWrapper import SPARQLWrapper, JSON


def fts_term(term):
    return f"""
    
    """

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


def get_vocabs_from_sparql_endpoint(query):
    sparql = SPARQLWrapper(
        endpoint=os.getenv("SPARQL_ENDPOINT")
    )
    sparql.setCredentials(
        user=os.getenv("SPARQL_USERNAME", ""),
        passwd=os.getenv("SPARQL_PASSWORD", ""),
    )
    sparql.setReturnFormat(JSON)
    sparql.setQuery(query)
    try:
        return sparql.queryAndConvert()
    except Exception as e:
        print(e)
