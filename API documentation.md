# The Semantic Analyser API

## API Developer Documentation

Swagger / OpenAPI documenation available at <https://semantics.bodc.ac.uk/api/docs>

### Base URL
All API endpoints are accessible under the following base URL:

```
https://semantics.bodc.ac.uk/api/
```

## Endpoints

### 1. Retrieve Vocabularies by Category

**GET** `/categories/{category}/vocabularies`

#### Description
Retrieves vocabularies based on a specific category.

#### Available Categories:
- `instrument`
- `platform`
- `theme`
- `all` (Retrieves all vocabularies across categories)

#### Example Request:
```http
GET https://semantics.bodc.ac.uk/api/categories/platform/vocabularies
```

#### Response:
```json
{
   "@context":{
      "rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
      "skos":"http://www.w3.org/2004/02/skos/core#"
   },
   "@graph":[
      {
         "@type":"SearchAction",
         "query":"/categories/platform/vocabularies",
         "result":[
            {
               "@id":"http://vocab.nerc.ac.uk/collection/R24/current/",
               "@type":"DefinedTermSet",
               "about":"platform",
               "name":"Argo platform maker"
            },
            {
               "@id":"http://vocab.nerc.ac.uk/collection/C17/current/",
               "@type":"DefinedTermSet",
               "about":"platform",
               "name":"ICES Platform Codes"
            }
         ]
      }
   ]
}
```

---

### 2. Retrieve All Categories

**GET** `/categories`

#### Description
Fetches a list of all available categories.

#### Example Request:
```http
GET https://semantics.bodc.ac.uk/api/categories
```

#### Response:
```json
{
   "@context":{
      "@vocab":"https://schema.org/"
   },
   "@graph":[
      {
         "@type":"SearchAction",
         "query":"categories",
         "result":[
            {
               "@id":"http://vocab.nerc.ac.uk/collection/L19/current/SDNKG03/",
               "@type":"DefinedTerm",
               "name":"parameter",
               "termCode":"SDNKG03"
            }
         ]
      }
   ]
}
```

---

### 3. Retrieve Match Types

**GET** `/matchType`

#### Description
Returns available match types for term analysis.

#### Example Request:
```http
GET https://semantics.bodc.ac.uk/api/matchType
```

#### Response:
```json
{
   "@context":"https://schema.org",
   "@type":"ItemList",
   "itemListElement":[
      "exactMatch",
      "proximityMatch",
      "wildcardMatch"
   ],
   "name":"SA matches"
}
```

---

### 4. Retrieve Match Properties

**GET** `/matchproperties`

#### Description
Fetches available properties used for term matching.

#### Example Request:
```http
GET https://semantics.bodc.ac.uk/api/matchproperties
```

#### Response:
```json
{
   "@context":{
      "@vocab":"https://schema.org/"
   },
   "@graph":[
      {
         "@type":"SearchAction",
         "query":"matchProperties",
         "result":[
            {
               "@id":"http://purl.org/dc/terms/identifier",
               "@type":"DefinedTerm",
               "name":"identifier",
               "termCode":"identifier"
            }
         ]
      }
   ]
}
```

---

### 5. Analyse Terms

**POST** `/analyse`

#### Description
Analyzes terms based on the provided match criteria.

#### Headers
```
Content-Type: application/json
```

#### Request Body
```json
{
  "terms": [
    "DYFAMED",
    "EuroSITES",
    "MOORING",
    "Observatory",
    "SmartBay"
  ],
  "matchType": ["exactMatch"],
  "matchProperties": ["altLabel", "prefLabel", "definition"],
  "vocabularies": ["http://vocab.nerc.ac.uk/collection/L06/current/"],
  "category": "platform"
}
```

#### Example Request:
```http
POST https://semantics.bodc.ac.uk/api/analyse
```

#### Response:
```json
{
  "@context": [
    "https://schema.org/",
    {
      "skos": "http://www.w3.org/2004/02/skos/core#"
    }
  ],
  "@graph": [
    {
      "@type": "SearchAction",
      "query": "MOORING",
      "result": [
        {
          "@id": "http://vocab.nerc.ac.uk/collection/L06/current/48/",
          "@type": [
            "DefinedTerm",
            "skos:Concept",
            "CreativeWork"
          ],
          "name": "mooring",
          "matchType": "ExactMatch",
          "termCode": "48"
        }
      ]
    }
  ],
  "search_terms_not_found": [
    "DYFAMED",
    "EuroSITES",
    "SmartBay",
    "Observatory"
  ],
  "stats": {
    "total_number_terms_found": 1
  }
}
```

---

## Notes
Ensure that all API requests use the appropriate HTTP methods and include required parameters or body data where applicable.
