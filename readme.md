# Fair-ease-matcher

The backend for the Semantic Analyzer <https://semantics.bodc.ac.uk/>

This repository has been forked from <https://github.com/Kurrawong/fair-ease-matcher> and is now
based on the Python FastAPI framework for faster and more scalable APIs.

The production API documentation can be accessed at <https://semantics.bodc.ac.uk/api/docs>

## Running locally (Linux based OS)

### Environment variables

Sparql requests are made to BODC's servers, therefore the following environment variables 
are required (For access refer to <https://gitlab.com/nocacuk/BODC/software/fuseki-fair-ease/>)

- SPARQL_ENDPOINT
- SPARQL_USERNAME
- SPARQL_PASSWORD

On terminal run (Install any missing dependencies reported)

`uvicorn src.app.fastapi_app:app --host localhost --port=8007`

Access and test api <http://localhost:8007/api/docs>

## Future Development - Notes

- Clean up of code - lots of redundancy from original forked repo.
- Dockerize
- Incoporate Tox and Poetry for Test, Dependency and Build management
