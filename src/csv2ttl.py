import csv
from datetime import datetime
from pathlib import Path

def csv2sssom_ttl(reader: csv.DictReader) -> str:
    # Define RDF prefixes
    prefixes = """@prefix sssom: <https://w3id.org/sssom/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix local: <https://www.local.com/> .
        @prefix semapv: <https://w3id.org/semapv/vocab/>  .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix dcat: <http://www.w3.org/dcat/> .
    """

    # MappingSet metadata
    mapping_set_id = "BCWODC17Platforms"
    mapping_set_desc = (
        "Manual Mappings for Blue Cloud project between WOD platforms and C17 codes"
    )

    today = datetime.today().strftime('%Y-%m-%d')
    mapping_entries = []
    mapping_ids = []
            
    for row in reader:
        subj_id = row.get("subject_id", "").strip()
        obj_id = row.get("object_id", "").strip()
        subj_label = row.get("subject_label", "").strip()
        creator_id = row.get("creator_id", "orcid:0000-0000-0000-0000").strip()
        predicate_id = row.get("predicate_id", "owl:sameAs").strip()
        theme = row.get("theme", "https://vocab.nerc.ac.uk/collection/L19/current/19/").strip()
        justification = row.get("mapping_justification", "semapv:ManualMapping").strip()
        map_date = row.get("mapping_date", today).strip()

        #local_subj = subj_id.split("/")[-1]
        local_obj = obj_id.split("/")[-1]        
        map_id = f"{subj_id}_{local_obj}"
        mapping_ids.append(f"local:{map_id}")

        block = f"""local:{map_id} a sssom:Mapping; 
            sssom:subject_id local:{subj_id}; 
            sssom:creator_id "{creator_id}"^^xsd:string ;
            sssom:mapping_date "{map_date}"^^xsd:date ;
            sssom:mapping_justification {justification};
            sssom:object_id <{obj_id}> ;
            sssom:predicate_id {predicate_id};
            dcat:theme <{theme}>;
            sssom:subject_label "{subj_label}"^^xsd:string .\n"""
        mapping_entries.append(block)

        # MappingSet header
        mappings_list = ",".join(mapping_ids)
        mapping_set_block = f"""local:{mapping_set_id} a sssom:MappingSet;
                            sssom:mapping_set_id "{mapping_set_id}";
                            sssom:mapping_set_description "{mapping_set_desc}";
                            sssom:mappings {mappings_list} .\n\n"""

    # Final RDF output
    output = prefixes + mapping_set_block + "\n".join(mapping_entries)        
    return output
