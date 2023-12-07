# omop2pheno

This repository converts the Observational Medical Outcomes Partnership Common Data Model ([OMOP CDM](https://www.ohdsi.org/data-standardization/)) to the Global Alliance for Genomics and Health ([GA4GH](https://www.ga4gh.org/#/)) [Phenopackets](http://phenopackets.org/) schema.

⏳ Accompanying manuscript under review
* This repository is part of study that aims to promote interoperability in precision medicine and translational research by aligning the OMOP and Phenopackets data models.

Note: This work expands on prior conversion from OMOP to Phenopackets developed that can be found here: https://github.com/phenopackets/omop-exporter

## Repository Guide
* **Mappings** Details on the mappings of OMOP to Phenopackets can be found in the `SupplementalFiles`. For further reference on the attributes of each model, see [Phenopackets](https://phenopacket-schema.readthedocs.io/en/latest/index.html) or [OMOP](https://ohdsi.github.io/CommonDataModel/cdm53.html) documentation.
* **SQL Extraction** SQL files to extract data from an OMOP CDM structured database can be found in `SQL Scripts` and are organized by OMOP table 
  * This repository assumes that clinical data that needs to be mapping is in the format of OMOP CDM and retrievable via SQL database connection.
* **OMOP2Pheno Transformation** `convertPheno.py` provides all necesary functions to convert OMOP to Phenopacket data including: extract patient data according to the `SQL Scripts`, transforming the data as needed to conform to Phenopackets specifications, semantic type filtering (see below<Semantic Type Filtering> , and generating a Phenopacket entity.
* **Notebook Implementation**  `PhenopacketsConverision.ipynb` implements all necessary steps from `convert_pheno.py`. The notebook takes as input SQL database connection details and the person identifiers (pid) for which you would like to convert data. (Supports variable number of PIDs, >=1)

## Semantic Type Filtering
There are certain domains (high-level categories) in the two data models that do not have clear correspondence, namely OMOP's [_Condition_](https://ohdsi.github.io/CommonDataModel/cdm53.html#CONDITION_OCCURRENCE) includes concepts that best align with either Phenopackets [_Disease_](https://phenopacket-schema.readthedocs.io/en/latest/disease.html) or [_PhenotypicFeature_](https://phenopacket-schema.readthedocs.io/en/latest/phenotype.html). To resolve this ambiguity in alignment, we incorporate semantic type filtering leveraging tools provided by the Unified Medical Language System ([UMLS](https://www.nlm.nih.gov/research/umls/index.html)). 

## Contact
https://people.dbmi.columbia.edu/~chw7007/
