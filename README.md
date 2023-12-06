# omop2pheno

This repository converts the Observational Medical Outcomes Partnership Common Data Model ([OMOP CDM](https://www.ohdsi.org/data-standardization/)) to the Global Alliance for Genomics and Health ([GA4GH](https://www.ga4gh.org/#/)) [Phenopackets](http://phenopackets.org/) schema.

Accompanying manuscript under review
* This repository is part of study that aims to promote interoperability in precision medicine and translational research by aligning the OMOP and Phenopackets data models.

This work expands on prior conversion from OMOP to Phenopackets developed during a hackathon, that can be found here: https://github.com/phenopackets/omop-exporter

# Repository Guide
* **SQL Extraction** SQL files to extract data from an OMOP CDM structured database can be found in `SQL Files` and are organized by OMOP table 
  * This repository assumes that clinical data that needs to be mapping is in the format of OMOP CDM and retrievable via SQL database connection.


`SupplementalFiles` contain the detailed mapping of domains from OMOP to Phenopackets. 

`convert_pheno.py` 

is to explore model alignment by characterizing the common data models using a newly developed data transformation process and evaluation method. Second, using OMOP normalized clinical data, we evaluate the mapping of real-world patient data to Phenopackets. We evaluate the suitability of Phenopackets as a patient data representation for real-world clinical cases. 
