"""
convertPheno takes in patient data in the OMOP CDM format and transforms that data to a Phenopacket for each patient. 
- Functions handle each Phenopacket top-level entity (e.g., Treatment, Procedure) separately. 
- There are four main groups of functions: 
    - SQL QUERIES read in SQL data from an OMOP CDM databse
    - PARSING parse SQL output to a dictionary 
    - FORMATTING make necessary conversions and transformations from OMOP to Phenopacket data
    - PHENOPACKET CREATION generate and save the Phenopacket data  
"""

from datetime import datetime

from google.protobuf.json_format import Parse, MessageToJson
from google.protobuf.timestamp_pb2 import Timestamp

from phenopackets import Phenopacket,Individual, Disease, Sex, PhenotypicFeature, OntologyClass,Treatment, \
		TimeElement,Procedure,VitalStatus,Quantity,Measurement,Value, MedicalAction, DoseInterval


import operator
import pandas as pd

import time
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

# SQL QUERIES
def get_individual_query(pid, db):
    logging.info(f"Extracting individual data")
	
    query = """select p3.id,
       null as alternate_ids,
       p3.date_of_birth,
       max(vo.visit_start_date) as time_at_last_encounter,
       p3.vital_status,
       p3.sex,
       null as karyotypic_sex,
       null as gender,
       'NCBITaxon:9606' as taxonomy_id,
       'human' as taxonomy_label
       from (
           select p2.id,p2.date_of_birth, p2.sex,
           (case when p2.death_pid is null then 0 else 2 end) as vital_status
           from (
            select p1.*, d.person_id as death_pid
            from
            (select p.person_id as id,
            p.birth_datetime as date_of_birth,
            (case when p.gender_concept_id is null then 0
            when p.gender_concept_id = 8532 then 1
            when p.gender_concept_id = 8507 then 2
            else 3 end) as sex
            from """ + db + """person p
            WHERE p.person_id in """ + pid + """) p1
            left join """ + db + """death d
            on p1.id = d.person_id) p2) p3
            left join """ + db + """visit_occurrence vo
            on p3.id = vo.person_id
            group by p3.id,p3.sex,p3.date_of_birth,p3.vital_status;"""
    return query

def get_vitalstatus_query(pid, db):
    logging.info(f"Extracting vital status data")
    query =  """select * from (select id as person_id,
       (case when pid_death.death_pid is null then 0 else 2 end) as vital_status,
       pid_death.time_of_death,
       null as cause_of_death_id,
       null as cause_of_death_label
       from (
        select p.person_id as id, d.person_id as death_pid, d.death_datetime as time_of_death
        from """ + db + """person p left join death d
        on p.person_id = d.person_id
        where p.person_id in """ + pid + """) pid_death) p1
        group by person_id, vital_status, time_of_death, cause_of_death_id, cause_of_death_label;"""
    return query 

def get_condition_query(pid, db, ohdsi_db):
    logging.info(f"Extracting condition data")
    query = """select a.person_id,
      a.term_id,
      a.term_label,
      a.condition_source_value,
      a.excluded,
      a.onset_timestamp,
      a.resolution,
      a.clinical_tnm_finding_id,
      a.clinical_tnm_finding_label,
      case when a.primary_site_concept is null then null
          else concat(primary_site_vocab, ':', primary_site_code)
          end as primary_site_id,
      a.primary_site_label, 
      a.concept_id
    from
    (select co.person_id,


        -- TERM
        concat(c.vocabulary_id,':',c.concept_code) as term_id,
        c.concept_name as term_label,
        c.concept_id,
        co.condition_source_value,


        -- Excluded
        0 as excluded,


        -- Onset
        co.condition_start_date as onset_timestamp,
        co.condition_end_date as resolution,


        -- Clinical_tnm_finding
        null as clinical_tnm_finding_id,
        null as clinical_tnm_finding_label,


        -- Primary Site
        cr.concept_id_2 as primary_site_concept,
        c2.concept_name as primary_site_label,
        c2.vocabulary_id as primary_site_vocab,
        c2.concept_code as primary_site_code


    from """ + db + """condition_occurrence co
    left join """ + ohdsi_db + """concept c
    on co.condition_concept_id = c.concept_id
    left join """ + ohdsi_db + """concept_relationship cr
    on cr.concept_id_1 = co.condition_concept_id and cr.relationship_id = 'Has finding site'
    left join """ + ohdsi_db + """concept c2
    on cr.concept_id_2 = c2.concept_id
    where person_id in """ + pid + """) a;"""

    return query

def get_phenofeature_query(pid, db, ohdsi_db):
    logging.info(f"Extracting phenotypic feature data")
    query = """select a.person_id, 
    a.type_id,
      a.type_label,
      case when a.value_as_concept_id is null then null
          else concat(a.modifier_vocab, ':', a.modifier_code) end as modifier_id,
      a.modifier_label,
      case when a.value_as_string is null then null
          else a.value_as_string end as description,
	  a.observation_datetime as onset_timestamp,
      concat('P',datediff(yyyy,a.birth_datetime,a.observation_datetime),'Y') as onset_age
    from (select obs.person_id, 
	    c.concept_name as type_label,
        concat(c.vocabulary_id, ':', c.concept_code) as type_id,
        obs.observation_concept_id,
        obs.value_as_concept_id,
        c2.concept_name as modifier_label,
        c2.vocabulary_id as modifier_vocab,
        c2.concept_code as modifier_code,
        obs.value_as_string,
        obs.value_as_number,
        obs.observation_datetime,
        p.birth_datetime
    from """ + db + """observation obs
    left join """ + ohdsi_db + """concept c
    on obs.observation_concept_id = c.concept_id
    left join person p
    on obs.person_id = p.person_id
    left join """ + ohdsi_db + """concept c2
    on obs.value_as_concept_id = c2.concept_id
    where obs.person_id in """ + pid + """) a;"""

    return query 

def get_measurement_query(pid, db, ohdsi_db):
    logging.info(f"Extracting measurement data")
    query =  """select m.person_id,
       m.measurement_concept_id,
     -- ASSAY
    concat(c.vocabulary_id,':',c.concept_code) as assay_id,
    c.concept_name as assay_label,
    -- VALUE - QUANTITY/NUMERIC
    m.value_as_number,
     -- VALUE - ORDINAL/Categorical/OntologyClass
    concat(c3.vocabulary_id,':',c3.concept_code) as value_id,
    c3.concept_name as value_label,

    -- RANGe
    m.range_low,
    m.range_high,


    -- time_observed
    m.measurement_datetime,
    -- UNIT
    concat(c2.vocabulary_id,':',c2.concept_code) as unit_id,
    c2.concept_name as unit_label,

    c2.concept_id,
    m.unit_source_value,

    m.visit_occurrence_id,

    row_number() over (partition by m.person_id, m.measurement_datetime, m.visit_occurrence_id order by m.person_id) as row_number
    FROM """ + db + """measurement m
    left join """ + ohdsi_db + """concept c on c.concept_id = m.measurement_concept_id
    left join """ + ohdsi_db + """concept c2 on c2.concept_id = m.unit_concept_id
    left join """ + ohdsi_db + """concept c3 on c3.concept_id = m.value_as_concept_id
    where m.person_id in """ + pid + """;"""

    return query

def get_treatment_query(pid, db, ohdsi_db):
    logging.info(f"Extracting treatment data")
    query = get_treatment_query = """select a.person_id,
      a.agent_id,
      a.agent_label,
      case when route_administration_code is null then null
          else concat(a.route_administration_vocab,':',a.route_administration_code) end as route_of_adminsitration_id,
      a.route_of_adminsitration_label,
      case when quantity_code_id is null then null
          else concat(a.quantity_vocab_id,':',a.quantity_code_id) end as quantity_id,
     a.quantity_unit_label,
     a.quantity_value,
     a.interval_start,
     a.interval_end,
     a.drug_type_id,
     a.sched_freq
    from
    (select de.person_id,
        -- Agent
        concat(c.vocabulary_id,':',c.concept_code) as agent_id,
        c.concept_name as agent_label,


        -- Route of administration
        c2.vocabulary_id as route_administration_vocab,
        c2.concept_code as route_administration_code,
        c2.concept_name as route_of_adminsitration_label,

        -- schedule_Freq
        CASE WHEN de.days_supply = 0 THEN 0 ELSE CEILING(de.quantity / de.days_supply)  END AS sched_freq,

        -- Dose Intervals
        -- dose intervals: dosage
        ds.amount_value as quantity_value,
        c3.vocabulary_id as quantity_vocab_id,
        c3.concept_code as quantity_code_id,
        c3.concept_name as quantity_unit_label,


        -- dose intervals: interval start/end
        de.drug_exposure_start_date as interval_start,
        dateadd(day, de.days_supply,de.drug_exposure_start_date) as interval_end,


        -- Drug_type
        c4.concept_id as drug_type_id,
        c4.concept_name as drug_type_name


    from """ + db + """drug_exposure de
    left join """ + ohdsi_db + """concept c
    on c.concept_id = de.drug_concept_id
    left join """ + ohdsi_db + """concept c2 on c2.concept_id = de.route_concept_id
    left join """ + ohdsi_db + """drug_strength ds -- joining for dosage based on drug concept id
    on ds.drug_concept_id = de.drug_concept_id
    left join """ + ohdsi_db + """concept c3
    on c3.concept_id = ds.amount_unit_concept_id-- joining drug strength concept id to concept table to get vocab id and label (name) of unit ;
    left join """ + ohdsi_db + """concept c4
    on c4.concept_id = de.drug_type_concept_id
    where person_id in """ + pid + """) a;
    """
    return query

def get_procedure_query(pid, db, ohdsi_db):
    logging.info(f"Extracting procedure data")
    query = """select a.person_id,
      a.code_id,
      a.code_label,
      case when a.body_site_concept_id is null then null
          else concat(body_site_vocab_id,':',body_site_concept_id) end as body_site_id,
      a.body_site_label,
      a.procedure_datetime as performed_timestamp,
      concat('P',datediff(yyyy,a.birth_datetime,a.procedure_datetime),'Y') as performed_age
    from
    (select po.person_id,


        -- code
        concat(c.vocabulary_id, ':', c.concept_code) as code_id,
        c.concept_name as code_label,


        c2.concept_id as body_site_concept_id,
        c2.vocabulary_id as body_site_vocab_id,
        c2.concept_name as body_site_label,


        po.procedure_datetime,
        p.birth_datetime


    from """ + db + """procedure_occurrence po
    left join """ + ohdsi_db + """concept c
    on c.concept_id = po.procedure_concept_id
    left join """ + ohdsi_db + """concept_relationship cr -- getting concept id of body site
    on cr.concept_id_1 = po.procedure_concept_id and cr.relationship_id = 'Has proc site'
    left join """ + ohdsi_db + """concept c2 -- getting vocab id, concept code, and name of body site
    on c2.concept_id = cr.concept_id_2
    left join """ + db + """person p 
    on po.person_id = p.person_id
    where po.person_id in """ + pid + """) a;"""

    return query

# PARSING 
def parse_Individual(records):
	fields=["id","alternate_ids","date_of_birth","time_at_last_encounter","vital_status","sex","karyotypic_sex","gender","taxonomy_id","taxonomy_label"]
	indivs = []

	for r in records:
		indivs.append({i:j for i,j in zip(fields,r) if j is not None })
	
	return indivs

def parse_VitalStatus(records):
	fields=["person_id","vital_status","time_of_death","cause_of_death_id","cause_of_death_label"]
	vitals = []

	for r in records:
		vitals.append({i:j for i,j in zip(fields,r) if j is not None })

	return vitals 	

def parse_Conditions(records, pheno_map):
	fields_con = ["person_id","term_id","term_label","condition_source_value","excluded","onset_timestamp","resolution","clinical_tnm_finding_id","clinical_tnm_finding_label","primary_site_id","primary_site_label","concept_id"]
	fields_phe = ["person_id","type_id","type_label","condition_source_value","excluded","onset_timestamp","resolution","clinical_tnm_finding_id","clinical_tnm_finding_label","modifier_id","modifier_label","concept_id"]

	diseases = []
	features = []

	values_nono=[None,"None:No matching concept","No matching concept"]
	for r in records:
			if(r[11] in pheno_map):
				features.append({i:j for i,j in zip(fields_phe,r) if j not in values_nono })
			else:
				diseases.append({i:j for i,j in zip(fields_con,r) if j not in values_nono })
	return diseases, features

def parse_PhenoFeatures(records):
	fields = ["person_id","type_id","type_label","modifier_id","modifier_label","description","onset_timestamp","onset_age"]

	phenoFeatures = []
	values_nono=[None,"None:No matching concept","No matching concept"]

	for r in records:
		phenoFeatures.append({i:j for i,j in zip(fields,r) if j not in values_nono })

	return phenoFeatures

def parse_Measurements(records):
	fields=["person_id","measurement_concept_id","assay_id","assay_label","value_as_number","value_id","value_label","range_low","range_high","measurement_datetime","unit_id","unit_label","concept_id","unit_source_value","visit_occurrence_id","row_number"]

	measurements=[]
	values_nono=[None,"None:No matching concept","No matching concept"]
	for r in records:
		measurements.append({i:j for i,j in zip(fields,r) if j not in values_nono })
	return measurements

def parse_Treatments(records):
	fields = ["person_id","agent_id","agent_label","route_of_administration_id","route_of_administration_label","quantity_id","quantity_unit_label","quantity_value","interval_start","interval_end","drug_type_id","sched_freq"]

	treatments = []
	values_nono=[None,"None:No matching concept","No matching concept"]

	for r in records:
		treatments.append({i:j for i,j in zip(fields,r) if j not in values_nono })

	return treatments

def parse_Procedures(records):
	fields = ["person_id","code_id","code_label","body_site_id","body_site_label","performed_timestamp","performed_age"]

	procedures = []
	values_nono=[None,"None:No matching concept","No matching concept"]

	for r in records:
		procedures.append({i:j for i,j in zip(fields,r) if j not in values_nono })

	return procedures

# TRANSFROMATION
def createDictIndividual(mydict,vsdict):
	idict_all = {}

	# Variables for logging
	discarded = 0
	alternate_ids = 0
	date_of_birth = 0 
	time_at_last_encounter = 0
	sex = 0
	vital_status = 0

	for i in mydict: 
		pid = i['id']
		idict={}
		
		if('id' in i):
			idict['id']=i['id']
		else:
			discarded += 1
			continue
		
		if('alternate_ids' in i):
			idict['alternate_ids']=i['alternate_ids']
			alternate_ids += 1
		if('date_of_birth' in i):
			idict['date_of_birth']=convert_time(i['date_of_birth'])
			date_of_birth += 1
		if('time_at_last_encounter' in i):
			idict['time_at_last_encounter']=convert_time(i['time_at_last_encounter'])
			time_at_last_encounter += 1
		if('sex' in i):
			sex += 1
			if(i['sex']==0):
				idict['sex']='UNKNOWN_SEX'
			elif(i['sex']==1):
				idict['sex']='FEMALE'
			elif(i['sex']==2):
				idict['sex']='MALE'
			elif(i['sex']==3):
				idict['sex']='OTHER_SEX'
		if('taxonomy_id' in i):
			idict['taxonomy']={'id':i['taxonomy_id'],'label':i['taxonomy_label']} 

		if('vital_status' in i and i['vital_status']!=0):
			vital_status += 1 

			tempdict={}
			if('vital_status' in vsdict):
				tempdict={}
				if(vsdict['vital_status']==0):
					tempdict['status']='UNKNOWN_STATUS'
				elif(vsdict['vital_status']==1):
					tempdict['status']='ALIVE'
				elif(vsdict['vital_status']==2):
					tempdict['status']='DECEASED'            
			if('time_of_death' in vsdict):
				tempdict['time_of_death']=convert_time(vsdict['time_of_death'])
			if('cause_of_death_id' in vsdict):
				tempdict['cause_of_death']={'id':vsdict['cause_of_death_id'],'label':vsdict['cause_of_death_label']}

			idict['vital_status']=tempdict
			
		idict['id'] = str(idict['id']).encode()
		
		idict_all[pid] = idict

	logging.info(f"Individual - Original - records fetched - Total: {len(mydict)}")
	logging.info(f"Individual - Discarded - based on absence of - id: {discarded}")    
	logging.info(f"Individual - Final - records included - Total: {len(idict_all)}")
	logging.info(f"Individual - Final - records with completed - date_of_birth: {date_of_birth}")
	logging.info(f"Individual - Final - records with completed - time_at_last_encounter: {time_at_last_encounter}")
	logging.info(f"Individual - Final - records with completed - sex: {sex}")
	logging.info(f"Individual - Final - records with completed - vital_status: {vital_status}")

	if not((len(idict_all) - discarded) == (len(mydict))):
		logging.info(f"Discrepancy in counts for Individual")

	return idict_all

def createListDictConditions(md):
    ilist_dict = {}
    ilist = []

    discarded = 0
    resolution = 0 
    primary_site = 0 

    discarded_primary_site = 0 
    discarded_resolution = 0 

    for m in md:
        pid = m['person_id']

        if(pid not in ilist_dict):
            ilist_dict[pid] = []

        if not('term_id' in m):
            ilist.append({'discarded':'yes'})
            ilist_dict[pid].append({'discarded':'yes'})
            discarded += 1 
            
            if('resolution' in m): discarded_resolution += 1
            if('primary_site' in m): discarded_primary_site += 1

            continue

        tempdict = {}
        tempdict['term'] = {'id':m['term_id'],'label':m['term_label']}
        
        time_temp = convert_time(m['onset_timestamp']) 
        
        tempdict['onset'] = {'timestamp':time_temp} # add date conversion 
        
        if ('resolution' in m):
            resolution_temp = convert_time(m['resolution'])
            tempdict['resolution'] = {'timestamp':resolution_temp} # add date conversion
            resolution += 1
        
        if ('primary_site_id' in m):
            tempdict['primary_site'] = {'id':m['primary_site_id'],'label':m['primary_site_label']}
            primary_site += 1

        ilist.append(tempdict)
        ilist_dict[pid].append(tempdict)
        
    logging.info(f"Condition - Original -  records fetched - Total: {len(ilist)}")
    logging.info(f"Condition - Discarded - Total, based on absence of - term: {discarded}")    
    logging.info(f"Condition - Discarded - Resolution, based on absence of - term: {discarded_resolution}")    
    logging.info(f"Condition - Discarded - Primary site, based on absence of - term: {discarded_primary_site}")    
    logging.info(f"Condition - Final - records included - Total: {len([i for i in ilist if 'discarded' not in i])}")
    logging.info(f"Condition - Final - records with completed (Dict) - resolution: {resolution}")
    logging.info(f"Condition - Final - records with completed (Dict) - primary_site: {primary_site}")

    if not((len(ilist) - discarded) == (len([i for i in ilist if 'discarded' not in i]))):
        logging.info(f"Discrepancy in counts for PhenotypicFeature from Condition")

    return ilist_dict
        
def createListDictPhenoFeature(md, flag = 'observation'):
	ilist = []
	ilist_dict = {}
	discarded = 0 
	modifier = 0 
	resolution = 0
	description = 0 

	discarded_modifier = 0 
	discarded_resolution = 0
	discarded_description = 0 

	for m in md:
		pid = m['person_id']
		
		if(pid not in ilist_dict):
			ilist_dict[pid] = []

		if not('type_id' in m):
			ilist.append({'discarded':'yes'})
			ilist_dict[pid].append({'discarded':'yes'})
			discarded += 1
			
			if('modifier_id' in m): discarded_modifier += 1 
			if('resolution' in m): discarded_resolution += 1 
			if('description' in m): discarded_description += 1

			continue

		tempdict = {}
		tempdict['type'] = {'id':m['type_id'],'label':m['type_label']}

		if('modifier_id' in m):
			tempdict['modifiers'] = {'id':m['modifier_id'],'label':m['modifier_label']}
			modifier += 1

		timstamp_temp = convert_time(m['onset_timestamp'])
		tempdict['onset'] = {'timestamp':timstamp_temp}
		
		if('resolution' in m):
			resolution_temp = convert_time(m['resolution'])
			tempdict['resolution'] = {'timestamp':resolution_temp}
			resolution += 1

		if('description' in m):
			tempdict['description'] = m['description']
			description += 1 

		ilist.append(tempdict)
		ilist_dict[pid].append(tempdict)
		
	if(flag == 'condition'):
		logging.info(f"PhenotypicFeature (from Condition) - Original -  records fetched - Total: {len(ilist)}")
		logging.info(f"PhenotypicFeature (from Condition) - Discarded - Total, based on absence of - type: {discarded}")    
		logging.info(f"PhenotypicFeature (from Condition) - Discarded - Modifier, based on absence of - type: {discarded_modifier}")    
		logging.info(f"PhenotypicFeature (from Condition) - Discarded - Resolution, based on absence of - type: {discarded_resolution}")    
		logging.info(f"PhenotypicFeature (from Condition) - Discarded - Description, based on absence of - type: {discarded_description}")    
		logging.info(f"PhenotypicFeature (from Condition) - Final - records included - Total: {len([i for i in ilist if 'discarded' not in i])}")
		logging.info(f"PhenotypicFeature (from Condition) - Final - records with completed (Dict) - modifier: {modifier}")
		logging.info(f"PhenotypicFeature (from Condition) - Final - records with completed (Dict) - resolution: {resolution}")
		logging.info(f"PhenotypicFeature (from Condition) - Final - records with completed (Dict) - description: {description}")

		if not((len(ilist) - discarded) == (len([i for i in ilist if 'discarded' not in i]))):
			logging.info(f"Discrepancy in counts for PhenotypicFeature from Condition")

	elif(flag == 'observation'):
		logging.info(f"PhenotypicFeature (from Observation) - Original -  records fetched - Total: {len(ilist)}")
		logging.info(f"PhenotypicFeature (from Observation) - Discarded - Total, based on absence of - type: {discarded}")    
		logging.info(f"PhenotypicFeature (from Observation) - Discarded - Modifier, based on absence of - type: {discarded_modifier}")    
		logging.info(f"PhenotypicFeature (from Observation) - Discarded - Resolution, based on absence of - type: {discarded_resolution}")    
		logging.info(f"PhenotypicFeature (from Observation) - Discarded - Description, based on absence of - type: {discarded_description}")    
		logging.info(f"PhenotypicFeature (from Observation) - Final - records included - Total: {len([i for i in ilist if 'discarded' not in i])}")
		logging.info(f"PhenotypicFeature (from Observation) - Final - records with completed (Dict) - modifier: {modifier}")
		logging.info(f"PhenotypicFeature (from Observation) - Final - records with completed (Dict) - resolution: {resolution}")
		logging.info(f"PhenotypicFeature (from Observation) - Final - records with completed (Dict) - description: {description}")

		if not((len(ilist) - discarded) == (len([i for i in ilist if 'discarded' not in i]))):
			logging.info(f"Discrepancy in counts for PhenotypicFeature from Observation")
	return ilist_dict

def createListDictMeasurements(md):

	ilist_dict = {}
	ilist = []

	discarded = 0
	discarded_dueto_assay = 0 
	discarded_dueto_value = 0 # Discarded because of missing value 
	discarded_dueto_both = 0 

	discarded_value = 0 # Loss of a value
	discarded_assay = 0 # Loss of assay 

	for m in md:
		pid = m['person_id']

		if(pid not in ilist_dict):
			ilist_dict[pid] = []

		# discard if no assay information
		if(not 'assay_label' in m):
			ilist_dict[pid].append({'discarded':'yes'})
			ilist.append({'discarded':'yes'})
			discarded += 1

			# Get reason for discard 
			if(not any(k in m for k in ('value_as_number','value_label'))):
				discarded_dueto_both += 1
			else:
				discarded_dueto_assay += 1
				discarded_value += 1

			continue

		# discard if no value (either number or label)
		elif(not any(k in m for k in ('value_as_number','value_label'))):
			ilist_dict[pid].append({'discarded':'yes'})
			ilist.append({'discarded':'yes'})
			discarded += 1
			discarded_dueto_value += 1

			discarded_assay += 1
			
			continue
		
		tempdict={}
		#assay
		tempdict['assay']={'id':m['assay_id'],'label':m['assay_label']}

		if('measurement_datetime' in m):
			tempdict['time_observed']=convert_time(m['measurement_datetime'])

		if('value_as_number' in m):#Measurement with quantity
			tdict={}
			tdict['value']=m['value_as_number']
			#if('.' in m['value_label']):
			#	tdict['value']=float(m['value_label'])
			#else:
			#	tdict['value']=int(m['value_label'])
			if('unit_label' in m):
				tdict['unit']={'id':m['unit_id'],'label':m['unit_label']}
			tempdict['value']={'quantity':tdict}

			if(any(k in m for k in ('range_low','range_high'))):
				tdict={}
				if('unit_label' in m):
					tdict['unit']={'id':m['unit_id'],'label':m['unit_label']}
				if('range_low' in m):
					tdict['low']=m['range_low']
				if('range_high' in m):
					tdict['high']=m['range_high']
				tempdict['reference_range']=tdict				
		else:#Measurement with ontology
			tempdict['value']={'id':m['value_id'],'label':m['value_label']}

		ilist_dict[pid].append(tempdict)
		ilist.append(tempdict)

	logging.info(f"Measurement - Original -  records fetched - Total: {len(ilist)}")
	logging.info(f"Measurement - Discarded - Total: {discarded}")    
	logging.info(f"Measurement - Discarded - Total, based on absence of - both assay and value: {discarded_dueto_both}")    
	logging.info(f"Measurement - Discarded - Total, based on absence of - assay: {discarded_dueto_assay}")    
	logging.info(f"Measurement - Discarded - Total, based on absence of - value: {discarded_dueto_value}")    
	logging.info(f"Measurement - Discarded - Assay: {discarded_assay}")    
	logging.info(f"Measurement - Discarded - Value: {discarded_value}")    
	logging.info(f"Measurement - Final - records included - Total: {len([i for i in ilist if 'discarded' not in i])}")
   
	return ilist_dict

def createListDictTreatment(txdict):
    # Sort the initial list by 'person_id', 'agent_id', and 'agent_label'
    txdict_orig = txdict
    txdict_discard = [i for i in txdict if 'agent_id' not in i or 'agent_label' not in i]
    txdict = [i for i in txdict if 'agent_id' in i and 'agent_label' in i]
    txdict.sort(key=operator.itemgetter("person_id", "agent_id", "agent_label"))

    logging.info(f"Treatment - Original - Total: {len(txdict_orig)}")
    logging.info(f"Treatment - Discarded - Total: {len([i for i in txdict_orig if 'agent_id' not in i or 'agent_label' not in i])}")
    logging.info(f"Treatment - Final - Total: {len(txdict)}")


    # Fields lost from discarded entries (drug_type/interval_start/quantity are excluded because they are equivalent to total)
    logging.info(f"Treatment - Discarded - route_of_administration: {len([i for i in txdict_discard if 'route_of_administration_id' in i])}")
    logging.info(f"Treatment - Discarded - interval_end: {len([i for i in txdict_discard if 'interval_end' in i])}")
    logging.info(f"Treatment - Discarded - schedule_frequency (missing agent): {len([i for i in txdict_discard if 'sched_freq' in i])}")

    distinct_person_ids = set(item['person_id'] for item in txdict)

    ilist_dict = {}

    # Variables for logging 
    drug_type_present = 0
    route_of_administration_present = 0 
    schedule_freq_present = 0
    interval_end_present = 0 
    quantity_present = 0 
    schedule_freq_discard = 0 

    for person_id in distinct_person_ids:
        ilist = []
        idx = 0
        total = len(txdict)

        while idx < total:
            agent_id = txdict[idx]['agent_id']
            agent_label = txdict[idx]['agent_label']

            # Check if the current entry belongs to the current person_id
            if txdict[idx]['person_id'] != person_id:
                idx += 1
                continue

            tempdict = {
                'agent': {'id': agent_id, 'label': agent_label},
                'route_of_administration': None,  # Initialize as None, will set it if found
                'drug_type': None,  # Initialize as None, will set it if found
                'dose_intervals': []
            }

            # Retrieve entries for the current agent
            for k in range(idx, total):

                # Check if it's the current person 
                if txdict[k]['agent_id'] != agent_id:
                    break

                route_of_administration_present += ('route_of_administration_id' in txdict[k])
                drug_type_present += ('drug_type_id' in txdict[k])
                interval_end_present += ('interval_end' in txdict[k])
                schedule_freq_present += txdict[k]['sched_freq'] <= 4 if 'sched_freq' in txdict[k] else False
                schedule_freq_discard += txdict[k]['sched_freq'] > 4 if 'sched_freq' in txdict[k] else False

                if tempdict['route_of_administration'] is None and 'route_of_administration_id' in txdict[k]:
                    tempdict['route_of_administration'] = {
                        'id': txdict[k]['route_of_administration_id'],
                        'label': txdict[k]['route_of_administration_label']
                    }

                if tempdict['drug_type'] is None:
                    if 'drug_type_id' in txdict[k]:
                        drug_type_id = txdict[k]['drug_type_id']
                        if drug_type_id == 32879:
                            tempdict['drug_type'] = 'ADMINISTRATION_RELATED_TO_PROCEDURE'
                        elif drug_type_id == 32839:
                            tempdict['drug_type'] = 'PRESCRIPTION'
                        elif drug_type_id in (32833, 32825, 32821, 32818):
                            tempdict['drug_type'] = 'EHR_MEDICATION_LIST'
                        else:
                            tempdict['drug_type'] = 'UNKNOWN_DRUG_TYPE'
                    else:
                        tempdict['drug_type'] = 'UNKNOWN_DRUG_TYPE'

                dose = createDoseInterval(txdict[k])
                tempdict['dose_intervals'].append(dose)


                idx += 1

        
            if(tempdict['route_of_administration'] is None): del tempdict['route_of_administration']
            if(tempdict['drug_type'] is None): del tempdict['drug_type']


            ilist.append(tempdict)

        ilist_dict[person_id] = ilist  # Add processed data for the current person_id to the main list

    # Fields lost from discarded entries (drug_type/interval_start/quantity are excluded because they are equivalent to total)
    logging.info(f"Treatment - Final - route_of_administration: {route_of_administration_present}")
    logging.info(f"Treatment - Final - drug_type: {drug_type_present}")
    logging.info(f"Treatment - Final - interval_end: {interval_end_present}")
    logging.info(f"Treatment - Final - schedule_frequency: {schedule_freq_present}")
    logging.info(f"Treatment - Final - quantity: {quantity_present}")
    logging.info(f"Treatment - Discard - schedule_frequency (sched_freq > 4): {schedule_freq_discard}")

    return ilist_dict

def createListDictProcedures(md):
	ilist = []
	ilist_dict = {}
	discarded = 0 
	body_site = 0

	for m in md:

		pid = m['person_id']

		if(pid not in ilist_dict):
			ilist_dict[pid] = []

		if not('code_id' in m):
			ilist_dict[pid].append({'discarded':'yes'})
			ilist.append({'discarded':'yes'})
			discarded += 1

			continue

		tempdict = {}
		tempdict['code'] = {'id':m['code_id'],'label':m['code_label']}

		if ('body_site_id' in m):
			tempdict['body_site'] = {'id':m['body_site_id'],'label':m['body_site_label']}
			body_site += 1
		
		timestamp_temp = convert_time(m['performed_timestamp'])
		tempdict['performed'] = {'age':{'iso8601duration':m['performed_age']},'timestamp':timestamp_temp}

		ilist.append(tempdict)
		ilist_dict[pid].append(tempdict)

	logging.info(f"Procedure - Original - records fetched - Total: {len(ilist)}")
	logging.info(f"Procedure - Discarded - based on absence of - code: {discarded}")    
	logging.info(f"Procedure - Final - records included - Total: {len([i for i in ilist if 'discarded' not in i])}")
	logging.info(f"Procedure - Final - records with completed - body_site: {body_site}")

	if not((len(ilist) - discarded) == (len([i for i in ilist if 'discarded' not in i]))):
		logging.info(f"Discrepancy in counts for Procedure")


	return ilist_dict

# CREATE PHENOPACKET
def createPhenoIndividual(individualdict):
	if('date_of_birth' in individualdict):
		individualdict['date_of_birth']=Timestamp(seconds=convert_time_toseconds(individualdict['date_of_birth']))
	if('time_at_last_encounter' in individualdict):
		individualdict['time_at_last_encounter']=TimeElement(timestamp=Timestamp(seconds=convert_time_toseconds(individualdict['time_at_last_encounter'])))
	if('taxonomy' in individualdict):
		tx=OntologyClass(id=individualdict['taxonomy']['id'],label=individualdict['taxonomy']['label'])
		individualdict['taxonomy']=tx	
	if('vital_status' in individualdict):
		if('time_of_death' in individualdict['vital_status']):
			individualdict['vital_status']['time_of_death']=TimeElement(timestamp=Timestamp(seconds=convert_time_toseconds(individualdict['vital_status']['time_of_death'])))
		if('cause_of_death' in individualdict['vital_status']):
			cd=OntologyClass(id=individualdict['vital_status']['cause_of_death']['id'], label=individualdict['vital_status']['cause_of_death']['label'])
			individualdict['vital_status']['cause_of_death']=cd
		vs=VitalStatus(**individualdict['vital_status'])
		individualdict['vital_status']=vs
	
	return Individual(**individualdict)

def createPhenoConditions(ilist):
	diseases=[]
	for i in ilist:
		tempdict = {}
		if('discarded' in i):
			continue

		# term
		tempdict['term']=OntologyClass(id=i['term']['id'],label=i['term']['label'])

		# Onset 
		if('onset' in i):
			tempdict['onset'] = TimeElement(timestamp=Timestamp(seconds=convert_time_toseconds(i['onset']['timestamp'])))

		# Resolution
		if('resolution' in i):
			tempdict['resolution'] = TimeElement(timestamp=Timestamp(seconds=convert_time_toseconds(i['resolution']['timestamp'])))

		# Primary Site
		if('primary_site' in i):
			tempdict['primary_site']=OntologyClass(id=i['primary_site']['id'],label=i['primary_site']['label'])

		diseases.append(Disease(**tempdict))

	return diseases

def createPhenoFeature(ilist):
	features=[]
	for i in ilist:
		tempdict = {}
		
		if('discarded' in i):
			continue
		
		#type
		tempdict['type']=OntologyClass(id=i['type']['id'],label=i['type']['label'])

		# Modifiers
		modifiers = []
		if('modifiers' in i):
			modifiers.append(OntologyClass(id=i['modifiers']['id'],label=i['modifiers']['label']))
		tempdict['modifiers'] = modifiers
		
		# onset 
		if('onset' in i):
			tempdict['onset']=TimeElement(timestamp=Timestamp(seconds=convert_time_toseconds(i['onset']['timestamp'])))

		# resolution
		if('resolution' in i):
			tempdict['resolution']=TimeElement(timestamp=Timestamp(seconds=convert_time_toseconds(i['resolution']['timestamp'])))

		# description
		if('description' in i):
			tempdict['description'] = i['description']

		features.append(PhenotypicFeature(**tempdict))

	return features

def createPhenoMeasurement(ilist):
	measurements=[]
	for i in ilist:
		if('discarded' in i):
			continue
		#assay
		i['assay']=OntologyClass(id=i['assay']['id'],label=i['assay']['label'])
		if('id' in i['value']): #ontology
			i['value']=Value(ontology_class=OntologyClass(id=i['value']['id'],label=i['value']['label']))

		else: #value as number
			if ('unit' in i['value']['quantity']):
				i['value']=Value(quantity=Quantity(unit=OntologyClass(id=i['value']['quantity']['unit']['id'], 
							  label=i['value']['quantity']['unit']['label']),
							  value=i['value']['quantity']['value']))
			
			else:
				i['value']=Value(quantity=Quantity(value=i['value']['quantity']['value']))

		if('time_observed' in i):
			i['time_observed']=TimeElement(timestamp=Timestamp(seconds=convert_time_toseconds(i['time_observed'])))

		if('reference_range' in i): # Getting rid of referenceRange because it's not including in protobuf methods
			i.pop('reference_range')


		measurements.append(Measurement(**i))

	return measurements

def createPhenoTreatment(ilist):
	
	treatments=[]
	for i in ilist:
		tempdict = {}
		
		if('discarded' in i):
			continue
		
		# Agent
		tempdict['agent']=OntologyClass(id=i['agent']['id'],label=i['agent']['label'])
		
		# Route of administration
		if('route_of_administration' in i):
			tempdict['route_of_administration']=OntologyClass(id=i['route_of_administration']['id'],label=i['route_of_administration']['label'])
		
		# DoseIntervals
		tempdict['dose_intervals'] = i['dose_intervals']

		# Drug Type
		if('drug_type' in i):
			tempdict['drug_type']=i['drug_type']
	
		treatments.append(Treatment(**tempdict))

	return treatments

def createPhenoProcedure(ilist):
	procedures=[]
	for i in ilist:
		tempdict = {}
		
		if('discarded' in i):
			continue
		
		#code
		tempdict['code']=OntologyClass(id=i['code']['id'],label=i['code']['label'])

		# body_site
		if('body_site' in i):
			tempdict['body_site']=OntologyClass(id=i['body_site']['id'],label=i['body_site']['label'])
		
		# performed 
		if('performed' in i):
			tempdict['performed']=TimeElement(timestamp=Timestamp(seconds=convert_time_toseconds(i['performed']['timestamp'])))

		procedures.append(Procedure(**tempdict))

	return procedures

def createPhenoMedicalAction(txpheno = None, procpheno = None):
	medicalactpheno = []
	if(txpheno != None):
		for i in txpheno:
			tempdict = {}
			tempdict['treatment'] = i

			medicalactpheno.append(MedicalAction(**tempdict))
	
	if(procpheno != None):
		for i in procpheno:
			tempdict = {}
			tempdict['procedure'] = i

			medicalactpheno.append(MedicalAction(**tempdict))	
		
	return medicalactpheno

def createPheno(myid,meta_data,subject=None,phenotypic_features=None,measurements=None,biosamples=None,interpretations=None,diseases=None,medical_actions=None,files=None):
	pheno={}
	pheno['id']=myid
	if(subject != None):
		pheno['subject']=subject
	if(phenotypic_features!= None):
		pheno['phenotypic_features']=phenotypic_features
	if(measurements != None):
		pheno['measurements']=measurements
	if(biosamples != None):
		pheno['biosamples']=biosamples
	if(interpretations != None):
		pheno['interpretations']=interpretations
	if(diseases != None):
		pheno['diseases']=diseases
	if(medical_actions != None):
		pheno['medical_actions']=medical_actions
	if(files != None):
		pheno['files']=files
	pheno['meta_data']=meta_data

	return Phenopacket(**pheno)

# HELPER FUNCTIONS 
def convert_time(time_datetime):
	return datetime.strftime(time_datetime, '%Y-%m-%dT%H:%M:%S.%fZ')

def convert_time_toseconds(time_string):
	dt=datetime.strptime(time_string,'%Y-%m-%dT%H:%M:%S.%fZ')
	return int(datetime.timestamp(dt))

def createDoseInterval(entryDict):
    i = entryDict
    dose = {}

    if('sched_freq' in i.keys()):
        if(i['sched_freq'] == 1):
            dose['schedule_frequency'] = OntologyClass(id = 'ncit:C125004', label = 'Once Daily')
        elif(i['sched_freq'] == 2):
            dose['schedule_frequency'] = OntologyClass(id = 'ncit:C64496', label = 'Twice Daily')
        elif(i['sched_freq'] == 3):
           dose['schedule_frequency'] = OntologyClass(id = 'ncit:C64527', label = 'Three Times Daily')
        elif(i['sched_freq'] == 4):
            dose['schedule_frequency'] = OntologyClass(id = 'ncit:C64530', label = 'Four Times Daily')
    
    if('quantity_value' in i.keys()):
        unit = OntologyClass(id=i['quantity_id'],label=i['quantity_unit_label'])
        dose['quantity'] = {'unit':unit, 'value':i['quantity_value']}

    if('interval_start' in i.keys()):
        int_start = Timestamp(seconds=convert_time_toseconds(convert_time(i['interval_start'])))

        if('interval_end' in i.keys()):
            int_end = Timestamp(seconds=convert_time_toseconds(convert_time(i['interval_end'])))
            dose['interval'] = {'start':int_start,'end':int_end}
        else:
            dose['interval'] = {'start':int_start}

    return DoseInterval(**dose)

def combineDicts(phelist1, phelist2):	
    # Initialize a result dictionary
    full_phelist = {}

    # Iterate through dict1 and dict2 to combine lists for the same key
    for key in phelist1.keys() | phelist2.keys():
        full_phelist[key] = phelist1.get(key, []) + phelist2.get(key, [])
    
    return full_phelist

def get_sem_mapping(sem_mapping_file):
    """Input: a CSV file with the following columns: 
            - concept_id (e.g., 22274)
            - concept_name (e.g., Neoplasm of uncertain behavior of larynx)
            - PhenotypicFeature (e.g., Disease OR PhenotypicFeature)
        Output: 
            - phefeatures: A list of all concept_ids that should be mapped from OMOP's condition_occurrence to Phenopacket's PhenotypicFeature
    """

    sem_mapping = pd.read_csv(sem_mapping_file)
    phefeatures = list(sem_mapping.loc[sem_mapping.Phenopacket == 'PhenotypicFeature'].concept_id) # list of concepts that should go to phenotypic features
	
    phefeatures.remove(0)
	
    return phefeatures

def createMetadata(myname):
	metadata={}
	metadata['created']=Timestamp(seconds=int(time.time()))
	metadata['created_by']=myname
	mdr=[]

	# https://registry.identifiers.org/registry/snomedct
	md={}
	md['id']='snomedct'
	md['name']=	'Systematized Nomenclature of Medicine - Clinical Terms(SNOMED-CT)'
	md['url']='http://www.snomedbrowser.com/'
	md['version']='SNOMEDCT_2023_03_01'
	md['namespace_prefix']='snomedct'
	md['iri_prefix']='snomedct'
	mdr.append(md)

	# https://bioregistry.io/registry/rxnorm
	md={}
	md['id']='rxnorm'
	md['name']=	'RxNorm'
	md['url']='https://mor.nlm.nih.gov/RxNav/search?searchBy=RXCUI&searchTerm=221058'
	md['version']='2023-01-01'
	md['namespace_prefix']='rxnorm'
	md['iri_prefix']='rxnorm'
	mdr.append(md)

	# https://bioregistry.io/registry/loinc
	md={}
	md['id']='loinc'
	md['name']=	'LOINC'
	md['url']='https://loinc.org/rdf/'
	md['version']='2022-04-01'
	md['namespace_prefix']='loinc'
	md['iri_prefix']='loinc'
	mdr.append(md)

	# https://obofoundry.org/ontology/ncit.html
	md={}
	md['id']='ncit'
	md['name']=	'NCIT'
	md['url']='http://purl.obolibrary.org/obo/ncit.owl'
	md['version']='2023-10-30'
	md['namespace_prefix']='ncit'
	md['iri_prefix']='ncit'
	mdr.append(md)

	metadata['resources']= mdr     
	metadata['phenopacket_schema_version']='2.0'
	logging.debug(f'metadata: {metadata}')
	return metadata
