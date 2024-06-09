#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Transaction
# Imports et clés

import pandas as pd
import requests
import configparser
from requests_oauthlib import OAuth1
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

config = configparser.ConfigParser()
config.read('config.ini')

# API Bitrix24
BITRIX_DOMAIN = config['BITRIX'].get('DOMAIN')
BITRIX_TOKEN = config['BITRIX'].get('TOKEN') 


# In[2]:


#Transaction
# Imports et clés

import pandas as pd
import configparser
import requests

config = configparser.ConfigParser()
config.read('config.ini')

# API Bitrix24
BITRIX_DOMAIN = config['BITRIX'].get('DOMAIN')
BITRIX_TOKEN = config['BITRIX'].get('TOKEN') 
api_endpoint = f"https://{BITRIX_DOMAIN}/rest/1/"


data_list = []


listUrls = []
listIds = []
listNomComplets = []
listNumeros = []
listEmails = []
listUtmSource = []
listobjectif = []
listCompagnePublicite = []
listBudjetJournalier = []
listCiblage = []
listMessagePublicitaire = []
listactioninitial = []
listDatedecreation = []
listNomDeLaCampageUtm = []
listinscription = []
listNAME = []
listLastName = []
listUrl = []
listidd = []
liststatut = []
liststatus = []
listEtapes = []
listID = []
listnote1 = []
listnote2 = []
listnote3 = []
listnote4 = []


def post_bitrix24_data(url, params):
    try:
        response = requests.post(url, json=params)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        raise ValueError(f'Erreur lors de l\'envoi de données à Bitrix24 : {e}')

# Fonction pour récupérer la liste des leads depuis Bitrix24
def CrmBitrix(content='', params=[], result=''):
    if not BITRIX_DOMAIN or not BITRIX_TOKEN:
        raise ValueError('Domaine et/ou token Bitrix manquants dans le fichier \'config.ini\'')

    params_str = '&'.join(params)
    BitrixUrl = f'http://{BITRIX_DOMAIN}/rest/1/{BITRIX_TOKEN}/{content}?{params_str}&start='

    url = BitrixUrl + "0"
    while True:
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise ValueError(f'Erreur lors de la récupération depuis Bitrix : {e}')

        data = response.json()
        if 'result' in data:
            prospects = data['result']
            for prospect in prospects:
                if result == 'email':
                    if 'EMAIL' in prospect:
                        email = prospect['EMAIL'][0]['VALUE']
                        listEmails.append(email)
                        mesNumeros = prospect['PHONE'][0]['VALUE']
                        listNumeros.append(mesNumeros)
                elif result == 'title':
                    maListNomComplet = prospect['TITLE']
                    listNomComplets.append(maListNomComplet)
                    if 'EMAIL' in prospect:
                        email = prospect['EMAIL'][0]['VALUE']
                        listEmails.append(email)
                    else:
                        listEmails.append('')
                    if 'PHONE' in prospect:
                        mesNumeros = prospect['PHONE'][0]['VALUE']
                        listNumeros.append(mesNumeros)
                    else:
                        listNumeros.append('')
                    maListdate = prospect['UF_CRM_1612958598']
                    listDatedecreation.append(maListdate)
                    maListstatus = prospect['STATUS_ID']
                    if maListstatus in status_id_to_etape:
                        etape_de_lead = status_id_to_etape[maListstatus]
                    else:
                        etape_de_lead = maListstatus
                    listEtapes.append(etape_de_lead)
                    maListurl = prospect['UF_CRM_1597812509613']
                    listUrl.append(maListurl)
                    maListname = prospect['NAME']
                    listNAME.append(maListname)
                    maListid = prospect['ID']
                    listID.append(maListid)
                    maListlastname = prospect['LAST_NAME']
                    listLastName.append(maListlastname)
                    maListCompagnePublicite = prospect['UF_CRM_1616091858429']
                    listCompagnePublicite.append(maListCompagnePublicite)
                    maListMessagePublicitaire = prospect['UF_CRM_1617807659790']
                    listMessagePublicitaire.append(maListMessagePublicitaire)
                    maListBudjetJournalier = prospect['UF_CRM_1616089911935']
                    listBudjetJournalier.append(maListBudjetJournalier)
                    maListinscription = prospect['UF_CRM_1612883730']
                    listinscription.append(maListinscription)
                    maListobjectif = prospect['UF_CRM_1597923071712']
                    listobjectif.append(maListobjectif)
                    maListactioninitial = prospect['UF_CRM_1616091444727']
                    listactioninitial.append(maListactioninitial)
                    maListCiblage = prospect['UF_CRM_1617807622632']
                    listCiblage.append(maListCiblage)
                    maListidd = prospect['UF_CRM_1597812738800']
                    listidd.append(maListidd)
                    maListnote1 = prospect['UF_CRM_1597923032544']
                    listnote1.append(maListnote1)
                    maListnote2 = prospect['UF_CRM_1597923044112']
                    listnote2.append(maListnote2)
                else:
                    return df_btx, listID 

        if 'next' in data:
            url = BitrixUrl + str(data['next'])
        else:
            break

    if result == 'email':
        df_btx = pd.DataFrame({'email': listEmails})
    elif result == 'title':
        df_btx = pd.DataFrame({
            'ID': listID,
            'Prénom': listNAME,
            'Nom': listLastName,
            "Email": listEmails,
            "Mobile bitrix": listNumeros,
            'Nom prospect': listNomComplets,
            "Url de la source": listUrl,
            "Inscription Utm Source": listinscription,
            "Action Initiale du prospect": listactioninitial,
            "Objectif_formulé": listobjectif,
            "Date de creation": listDatedecreation,
            "Budjet journalier": listBudjetJournalier,
            "Nom de la campagne de publicité utm campaign": listCompagnePublicite,
            "Ciblage": listCiblage,
            "Message publicitaire": listMessagePublicitaire,
            "Étapes": listEtapes,
            "Note 1": listnote1,
            "Note 2": listnote2,
            "IDS": listidd,
        })
    else:
        return 'Erreur avec les données attendues. Merci de vérifier la variable \'result\'.'

    return df_btx

status_id_to_etape = {
    "IN_PROCESS": "Non Contacté", # pour convertir les in_process en non contacté parce que c'est le statut par défaut des leads créés dans Bitrix
    "19": "Trop cher pour lui",
    "3": "Appel effectué mais réfléchis",
    "UC_OVJ9DJ": "SEG site finalisée non payée",
    "17": "SEG Prévu sur Acuity",
    "NEW": "A contacter dans le futur",
    "UC_YNXJB3": "A appelé pour un proche",
    "6": "NRP Appel + SMS personnalisé",
    "8": "Faux Numéro",
    "JUNK": "Mauvais prospect",
    "5": "NRP Appel + Message Vocal",
    "18": "Mail Personnalisé Envoyé",
    "CONVERTED":"A été converti(e)",
    "21": "SMS Personnalisé Envoyé",
    "22": "A répondu au message personnalisé suite NRP",
    "14": "SEG Annulé / Non honoré",
    "4":"SEG Prevu sur plateforme (calendly, doctolib, psyhcologue.net, resalib, starofservice)",
    "7":"NRP Appel + Mail personnalisé",
    "23":"A fait une demande de rappel",
    "20":"Appel Sortant Effectué Doit Rappeler"
    
}

# Appel de la fonction CrmBitrix pour récupérer la liste des leads
dfb = CrmBitrix(
    content='crm.lead.list.json',
    params=[
        'select[]=TITLE',
        'select[]=ID',
        'select[]=NAME',
        'select[]=LAST_NAME',
        'select[]=EMAIL',
        'select[]=PHONE',
        'select[]=UF_CRM_1612958598',
        'select[]=STATUS_ID',
        'select[]=UF_CRM_1597812509613',
        'select[]=UF_CRM_1616091858429',
        'select[]=UF_CRM_1617807659790',
        'select[]=UF_CRM_1612883730',
        'select[]=UF_CRM_1616089911935',
        'select[]=UF_CRM_1597923071712',
        'select[]=UF_CRM_1616091444727',
        'select[]=UF_CRM_1617807622632',
        'select[]=UF_CRM_1597812738800',
        'select[]=UF_CRM_1597923032544',
        'select[]=UF_CRM_1597923044112',
    ],
    result="title"
)

dfb


# In[7]:


df_Bitrix_Part2 = dfb.copy()
df_Bitrix_Part2.to_csv("liste des prospect.csv", index=False)
df_Bitrix_Part2


# In[69]:


import pandas as pd

df_Bitrix_Part2 = pd.read_csv("liste des prospect.csv", encoding='utf-8')

df_Bitrix_Part2


# In[70]:


import pandas as pd

df_bitrix = pd.read_csv("listes des commentaires.csv", encoding='utf-8')

df_bitrix


# In[60]:


import pandas as pd

# Assuming df_bitrix is your DataFrame
# Replace NaN with an empty string and convert all items to string
df_bitrix['Commentaire'] = df_bitrix['Commentaire'].fillna('').astype(str)

# Now apply the join operation
grouped_comments = df_bitrix.groupby('ID')['Commentaire'].apply(', '.join).reset_index()

grouped_comments


# In[ ]:





# In[32]:


import pandas as pd
import requests
import time

url = "https://institutadios.bitrix24.fr/rest/1/********/"

data_list = []


def post_bitrix24_data_paginated(url, data):
    all_data = []
    start = 0 
    attempt = 0  
    max_attempts = 5 
    while True: #
        print(f"Demande de données à partir de {start}...")
        data['start'] = start
        try: 
            response = requests.post(url, json=data) 
            response.raise_for_status()
            response_data = response.json() 
            if response_data and "result" in response_data: sponse_data 
                all_data.extend(response_data["result"]) 
                print(f"Récupéré {len(response_data['result'])} éléments") #
                if "next" in response_data: 
                    start = response_data["next"] 
                else:
                    break #n
            else:
                print("Aucun résultat supplémentaire.")
                break
        except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
            print(f"Erreur de requête : {e}")
            attempt += 1# 
            if attempt >= max_attempts: # 
                print("Nombre maximal de tentatives atteint. Abandon.")
                break
            print(f"Tentative de reconnexion dans 60 secondes. Tentative {attempt} de {max_attempts}.")
            time.sleep(60)
            continue
        time.sleep(60)# 
    return all_data

def get_bitrix24_comments(entity_id, entity_type):
    comments = []
    start = 0
    while True:
        print(f"Récupération des commentaires pour l'entité {entity_id} à partir de {start}...")
        comments_url = f"{api_endpoint}crm.timeline.comment.list"
        data = {
            "filter": {
                "ENTITY_ID": entity_id,
                "ENTITY_TYPE": entity_type,
            },
            "select": ["ID", "COMMENT", "FILES"],
            "start": start
        }
        response = requests.post(comments_url, json=data)
        try:
            response.raise_for_status()
            comments_data = response.json()
            if comments_data and "result" in comments_data:
                comments.extend(comments_data["result"])
                print(f"Récupéré {len(comments_data['result'])} commentaires")
                if "next" in comments_data:
                    start = comments_data["next"]
                else:
                    break
            else:
                print("Aucun commentaire supplémentaire.")
                break
        except requests.exceptions.RequestException as e:
            print(f"Erreur de requête : {e}")
            break
        time.sleep(60)
    return comments

print("Récupération des IDs de lead...")
leads_ids_data = post_bitrix24_data_paginated(f"{api_endpoint}crm.lead.list",
                                              {"select": ["ID"]})

for lead_id in leads_ids_data:
    lead_id = lead_id["ID"]
    print(f"Traitement du lead ID: {lead_id}")
    lead_details_data = post_bitrix24_data_paginated(f"{api_endpoint}crm.lead.get",
                                                     {"id": int(lead_id), "select": ["*"]})
    if lead_details_data:
        lead_details = lead_details_data[0]
        lead_data = {"ID": lead_id}
        data_list.append(lead_data)

        print(f"Récupération des activités pour le lead ID: {lead_id}")
        activities_data = post_bitrix24_data_paginated(f"{api_endpoint}crm.activity.list", {
            "order": {"ID": "DESC"},
            "filter": {"OWNER_ID": int(lead_id)},
            "select": ["*", "COMMUNICATIONS", "RESPONSIBLE_ID", "OWNER_ID",
                       "BINDINGS", "DESCRIPTION_TYPE"]
        })

        for activity in activities_data:
            activity_data = {
                "ID": lead_id,
                
                "Commentaire": activity.get('SUBJECT', 'N/A'),
                "Date de début": activity['START_TIME'],
                "Date de fin": activity['END_TIME'],
            }
            communications = activity.get('COMMUNICATIONS', [])
            for communication in communications:
                communication_data = {"Type de communication": communication['TYPE']}
                activity_data.update(communication_data)
            data_list.append(activity_data)

        print(f"Récupération des commentaires pour le lead ID: {lead_id}")
        comments = get_bitrix24_comments(lead_id, "lead")
        for comment in comments:
            comment_data = {
                "ID": lead_id,
                
                "Commentaire": comment.get('COMMENT', 'N/A'),
            }
            data_list.append(comment_data)

df = pd.DataFrame(data_list)
print(df)


# In[34]:


df.to_csv("commenteurs.csv", index=False)
df


# In[6]:


import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time

def get_list_leads(base_url, start=0):
    list_leads_id = []
    while start is not None:
        response = requests.get(f"{base_url}crm.lead.list?start={start}")
        if response.ok:
            leads = response.json()
            for lead in leads['result']:
                list_leads_id.append(int(lead.get('ID')))
            start = leads.get('next')
            time.sleep(3)
        else:
            print(f"Erreur code : {response.status_code}\nMessage : {response.text}")
            break
    print(f"Total leads : {str(len(list_leads_id))}")
    return list_leads_id

def get_lead(base_url, lead_id):
    lead = {}
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    try:
        response = session.get(f"{base_url}crm.lead.get.json?ID={lead_id}")
        response.raise_for_status()
        data = response.json()['result']
        lead.update({'ID': data.get('ID'), 'Title': data.get('TITLE',''), 'Name': data.get('NAME',''), 'Lastname': data.get('LAST_NAME','')})
    except requests.exceptions.RequestException as err:
        print(f"Function : get_lead\nLead id : {lead_id}\nErreur : {err}")
        lead = {'ID': lead_id, 'Title': '', 'Name': '', 'Lastname': ''}

    return lead

def get_activities(base_url, start=0):
    list_activities_id = []
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    while start is not None:
        try:
            response = session.get(f"{base_url}crm.activity.list?start={start}")
            response.raise_for_status()
            activities = response.json()
            for activity in activities['result']:
                list_activities_id.append(int(activity.get('ID')))
            start = activities.get('next')
            time.sleep(3)
        except requests.exceptions.RequestException as err:
            print(f"Function : get_activities\nStart : {start}\nErreur : {err}")
            break
    print(f"Total activities : {str(len(list_activities_id))}")
    return list_activities_id

def get_activity(base_url, activity_id):
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    try:
        response = session.get(f"{base_url}crm.activity.get.json?ID={activity_id}")
        response.raise_for_status()
        data = response.json()['result']
        activity = {'Id activity': data.get('ID'), 'Owner Id': data.get('OWNER_ID'), 'Subject': data.get('SUBJECT'), 'Created': data.get('CREATED'), 'Last Update': data.get('LAST_UPDATED')}
        lead = get_lead(base_url, data.get('OWNER_ID'))
        activity_lead = {**activity, **lead}
        print(f"Activité récupérée : ID {activity_id}")
        return activity_lead
    except requests.exceptions.RequestException as err:
        print(f"Function : get_activity\nActivity id : {activity_id}\nErreur : {err}")
        return {'Id activity': activity_id, 'Owner Id': '', 'Subject': '', 'Created': '', 'Last Update': '', 'ID': '', 'Title': '', 'Name': '', 'Lastname': ''}

base_url = "https://institutadios.bitrix24.fr/rest/1/tfs4ck7sh7t99wk2/"
activities = get_activities(base_url)

activities_data = []
for activity_id in activities:
    activity = get_activity(base_url, activity_id)
    activities_data.append(activity)

dataframe = pd.DataFrame(activities_data)
print(dataframe)


# In[7]:


p=dataframe
p


# In[9]:


p.to_csv("commentor.csv", index=False)
p


# In[10]:


del p["Id activity"]
del p["Owner Id"]
del p["Title"]
del p["Name"]
del p["Lastname"]
p


# In[79]:


liste_de_mes_prospects= dfb
liste_de_mes_commentaires = df
merge_prospects_et_commentaire = liste_de_mes_prospects.merge(liste_de_mes_commentaires,how='left') 
enleve_les_doublons_de_merge = merge_prospects_et_commentaire.drop_duplicates()
merge_prospects_et_commentaire.to_csv("merge_liste_des_prospects_et_leur_commentaires.csv",
                                      index=False)
print("c'est bon")
merge_prospects_et_commentaire


# In[3]:


import requests
import json

# Paramètres
API_KEY_FRESHWORK = '1oRGrwUiQnGdbaYDwVIo5A'
DOMAIN_FRESHWORK = 'institutadios'
BASE_URL = f'https://{DOMAIN_FRESHWORK}.myfreshworks.com/crm/sales/'

# Données du contact
contact_data = {
    "first_name": "John",
    "last_name": "Doe",
    "email": "johndoe@example.com",
    "custom_field": {
        "cf_prospect": "Nouveau prospect",
        "cf_id_bitrix": "12345"  # Remplacez par l'ID Bitrix approprié
    }
}

# Création du contact
headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Token token={API_KEY_FRESHWORK}'
}

contact_response = requests.post(f"{BASE_URL}api/v2/contacts", headers=headers, json=contact_data)

if contact_response.status_code == 200:
    print("Contact créé avec succès.")
else:
    print(f"Erreur lors de la création du contact : {contact_response.status_code}")
    exit()

# Données de la tâche
task_data = {
    "task": {
        "title": "Tâche de suivi",
        "description": "Suivre le nouveau prospect",
        "due_date": "2023-01-01T23:00:00+00:00",  # Date de l'échéance
        "owner_id": 31000890026,  # ID de l'utilisateur responsable
        "targetable_type": "Contact",
        "task_users_attributes": [{
            "user_id": 31000890026  # ID de l'utilisateur associé à la tâche
        }],
        "targetable_id": contact_response.json()['contact']['id']  # ID du contact créé
    }
}

# Création de la tâche associée au contact
task_response = requests.post(f"{BASE_URL}api/tasks", headers=headers, json=task_data)

if task_response.status_code == 200:
    print("Tâche créée avec succès.")
else:
    print(f"Erreur lors de la création de la tâche : {task_response.status_code}")


# In[1]:


import csv
import requests
import json
import time

# Paramètres
API_KEY_FRESHWORK = '1oRGrwUiQnGdbaYDwVIo5A'
DOMAIN_FRESHWORK = 'institutadios'
BASE_URL = f'https://{DOMAIN_FRESHWORK}.myfreshworks.com/crm/sales/'
BATCH_SIZE = 700  # Taille du lot
SLEEP_TIME = 3600  # Temps d'attente entre les lots (en secondes)


def find_contact_by_bitrix_id(existing_contacts, bitrix_id):
    for contact in existing_contacts:
        if contact['custom_field']['cf_id_bitrix'] == bitrix_id:
            return contact
    return None
'''def get_existing_contacts(api_key, domain):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token token={api_key}'
    }
    
    endpoint = "contacts/view/31004512042"
    url = f"{BASE_URL}{endpoint}"
    
    try:
        response = requests.get(url, headers=headers)
        print(f"Status code de récupération des contacts existants : {response.status_code}")
        if response.ok:
            existing_contacts = response.json()['contacts']
            print(f"Contacts récupérés avec succès : {len(existing_contacts)}")
            return existing_contacts
        else:
            print(f"Erreur lors de la récupération des contacts : {response.status_code}")
            return []
    except Exception as e:
        print(f"Erreur Serveur : {str(e)}")'''
        
def get_existing_contacts(api_key, domain):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token token={api_key}'
    }
    endpoint = "contacts/view/31004512042"
    url = f"{BASE_URL}{endpoint}"
    all_contacts = {}
    page = 1

    while True:
        params = {'per_page': 100, 'page': page}
        response = requests.get(url, headers=headers, params=params)
        if not response.ok:
            print(f"Error retrieving contacts: {response.status_code}")
            break
        contacts_data = response.json()['contacts']
        for contact in contacts_data:
            bitrix_id = contact['custom_field']['cf_id_bitrix']
            all_contacts[bitrix_id] = contact
        if len(contacts_data) < params['per_page']:
            break
        page += 1

    print(f"Contacts récupérés avec succès : {len(all_contacts)}")
    return all_contacts

def create_contact(api_key, domain, contact_data):
    url = f"{BASE_URL}api/contacts"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token token={api_key}'
    }
    response = requests.post(url, headers=headers, json={'contact': contact_data})
    if response.status_code == 201:
        print(f"Contact créé avec succès : {contact_data['custom_field']['cf_id_bitrix']}")
    else:
        print(f"Erreur lors de la création du contact {contact_data['custom_field']['cf_id_bitrix']}: {response.status_code}")
    time.sleep(5)  # Pause d'une minute
    return response

def update_contact(api_key, domain, contact_data, contact_id):
    url = f"{BASE_URL}api/contacts/{contact_id}"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token token={api_key}'
    }
    response = requests.put(url, headers=headers, json={'contact': contact_data})
    if response.status_code == 200:
        print(f"Contact mis à jour avec succès : {contact_data['custom_field']['cf_id_bitrix']}")
    else:
        print(f"Erreur lors de la mise à jour du contact {contact_data['custom_field']['cf_id_bitrix']}: {response.status_code}")
    time.sleep(5)  # Pause d'une minute
    return response

def create_task(api_key, domain, task_data):
    url = f"{BASE_URL}api/tasks"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token token={api_key}'
    }
    response = requests.post(url, headers=headers, json=task_data)
    if response.status_code == 201:
        print(f"Tâche créée avec succès pour le contact ID : {task_data['task']['targetable_id']}")
    else:
        print(f"Erreur lors de la création de la tâche pour le contact ID {task_data['task']['targetable_id']}: {response.status_code}")
    time.sleep(5)  # Pause d'une minute
    return response



def extract_contact_info(row):
    # Extrait et renvoie les informations du contact de la ligne
    return {
        "first_name": str(row["Prénom"]),
        "last_name": str(row["Nom"]),
        "email": str(row["Email"]),
        "work_number": str(row["Mobile bitrix"]),
        "custom_field": {
            "cf_prospect": str(row["Nom prospect"]),
            "cf_url_de_la_source": str(row["Url de la source"]),
            "cf_inscription_utm_source": str(row["Inscription Utm Source"]),
            "cf_action_initiale_du_prospect": str(row["Action Initiale du prospect"]),
            "cf_objectif_formul": str(row["Objectif_formulé"]),
            "cf_date_de_creation": str(row["Date de creation"]),
            "cf_budget_journalier_utm_medium": str(row["Budjet journalier"]),
            "cf_nom_de_la_campagne_de_publicit_utm_campaign": str(row["Nom de la campagne de publicité utm campaign"]),
            "cf_ciblage_utm_term": str(row["Ciblage"]),
            "cf_message_publicitaire_utm_content": str(row["Message publicitaire"]),
            "cf_statutss": str(row["Étapes"]),
            "cf_note1": str(row["Note 1"]),
            "cf_note2": str(row["Note 2"]),
            "cf_id_bitrix": str(row["ID"])  # Supposons que ID est le cf_id_bitrix
        }
    }

def extract_task_info(row):
    # Extrait et renvoie les informations de la tâche de la ligne
    return {
        "task": {
            "status_name": str(row["Étapes"]),
            "title": str(row["Nom prospect"]) if row["Nom prospect"] else "Champs vide",
            "description": str(row["Commentaire"]),
            "due_date": str(row["Date de début"]) if row["Date de début"] else "2023-01-01T23:00:00+00:00",
            "owner_id": 31000890026,
            "targetable_type": "Contact",
            "task_users_attributes": [{
                "user_id": 31000890026
            }]
        }
    }
def group_tasks_by_contact(bitrix_contacts):
    grouped_data = {}
    for row in bitrix_contacts:
        contact_id = row['ID']  # ou une autre clé unique pour chaque contact
        if contact_id not in grouped_data:
            grouped_data[contact_id] = {
                'contact_info': extract_contact_info(row),
                'tasks': []
            }
        grouped_data[contact_id]['tasks'].append(extract_task_info(row))

    return grouped_data

def create_or_update_task(api_key, domain, task_data, contact_id):
    # Créer une nouvelle tâche pour chaque entrée de tâche
    task_data["task"]["targetable_id"] = contact_id
    create_task(api_key, domain, task_data)
    
def process_contact_tasks(api_key, domain, existing_contacts, contact_data, tasks_data):
    bitrix_id = contact_data['custom_field']['cf_id_bitrix']
    existing_contact = existing_contacts.get(bitrix_id)
    contact_id = None

    if existing_contact:
        contact_id = existing_contact['id']
        update_contact(api_key, domain, contact_data, contact_id)
    else:
        contact_response = create_contact(api_key, domain, contact_data)
        if contact_response.ok:
            contact_id = contact_response.json().get('contact', {}).get('id')

    if contact_id:
        for task_data in tasks_data:
            task_data["task"]["targetable_id"] = contact_id
            create_task(api_key, domain, task_data)
def main():
    try:
        with open('betravvv.csv', 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            bitrix_contacts = list(reader)
    except Exception as e:
        print("Fichier non trouvé\nErreur lors de la récupération des contacts Bitrix")
        return

    if not bitrix_contacts:
        print("Aucun contact trouvé dans le fichier CSV")
        return

    existing_contacts = get_existing_contacts(API_KEY_FRESHWORK, DOMAIN_FRESHWORK)
    contacts_with_tasks = group_tasks_by_contact(bitrix_contacts)

    total_contacts = len(contacts_with_tasks)
    batches = [dict(list(contacts_with_tasks.items())[i:i + BATCH_SIZE]) for i in range(0, total_contacts, BATCH_SIZE)]
    starting_batch = 42  

    for batch_num, batch in enumerate(batches, start=1):
        if batch_num < starting_batch:
            continue  

    
        print(f"Traitement du lot {batch_num} de {len(batches)}")
        for contact_id, contact_info in batch.items():
            process_contact_tasks(API_KEY_FRESHWORK, DOMAIN_FRESHWORK, existing_contacts, contact_info['contact_info'], contact_info['tasks'])

        if batch_num < len(batches):
            print(f"Pause de {SLEEP_TIME} secondes avant le prochain lot...")
            time.sleep(3600)

    print("Traitement terminé.")

if __name__ == "__main__":
    main()


# In[ ]:





# In[6]:


import csv
import requests
import json
import time

BATCH_SIZE = 700  # Taille du lot
SLEEP_TIME = 3600  # Temps d'attente entre les lots (en secondes)
API_KEY_FRESHWORK = 'clé api'
DOMAIN_FRESHWORK = 'institutadios'
BASE_URL = f'https://{DOMAIN_FRESHWORK}.myfreshworks.com/crm/sales/'

def get_existing_contacts(api_key, domain):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token token={api_key}'
    }
    endpoint = "contacts/view/31004512042?include=contact_status"
    url = f"{BASE_URL}{endpoint}"
    all_contacts = {}
    page = 1

    while True:
        params = {'per_page': 100, 'page': page}
        response = requests.get(url, headers=headers, params=params)
        if not response.ok:
            print(f"Error retrieving contacts: {response.status_code}")
            break
        contacts_data = response.json()['contacts']
        for contact in contacts_data:
            bitrix_id = contact['custom_field']['cf_id_bitrix']
            all_contacts[bitrix_id] = contact
        if len(contacts_data) < params['per_page']:
            break
        page += 1

    print(f"Contacts récupérés avec succès : {len(all_contacts)}")
    return all_contacts

def create_contact(api_key, domain, contact_data):
    url = f"{BASE_URL}api/contacts"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token token={api_key}'
    }
    response = requests.post(url, headers=headers, json={'contact': contact_data})
    if response.status_code == 201:
        print(f"Contact créé avec succès : {contact_data['custom_field']['cf_id_bitrix']}")
    else:
        print(f"Erreur lors de la création du contact {contact_data['custom_field']['cf_id_bitrix']}: {response.status_code}")
    time.sleep(1)  # Pause
    return response

def update_contact(api_key, domain, contact_data, contact_id):
    url = f"{BASE_URL}api/contacts/{contact_id}"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token token={api_key}'
    }
    response = requests.put(url, headers=headers, json={'contact': contact_data})
    if response.status_code == 200:
        cf_prospect = contact_data['custom_field'].get('cf_prospect')  #
        #status_name = contact_data['custom_field'].get('cf_statutss')
        status_name = contact_data['custom_field'].get('cf_statutss') if contact_data['custom_field'].get('cf_statutss') else 'Non Contacté'
       
        print(f"Contact mis à jour avec succès : {contact_data['custom_field']['cf_id_bitrix']}. Prospect: {cf_prospect}, Statut: {status_name}")
        
    else:
        print(f"Erreur lors de la mise à jour du contact {contact_data['custom_field']['cf_id_bitrix']}: {response.status_code}, Statut: {status_name}")
    time.sleep(1)  # Pause
    return response

def extract_contact_info(row, status_name_to_id, default_status_id):
    status_name = str(row["Étapes"])
    status_id = status_name_to_id.get(status_name, default_status_id)
    return {
        "first_name": str(row["Prénom"]),
        "last_name": str(row["Nom"]), 
        "email": str(row["Email"]),
        "work_number": str(row["Mobile bitrix"]),
        "custom_field": {
            "cf_prospect": str(row["Nom prospect"]),
            "cf_url_de_la_source": str(row["Url de la source"]),
            "cf_inscription_utm_source": str(row["Inscription Utm Source"]),
            "cf_action_initiale_du_prospect": str(row["Action Initiale du prospect"]),
            "cf_objectif_formul": str(row["Objectif_formulé"]),
            "cf_date_de_creation": str(row["Date de creation"]),
            "cf_budget_journalier_utm_medium": str(row["Budjet journalier"]),
            "cf_nom_de_la_campagne_de_publicit_utm_campaign": str(row["Nom de la campagne de publicité utm campaign"]),
            "cf_ciblage_utm_term": str(row["Ciblage"]),
            "cf_message_publicitaire_utm_content": str(row["Message publicitaire"]),
            "cf_statutss": str(row["Étapes"]),
            "cf_note1": str(row["Note 1"]),
            "cf_note2": str(row["Note 2"]),
            "cf_id_bitrix": str(row["ID"]),
            "status_name": status_name 
        },
        "contact_status_id": status_id  
    }

def main():
    try:
        with open('betravvv.csv', 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            bitrix_contacts = list(reader)
    except Exception as e:
        print("Fichier non trouvé\nErreur lors de la récupération des contacts Bitrix")
        return

    if not bitrix_contacts:
        print("Aucun contact trouvé dans le fichier CSV")
        return

    
    status_name_to_id = {
    "Non Contacté": 31001060874,
    "Faux Numéro": 31001060875,
    "Mail Personnalisé Envoyé": 31001060876,
    "SMS Personnalisé Envoyé": 31001060877,
    "NRP Appel + SMS personnalisé": 31001060878,
    "NRP Appel + Message Vocal": 31001060879,
    "NRP Appel + Mail personnalisé": 31001060880,
    "A répondu au message personnalisé suite NRP": 31001060881,
    "A fait une demande de rappel": 31001060882,
    "A appelé pour un proche": 31001060883,
    "Trop cher pour lui": 31001060884,
    "Appel Sortant Effectué Doit Rappeler": 31001060885,
    "Appel effectué mais réfléchis": 31001060886,
    "A contacter dans le futur": 31001060887,
    "Achat Livre": 31001060888,
    'A été converti(e)': 31001100607,
    'Stage de sensibilisation':1001100608,
    "SEG site finalisée non payée": 31001060889,
    "SEG Prevu sur plateforme (calendly, doctolib, psyhcologue.net, resalib, starofservice)": 31001060890,
    "SEG Prévu sur Acuity": 31001060891,
    "Mauvais prospect": 31001060892,
    "SEG Annulé / Non honoré": 31001060893,
    "Non contacté": 31001060872,  
    "Contact perdu": 31001060873,
    "Qualified": 31001060868,
    "Won": 31000694287,
    "Churned": 31000694288,
}

    default_status_id = 31001060874 

    existing_contacts = get_existing_contacts(API_KEY_FRESHWORK, DOMAIN_FRESHWORK)

    total_contacts = len(bitrix_contacts)
    batches = [bitrix_contacts[i:i + BATCH_SIZE] for i in range(0, total_contacts, BATCH_SIZE)]

    starting_batch = 47

    for batch_index, batch in enumerate(batches, start=1):
        if batch_index < starting_batch:
            continue 

        for contact in batch:
            contact_info = extract_contact_info(contact, status_name_to_id, default_status_id)
            bitrix_id = contact_info['custom_field']['cf_id_bitrix']
            if bitrix_id in existing_contacts:
                existing_contact_id = existing_contacts[bitrix_id]['id']
                update_contact(API_KEY_FRESHWORK, DOMAIN_FRESHWORK, contact_info, existing_contact_id)
            else:
                create_contact(API_KEY_FRESHWORK, DOMAIN_FRESHWORK, contact_info)

        print(f"Processing batch {batch_index} of {len(batches)}.")

        if batch_index < len(batches):
            print(f"Pausing for {SLEEP_TIME} seconds before the next batch...")
            time.sleep(SLEEP_TIME)

    print("Processing completed.")

if __name__ == "__main__":
    main()


# In[5]:


# Imports et clés

import pandas as pd
import requests
import configparser
from requests_oauthlib import OAuth1
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

config = configparser.ConfigParser()
config.read('config.ini')

# API Bitrix24
BITRIX_DOMAIN = config['BITRIX'].get('DOMAIN')
BITRIX_TOKEN = config['BITRIX'].get('TOKEN')


# In[46]:


# Bitrix API

def CrmBitrix(content='', params=[], result=''):

    if 'BITRIX' not in config:
        raise ValueError('Section BITRIX manquante dans le fichier \'config.ini\'')

    if not BITRIX_DOMAIN or not BITRIX_TOKEN:
        raise ValueError('Domaine et/ou token Bitrix manquants dans le fichier \'config.ini\'')

    params_str = ''
    for param in params:
        params_str += param + '&'

    BitrixUrl = f'http://{BITRIX_DOMAIN}/rest/1/{BITRIX_TOKEN}/{content}?{params_str}start='
    listUrls = []
    listIds = []
    listNomComplets = []
    listNumeros = []
    listEmails = []
    listUtmSource = []
    listaproposdelasource = []
    listCompagnePublicite = []
    listBudjetJournalier = []
    listCiblage = []
    listMessagePublicitaire = []
    listactioninitial = []
    listDatedecreation = []
    listNomDeLaCampageUtm = []
    listinscription = []
    listNAME = []
    listLastName = []
    listUrl = []
    listidd = []
    liststatut = []
    listprenom = []
    listnom = []
    listId = []
    listnote1 = []
    listnote2 =[]
    listobjectif = []
    listdureedevisionnagedulive = []
    listdureedevisionnagedureplay = []
    listachatdurantlewebinaire = []
    listachatdurantlereplay = []
    listmontantpayedurantlewebinaire = []
    listassisteaureplay=[]
    listassisteaulive =[]

    url = BitrixUrl + "0"
    while True:
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise ValueError(f'Erreur lors de la récupération depuis Bitrix : {e}')
        
        data = response.json()
        if 'result' in data:
            contacts = data['result']
            for contact in contacts:
                

                # Récupérer uniquement les adresses mails
                if result == 'email' :
                    if 'EMAIL' in contact:
                        email = contact['EMAIL'][0]['VALUE']
                        listEmails.append(email)
                        mesNumeros = contact['PHONE'][0]['VALUE']
                        listNumeros.append(mesNumeros)
                        
                elif result == 'title': 
                    maListPrenom = contact['NAME']
                    listprenom.append(maListPrenom)
                    
                    maListnom = contact['LAST_NAME']
                    listnom.append(maListnom)
                    if 'EMAIL' in contact:
                        email = contact['EMAIL'][0]['VALUE']
                        listEmails.append(email)
                    else:listEmails.append('')
                    maListdate = contact['UF_CRM_60AB812401EE3']
                    listDatedecreation.append(maListdate)
                    if 'PHONE' in contact:
                        mesNumeros = contact['PHONE'][0]['VALUE']
                        listNumeros.append(mesNumeros)
                    else:
                        listNumeros.append('')
                    
                    
                    maListid = contact['ID']
                    listId.append(maListid)
                    
                    maListurl = contact['UF_CRM_1617291265448']
                    listUrl.append(maListurl)
                    
                    maListCompagnePublicite = contact['UF_CRM_1598007792851']
                    listCompagnePublicite.append(maListCompagnePublicite)
                    
                    maListMessagePublicitaire = contact['UF_CRM_608A81E9305CF']
                    listMessagePublicitaire.append(maListMessagePublicitaire)
                    
                    maListBudjetJournalier = contact['UF_CRM_6057723EC1F82']
                    listBudjetJournalier.append(maListBudjetJournalier)
                    
                    maListinscription = contact['UF_CRM_608A81E8C0293']
                    listinscription.append(maListinscription)
                    
                    maListaproposdelasource = contact['SOURCE_DESCRIPTION']
                    listaproposdelasource.append(maListaproposdelasource)
                    
                    maListactioninitial = contact['UF_CRM_1598007820310']
                    listactioninitial.append(maListactioninitial)
                    
                    maListobjectif = contact['UF_CRM_608A81E89D1F4']
                    listobjectif.append(maListobjectif)
                    
                    maListCiblage = contact['UTM_TERM']
                    listCiblage.append(maListCiblage)
                    
                    maListidd = contact['UF_CRM_1593018402790']
                    listidd.append(maListidd)
                    
                    maListnote1 = contact['UF_CRM_1597993609253']
                    listnote1.append(maListnote1)
                    
                    maListnote2 = contact['UF_CRM_1597993625906']
                    listnote2.append(maListnote2)
                    
                    madureedevisionnagedulive = contact['UF_CRM_1614878752917']
                    listdureedevisionnagedulive.append(madureedevisionnagedulive)
                    
                    malistdureedevisionnagedureplay = contact['UF_CRM_1614878783136']
                    listdureedevisionnagedureplay.append(malistdureedevisionnagedureplay)
                    
                    malistachatdurantlewebinaire = contact['UF_CRM_1614878685354']
                    listachatdurantlewebinaire.append(malistachatdurantlewebinaire)
                    
                    malistachatdurantlereplay = contact['UF_CRM_1614878721492']
                    listachatdurantlereplay.append(malistachatdurantlereplay)
                    
                    malistmontantpayedurantlewebinaire = contact['UF_CRM_1614878992542']
                    listmontantpayedurantlewebinaire.append(malistmontantpayedurantlewebinaire)
                    
                    malistassiste_au_replay = contact['UF_CRM_1601820632']
                    listassisteaureplay.append(malistassiste_au_replay)
                    
                    malistassiste_au_live = contact['UF_CRM_608A81E90D435']
                    listassisteaulive.append(malistassiste_au_live)

                

                else : 
                    return 'Erreur avec les données attendues. Merci de vérifier la variable \'result\'.'

        if 'next' in data:
            url = BitrixUrl + str(data['next'])
        else:
            break

    # Récupérer uniquement les adresses mails
    if result == 'email' :
        df_btx = pd.DataFrame({'email': listEmails})
    elif result == 'title':
        df_btx = pd.DataFrame({
                               'ID':listId,
                               'prenom':listprenom,
                               'nom': listnom,
                               "Email":listEmails,
                               "Mobile bitrix":listNumeros,                     
                               "Date de creation":listDatedecreation,
                               "Url de la source":listUrl,
                               "Nom de la campagne de publicité utm campaign":listCompagnePublicite,
                               "Message publicitaire":listMessagePublicitaire,
                               "Budjet journalier":listBudjetJournalier,
                               "Inscription Utm Source":listinscription,
                               "A propos de la source":listaproposdelasource,
                               "Action Initiale du prospect":listactioninitial,
                               "Objectif_formulé": listobjectif,
                               "Ciblage":listCiblage,
                               "IDS":listidd,
                               "Note 1":listnote1,
                               "Note 2":listnote2,
                               "Assisté au replay" : listassisteaureplay,
                                "Assisté au live": listassisteaulive,
                               "Durée de visionnage du live":listdureedevisionnagedulive,
                               "Durée de visionnage du replay": listdureedevisionnagedureplay,
                               "Achat durant le webinaire": listachatdurantlewebinaire,
                               "Achat durant le replay": listachatdurantlereplay,
                               "Montant paye durant le webinaire": listmontantpayedurantlewebinaire,
                              
                              })
                               

    
    else:
        return 'Erreur avec les données attendues. Merci de vérifier la variable \'result\'.'

    return df_btx


##########
dfd = CrmBitrix(
    content = 'crm.contact.list.json',
    params = [
        'select[]=ID',
        'select[]=NAME',
        'select[]=LAST_NAME',
        'select[]=EMAIL',
        'select[]=PHONE',
        'select[]=UF_CRM_60AB812401EE3',
        'select[]=UF_CRM_608A81E89D1F4',
        'select[]=UF_CRM_1617291265448',
        'select[]=UF_CRM_1598007792851',
        'select[]=UF_CRM_608A81E9305CF',
        'select[]=UF_CRM_6057723EC1F82',
        'select[]=UF_CRM_608A81E8C0293',
        'select[]=SOURCE_DESCRIPTION',
        'select[]=UF_CRM_1598007820310',
        'select[]=UTM_TERM',
        'select[]=UF_CRM_1593018402790',
        'select[]=UF_CRM_1597993609253',
        'select[]=UF_CRM_1597993625906',
        'select[]=UF_CRM_1614878752917',
        'select[]=UF_CRM_1614878783136',
        'select[]=UF_CRM_1614878685354',
        'select[]=UF_CRM_1614878721492',
        'select[]=UF_CRM_1614878992542',
        'select[]=UF_CRM_1601820632',
        'select[]=UF_CRM_608A81E90D435',
        
        

    ],
    result ="title"
)

dfd['Nom'] = dfd['nom'] +" " + dfd['prenom']
dfd


# In[47]:


df_Bitrix_contact2 = dfd.copy()
#del df_Bitrix_contact2['Email societe']
df_Bitrix_contact2.to_csv("listesdescontactbitrixss.csv", index=False)
df_Bitrix_contact2


# In[36]:


import pandas as pd
import csv
import requests
import datetime
from requests.auth import HTTPBasicAuth
import re
import stripe
import sellsy_api
import numpy as np
#debut bitrix 24 sellsy societe optico acuity
import requests
import json
import pandas as pd
import csv
import json
import requests
from requests_oauthlib import OAuth1
from oauthlib.oauth1 import SIGNATURE_PLAINTEXT, SIGNATURE_TYPE_BODY
import pandas as pd
import stripe
defaultCrmProspectListUrl = "http://institutadios.bitrix24.fr/rest/1/tfs4ck7sh7t99wk2/crm.contact.userfield.list?ID="

listUrls = []
listeId = []
listID = []
listValuesource = []
listIDd = []

datas = {}

responses = requests.get(defaultCrmProspectListUrl)
        
def testedv():
    if responses.status_code == 200:
        datas = responses.json()
        companyfield = datas['result'][1]['LIST']

        for source in companyfield:
            listID.append(source['ID'])
            listValuesource.append(source['VALUE'])
           
            

            df_btx = pd.DataFrame(list(zip(listID, listValuesource)),
                              columns = ["IDS",'Etat'])

        return df_btx


# In[37]:


s = testedv()
s


# In[38]:


df_Bitrixcompany_Final2234 = df_Bitrix_contact2.merge(s, how='left')
df_Bitrixcompany_Final2234.to_csv("listes__descontactbitrix.csv", index=False)
df_Bitrixcompany_Final2234.drop_duplicates()
df_Bitrixcompany_Final2234


# In[26]:


import pandas as pd
df_Bitrixcompany_Final2234['Etat'] = df_Bitrixcompany_Final2234['Etat'].fillna("Non Contacté")
df_Bitrixcompany_Final2234['Etat'] = df_Bitrixcompany_Final2234['Etat'].replace('', "Non Contacté")
df_Bitrixcompany_Final2234.to_csv("listesdescontactbitrix.csv", index=False)
df_Bitrixcompany_Final2234


# In[ ]:


import pandas as pd
import requests
import time

url = "https://institutadios.bitrix24.fr/rest/1/tfs4ck7sh7t99wk2/"

data_list = []


def post_bitrix24_data_paginated(url, data):
    all_data = []
    start = 0 
    attempt = 0  
    max_attempts = 5 
    while True: #
        print(f"Demande de données à partir de {start}...")
        data['start'] = start
        try: 
            response = requests.post(url, json=data) 
            response.raise_for_status()
            response_data = response.json() 
            if response_data and "result" in response_data: sponse_data 
                all_data.extend(response_data["result"]) 
                print(f"Récupéré {len(response_data['result'])} éléments") #
                if "next" in response_data: 
                    start = response_data["next"] 
                else:
                    break #n
            else:
                print("Aucun résultat supplémentaire.")
                break
        except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
            print(f"Erreur de requête : {e}")
            attempt += 1# 
            if attempt >= max_attempts: # 
                print("Nombre maximal de tentatives atteint. Abandon.")
                break
            print(f"Tentative de reconnexion dans 60 secondes. Tentative {attempt} de {max_attempts}.")
            time.sleep(60)
            continue
        time.sleep(60)# 
    return all_data

def get_bitrix24_comments(entity_id, entity_type):
    comments = []
    start = 0
    while True:
        print(f"Récupération des commentaires pour l'entité {entity_id} à partir de {start}...")
        comments_url = f"{api_endpoint}crm.timeline.comment.list"
        data = {
            "filter": {
                "ENTITY_ID": entity_id,
                "ENTITY_TYPE": entity_type,
            },
            "select": ["ID", "COMMENT", "FILES"],
            "start": start
        }
        response = requests.post(comments_url, json=data)
        try:
            response.raise_for_status()
            comments_data = response.json()
            if comments_data and "result" in comments_data:
                comments.extend(comments_data["result"])
                print(f"Récupéré {len(comments_data['result'])} commentaires")
                if "next" in comments_data:
                    start = comments_data["next"]
                else:
                    break
            else:
                print("Aucun commentaire supplémentaire.")
                break
        except requests.exceptions.RequestException as e:
            print(f"Erreur de requête : {e}")
            break
        time.sleep(60)
    return comments

print("Récupération des IDs de lead...")
leads_ids_data = post_bitrix24_data_paginated(f"{api_endpoint}crm.lead.list",
                                              {"select": ["ID"]})

for lead_id in leads_ids_data:
    lead_id = lead_id["ID"]
    print(f"Traitement du lead ID: {lead_id}")
    lead_details_data = post_bitrix24_data_paginated(f"{api_endpoint}crm.contact.get",
                                                     {"id": int(lead_id), "select": ["*"]})
    if lead_details_data:
        lead_details = lead_details_data[0]
        lead_data = {"ID": lead_id}
        data_list.append(lead_data)

        print(f"Récupération des activités pour le lead ID: {lead_id}")
        activities_data = post_bitrix24_data_paginated(f"{api_endpoint}crm.activity.list", {
            "order": {"ID": "DESC"},
            "filter": {"OWNER_ID": int(lead_id)},
            "select": ["*", "COMMUNICATIONS", "RESPONSIBLE_ID", "OWNER_ID",
                       "BINDINGS", "DESCRIPTION_TYPE"]
        })

        for activity in activities_data:
            activity_data = {
                "ID": lead_id,
                
                "Commentaire": activity.get('SUBJECT', 'N/A'),
                "Date de début": activity['START_TIME'],
                "Date de fin": activity['END_TIME'],
            }
            communications = activity.get('COMMUNICATIONS', [])
            for communication in communications:
                communication_data = {"Type de communication": communication['TYPE']}
                activity_data.update(communication_data)
            data_list.append(activity_data)

        print(f"Récupération des commentaires pour le lead ID: {lead_id}")
        comments = get_bitrix24_comments(lead_id, "lead")
        for comment in comments:
            comment_data = {
                "ID": lead_id,
                
                "Commentaire": comment.get('COMMENT', 'N/A'),
            }
            data_list.append(comment_data)

df = pd.DataFrame(data_list)
print(df)


# In[2]:


#envoi des comptes dans les contacts

import csv
import requests
import json
import time

BATCH_SIZE = 500  # Taille du lot
SLEEP_TIME = 3600  # Temps d'attente entre les lots (en secondes)
API_KEY_FRESHWORK = '*******'
DOMAIN_FRESHWORK = 'institutadios'
BASE_URL = f'https://{DOMAIN_FRESHWORK}.myfreshworks.com/crm/sales/'

def get_existing_contacts(api_key, domain):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token token={api_key}'
    }
    endpoint = "contacts/view/31004512042?include=contact_status"
    url = f"{BASE_URL}{endpoint}"
    all_contacts = {}
    page = 1

    while True:
        params = {'per_page': 100, 'page': page}
        response = requests.get(url, headers=headers, params=params)
        if not response.ok:
            print(f"erreur lors de la recuperation des données: {response.status_code}")
            break
        contacts_data = response.json()['contacts']
        for contact in contacts_data:
            bitrix_id = contact['custom_field']['cf_id_bitrix']
            all_contacts[bitrix_id] = contact
        if len(contacts_data) < params['per_page']:
            break
        page += 1

    print(f"Contacts récupérés avec succès : {len(all_contacts)}")
    return all_contacts

def create_contact(api_key, domain, contact_data, contact_id):
    url = f"{BASE_URL}api/contacts"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token token={api_key}'
    }
    print("Envoi des données de contact à l'API :")

    response = requests.post(url, headers=headers, json={'contact': contact_data})
    print("Réponse de l'API lors de la création du contact :")
    #print(response.json())  
    if response.status_code == 201:
        cf_prospect = contact_data['custom_field'].get('cf_prospect')  
        status_name = contact_data['custom_field'].get('status_name')
        print(f"Contact crée avec succès : {contact_data['custom_field']['cf_id_bitrix']}. Prospect: {cf_prospect}: {response.status_code}, Statut: {status_name}")
    else:
        print(f"Erreur lors de la création du contact {contact_data['custom_field']['cf_id_bitrix']}: {response.status_code}")
    time.sleep(1)  # Pause
    return response


def update_contact(api_key, domain, contact_data, contact_id):
    url = f"{BASE_URL}api/contacts/{contact_id}"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token token={api_key}'
    }
    response = requests.put(url, headers=headers, json={'contact': contact_data})
    cf_prospect = contact_data['custom_field'].get('cf_prospect')
    status_name = contact_data['custom_field'].get('status_name')  # Définissez status_name en amont

    if response.status_code == 200:        
        print(f"Contact mis à jour avec succès : {contact_data['custom_field']['cf_id_bitrix']}. Prospect: {cf_prospect}, Statut: {status_name}")        
    else:
        # Utilisez status_name qui a été défini en amont
        print(f"Erreur lors de la mise à jour du contact {contact_data['custom_field']['cf_id_bitrix']}: {response.status_code}, Statut: {status_name}")
    time.sleep(1)  # Pause
    return response

   

def extract_contact_info(row, status_name_to_id, lifecycle_stage_id=32019529362):
    status_name = str(row["Etat"]).strip()
    status_id = status_name_to_id.get(status_name) 
    if status_id is None:  
        print(f"Statut '{status_name}' non trouvé. Vérifiez l'orthographe ou l'existence dans status_name_to_id.")
        status_id = status_name_to_id.get("Non contacté")  # Applique un statut par défaut si non trouvé
    return {
        "first_name": str(row["prenom"]),
        "last_name": str(row["nom"]),
        "email": str(row["Email"]),
        "work_number": str(row["Mobile bitrix"]),
        "custom_field": {
            "cf_prospect": str(row["Nom"]),
            "cf_url_de_la_source": str(row["Url de la source"]),
            "cf_inscription_utm_source": str(row["Inscription Utm Source"]),
            "cf_action_initiale_du_prospect": str(row["Action Initiale du prospect"]),
            "cf_objectif_formul": str(row["Objectif_formulé"]),
            "cf_date_de_creation": str(row["Date de creation"]),
            "cf_budget_journalier_utm_medium": str(row["Budjet journalier"]),
            "cf_nom_de_la_campagne_de_publicit_utm_campaign": str(row["Nom de la campagne de publicité utm campaign"]),
            "cf_ciblage_utm_term": str(row["Ciblage"]),
            "cf_message_publicitaire_utm_content": str(row["Message publicitaire"]),
            "cf_note1": str(row["Note 1"]),
            "cf_note2": str(row["Note 2"]),
            "cf_id_bitrix": str(row["ID"]),
            "status_name": status_name,
        },
        "contact_status_id": status_id,
        "lifecycle_stage_id": lifecycle_stage_id
    }

def main():
    try:
        with open('listesdescontactbitrix.csv', 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            bitrix_contacts = list(reader)
    except Exception as e:
        print("Fichier non trouvé\nErreur lors de la récupération des contacts Bitrix")
        return

    if not bitrix_contacts:
        print("Aucun contact trouvé dans le fichier CSV")
        return

    
    status_name_to_id = {
    "Non Contacté": 31001128798,
    "Faux Numéro": 31001124983,
    "Obtenir le numéro": 31001124968,
    "SMS Envoyé": 31001124969,
    "NRP Appel + Msg Vocal": 31001124970,
    "NRP Appel + SMS": 31001124971,
    "NRP Appel + Email": 31001124972,
    "SMS Répondu": 31001124973,
    "Commentaire Webinaire Répondu": 31001124974,
    "Appel effectué mais réfléchi": 31001124974,
    "Appel effectué très bon Lead": 31001124976,
    "BPF Prévu": 31001124977,
    "BPF Non venu": 31001124978,
    "BPF Effectué": 31001124979,
    "A contacter dans le futur": 31001124980,
    "Compte faire la formation": 31001124981,
    "Mail envoyé": 31001124982,
    "Demande brochure": 31001128212,
    "Intéressé session 2024": 31001128211,
    "Faux numéro": 31001124983,
    "Lead perdu":31001060873,
    "Mauvais prospect": 31001131591,
    "Trop cher pour lui": 31001131754,
    
}

   

    existing_contacts = get_existing_contacts(API_KEY_FRESHWORK, DOMAIN_FRESHWORK)

    total_contacts = len(bitrix_contacts)
    batches = [bitrix_contacts[i:i + BATCH_SIZE] for i in range(0, total_contacts, BATCH_SIZE)]

    starting_batch = 35

    existing_contact_id = None  # Initialisation de la variable
    for batch_index, batch in enumerate(batches, start=1):
        if batch_index < starting_batch:
            continue 

        for contact in batch:
            contact_info = extract_contact_info(contact, status_name_to_id)
            bitrix_id = contact_info['custom_field']['cf_id_bitrix']
            if bitrix_id in existing_contacts:
                existing_contact_id = existing_contacts[bitrix_id]['id']
                update_contact(API_KEY_FRESHWORK, DOMAIN_FRESHWORK, contact_info, existing_contact_id)
            else:
                print("Création du contact:")
                print(contact_info)  # 
                response = create_contact(API_KEY_FRESHWORK, DOMAIN_FRESHWORK, contact_info, existing_contact_id)
                if response.status_code == 201:
                    existing_contact_id = response.json()['contact']['id']

        print(f"Processing batch {batch_index} of {len(batches)}.")

        if batch_index < len(batches): # si ce n'est pas le dernier lot donc 
            print(f"Pausing for {SLEEP_TIME} seconds before the next batch...")
            time.sleep(3600)


    print("Processing completed.")

if __name__ == "__main__":
    main()

