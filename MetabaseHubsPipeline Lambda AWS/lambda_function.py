import json
import pandas as pd
import numpy as np
import requests
import boto3
from io import StringIO
import os
from hubspot import HubSpot
from hubspot.crm.contacts import SimplePublicObjectInputForCreate, ApiException

# Mapeamento das chaves antigas para as novas
key_mapping = {
    "Monthly Income": "faixa_de_faturamento",
    "Email": "email",
    "Created At": "data_de_mql_pro",
    "Shop ID": "bgstoreid",
    "Utm Campaign": "oficial_utm_campaign",
    "Utm Source": "oficial_utm_source",
    "Utm Medium": "oficial_utm_medium",
    "Erp Discounts - Discount → Code": "bg_discountcoupon",
    "Phone": "phone",
    "Name": "firstname",
    "Erp Partners - Partner → Name": "partner_id",
    "Erp Plans - Campaign → Name": "bg_subscriptionplanname"
}

# Chaves a serem removidas
keys_to_remove = {"ID", "Updated At", "Converted At", "Campaign ID", "Erp Plans - Campaign → Price"}

def transform_data(data_list, key_mapping, keys_to_remove):
    transformed_list = []
    for item in data_list:
        new_item = {}
        for key, value in item.items():
            if key in key_mapping:
                # Substituir a chave pelo novo nome
                new_item[key_mapping[key]] = value
            elif key not in keys_to_remove:
                # Manter chaves que não estão no mapeamento nem na lista de remoção
                new_item[key] = value
                
        new_item["formulario_integracao"] = "MetabaseHubsPipeline"
        new_item["origem_do_lead"] = "Checkout"
        transformed_list.append(new_item)
    return transformed_list

def get_contact_id(cliente, email):
    contact = cliente.crm.contacts.basic_api.get_by_id(email, id_property='email', archived=False)
    contact_id = (contact.to_dict()['id'])
    return contact_id

def lambda_handler(event, context):
    response_lambda_body = {}
    
    
    metabase_url = os.getenv('METABASE_URL')
    username = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    hubspot_api_key = os.getenv('HUBSPOT_API_KEY')
    
    s3 = boto3.client('s3')
    client = HubSpot(access_token=hubspot_api_key)
    

    s3_response = s3.get_object(Bucket="leadscheckout", Key="json_atual.json")
    
    file_content = s3_response['Body'].read().decode('utf-8')

    json_atual = json.loads(file_content)
    

    auth_payload = {
        "username": username,
        "password": password
    }
    metabase_response = requests.post(f"{metabase_url}/api/session", json=auth_payload)

    if metabase_response.status_code == 200:
        session_token = metabase_response.json()["id"]

    else:
        body = {
            "message": f"The Client App is not authorized to access the data",
            "resposta": metabase_response.reason
        }
        
        response = {
            "statusCode": metabase_response.status_code,
            "body": json.dumps(body)
        }
        
        return response
    
    response_lambda_body["session_token"] = session_token
    
    
    headers_query = {
        "Content-Type": 'application/x-www-form-urlencoded',
        "X-Metabase-Session": session_token
    }
    question_id = 903
    
    query_response = requests.post(f"{metabase_url}/api/card/{question_id}/query/json", headers=headers_query)
    
    json_novo = json.loads(query_response.content)

    json_para_carregar = [item for item in json_novo if item not in json_atual]
    
    
    json_para_carregar_transformado = transform_data(json_para_carregar, key_mapping, keys_to_remove)
    response_lambda_body["objetos_para_upload"] = len(json_para_carregar_transformado)

    
    for lead_checkout in json_para_carregar_transformado:
        
        properties = {}
        for key, value in lead_checkout.items():
            if value:
                properties[key] = value;
            
        email = properties['email']
        
        simple_public_object_input_for_create = SimplePublicObjectInputForCreate(properties=properties)
        
        try:
            api_response = client.crm.contacts.basic_api.create(simple_public_object_input_for_create=simple_public_object_input_for_create)

        except ApiException as e:
            contact_id = get_contact_id(client, email)
            api_response = client.crm.contacts.basic_api.update(contact_id=contact_id, simple_public_object_input=simple_public_object_input_for_create)
            
   
        
        
    s3 = boto3.client('s3')

    s3.put_object(Bucket='leadscheckout', Key='json_atual.json', Body = query_response.content)



    response = {
        "statusCode": 200,
        "body": json.dumps(response_lambda_body)
    }
    
    return response

