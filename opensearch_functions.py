from opensearchpy import helpers
import json


def create_index(name, client):
    '''
    Function for creating index. 

    Input (str): name of index
    Return: None
    '''
    # Explicit mapping of index (tried to do dynamic, but got some errors due to some fields named _version, _id, etc.)
    # The mapping will tell the index how to store the documents 
    index_body = {
        "settings": {
            "number_of_shards": 2,
            "number_of_replicas": 1,
        }
    }
    
    index_name = name
    
    try: # Create index in OpenSearch cluster with name of index and the body that we mapped 
        response = client.indices.create(
            index_name, 
            body=index_body
        )
        print('\nCreating index:')
        print(response)
        
    except Exception as e: #If index exist, do nothing
        print(e)
        

       
def delete_index(index_name, client):
    '''
    Function for deleting an index. 

    Input (str): name of index to delete
    Return: response of deletion
    ''' 
    response = client.indices.delete(
        index = index_name
    )
    print(response)
    return response
    
      
def index_documents(index_name, client, documents): 
    '''
    Function for importing documents with bulk operation to an index. 

    Input (str): name of index, client, documents
    Return: response of import
    ''' 
    
    actions = [] #actions to be made when doing bulk opertions
    
    # Iterate over every document in the json-file
    for document in documents:
        doc_id = document.pop("_id", None)  # Remove _id from document and use it in the action
        
        # We face a naming conflict iwth the _version key in the json-files
        # OpenSearch reserves the _version field for its internal use and we therefore have to change its name before processing
        if "_version" in document:
            document["app_version"] = document.pop("_version")
        
        # Transform json-object to string, was not able to parse this before
        if "origin" in document:
            document["origin"] = json.dumps(document["origin"])
        
        # Every action in a bulk operation needs at least an index, therefore we add this and the fields _id and _soruce to each document
        actions.append(
            {
                "_index": index_name,
                "_id": doc_id,
                "_source": document,
            }
        )
    
    # Perform bulk operation to client
    response = helpers.bulk(client, actions)
    print("Response: ", response)
    return response


def count_indicies(index_name, client):
    '''
    Function for counting the number in an index. 

    Input (str): name of index
    Return: None
    '''
    client.indices.refresh(index=index_name)
    print(client.cat.count(index=index_name, format="json"))
    

# Search for data with API call
def search(index_name, field, searchword, client):
    resp = client.search(
        index=index_name,
        body={
            "query": {
                "bool": {
                    "must": [ {
                        "match_phrase": {
                            field: searchword,
                        }
                    }],
                },
            },            
        }
    )
    
    print(resp)
    return resp