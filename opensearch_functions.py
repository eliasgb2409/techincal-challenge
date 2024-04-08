from opensearchpy import helpers
import json
import time

async def create_index(name, client):
    '''
    Function for creating index. 

    Input (str): name of index, client
    Return: None
    '''
    
    # Explicit mapping of index (tried to do dynamic, but got some errors due to some fields named _version, _id, etc.)
    # The mapping will tell the index how to store the documents 
    index_body = {
        "settings": {
            "number_of_shards": 2,
            "number_of_replicas": 1,
            "refresh_interval": "30s", # Setting a higher refresh_interval to improve indexing performance
        }
    }
        
    index_name = name
    
    try: # Create index in OpenSearch cluster with name of index and the body that we mapped
        if not await client.indices.exists(index=index_name):
            
            response = await client.indices.create(
                index_name, 
                body=index_body
            )
            print('\nCreating index:')
            print(response)
        
    except Exception as e: #If index exist, do nothing
        print(e)
        

       
async def delete_index(index_name, client):
    '''
    Function for deleting an index. 

    Input (str): name of index to delete, client
    Return: response of deletion
    ''' 
    response = await client.indices.delete(
        index = index_name
    )
    print(response)
    return response
    
      
async def index_documents(index_name, client, documents, file_name): 
    '''
    Function for importing documents with bulk operation to an index. 

    Input (str): name of index, client, documents to index, name of file to index
    Return: response of import
    ''' 
    
    start_time = time.time()
    actions = [] #actions to be made when doing bulk opertions
    
    
    # Iterate over every document in the json-file
    for document in documents:
        doc_id = document.pop("_id", None)  # Remove _id from document and use it in the action
        
        # We face a naming conflict with the _version key in the json-files
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
    response = await helpers.async_bulk(client, actions)
    duration = time.time() - start_time
    print(f'\nIndexing...')
    print(f'Indexing {file_name}...')
    print("Response: ", response)
    print(f'Bulk indexing completed in {format(duration,".2f")} seconds.')
    return response


async def count_indicies(index_name, client):
    '''
    Function for counting the number in an index. 

    Input (str): name of index, client
    Return: None
    '''
    await client.indices.refresh(index=index_name)
    print(await client.cat.count(index=index_name, format="json"))
    

# Search for data with API call
async def search(index_name, field, searchword, client):
    resp = await client.search(
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