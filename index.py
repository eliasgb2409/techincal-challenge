from opensearchpy import OpenSearch, AsyncOpenSearch
import json
from dotenv import load_dotenv
import os
from opensearch_functions import create_index, index_documents, count_indicies, search
import asyncio
import aiofiles
         
async def load_json(path):
    '''
    Function for loading a json file. 

    Input (str): path to json-file
    Return: loaded json-file
    ''' 
    async with aiofiles.open(path, 'r') as file:
        data = await file.read()
        return json.loads(data)
    

async def main():
    """
    1. Create the client for OpenSearch
    2. Creating index
    3. Reading json-files
    4. Index documents 
    5. Count indicies
    """

    load_dotenv()
    ADMIN_USERNAME = os.getenv('ADMIN_USERNAME')
    ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD')

    # Creating client
    host = 'localhost'
    port = 9200
    auth = (ADMIN_USERNAME, ADMIN_PASSWORD) 
    client = AsyncOpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_auth = auth,
        use_ssl = True,
        verify_certs = False,
        ssl_assert_hostname = False,
        ssl_show_warn=False,
        http_compress = True, # enables gzip compression for request bodies
    )

    try:
        
        info = await client.info()
        
        index_name = "ardoq-challenge"
        
        # Creating index
        await create_index(index_name, client)
        
        # Reading json-files
        comp_documents = await load_json("components.json")
        ref_documents = await load_json("references.json")
        await client.indices.refresh(index="ardoq-challenge")
        
        # Import data to index and run the indexing-functions concurrently
        await asyncio.gather(
            index_documents("ardoq-challenge", client, comp_documents, "components.json"),
            index_documents("ardoq-challenge", client, ref_documents, "references.json")
        )
        print("Done indexing...")
        await count_indicies(index_name, client) #Printing out the number of documents in index
    
    finally:
        await client.close()

    
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
    loop.close()