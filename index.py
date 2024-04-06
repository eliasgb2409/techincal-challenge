from opensearchpy import OpenSearch
import json
from dotenv import load_dotenv
import os
from opensearch_functions import create_index, index_documents, count_indicies, search


         
def load_json(path):
    '''
    Function for loading a json file. 

    Input (str): path to json-file
    Return: loaded json-file
    ''' 
    with open(path, 'r') as file:
        return json.load(file)
    

def main():
    """
    1. Create the client for OpenSearch
    2. Index documents 
    3. Count indicies
    """

    load_dotenv()
    ADMIN_USERNAME = os.getenv('ADMIN_USERNAME')
    ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD')

    host = 'localhost'
    port = 9200
    auth = (ADMIN_USERNAME, ADMIN_PASSWORD) 
    client = OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_auth = auth,
        use_ssl = True,
        verify_certs = False,
        ssl_assert_hostname = False,
        ssl_show_warn=False,
    )

    info = client.info()
    
    index_name = "ardoq-challenge"
    
    create_index(index_name, client)
    
     # Import data to index
    documents = load_json("components.json")
    ref_documents = load_json("references.json")
    client.indices.refresh(index="ardoq-challenge")
    
    print("Indexing...")
    print("Indexing components.json...")
    index_documents("ardoq-challenge", client, documents)
    print(f'\nIndexing references.json...')
    index_documents("ardoq-challenge", client, ref_documents)
    print("Done indexing...")
    
    count_indicies(index_name, client)

    
if __name__ == "__main__":
    main()
