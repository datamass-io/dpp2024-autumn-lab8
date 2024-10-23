import os
import datetime
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes._generated.models import IndexingParametersConfiguration
from azure.storage.blob import BlobServiceClient
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient, SearchIndexerClient
from azure.search.documents.indexes.models import (
    CorsOptions,
    SearchIndex,
    SearchFieldDataType,
    SimpleField,
    SearchField,
    SearchIndexerDataSourceConnection,
    SearchIndexerDataContainer,
    SearchIndexer,
    IndexingSchedule,
    IndexingParameters
)

# TODO define all the environment variables
connect_str = os.getenv('STORAGE_CONNECTION_STRING')
endpoint = os.environ["SEARCH_ENDPOINT"]
key = os.environ["SEARCH_API_KEY"]
credential = AzureKeyCredential

container_name = "search"
index_name = "jeopardy-questions-index"
data_source_name = "jeopardy-questions-on-blob-storage"


def upload_data_to_blob_storage():
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    # Create container, or if we get an exception get container client
    try:
        container_client = blob_service_client.create_container(container_name)
    except Exception as ex:
        container_client = blob_service_client.get_container_client(container_name)
        print("Exception: ")
        print(ex)

    local_path = "./data"
    local_file_name = "questions.json"
    upload_file_path = os.path.join(local_path, local_file_name)

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
    print("\nUploading questions.json to Azure Blob Storage:\n\t" + local_file_name)
    with open(file=upload_file_path, mode="rb") as data:
        blob_client.upload_blob(data)


def create_index():
    # Create the index for the following fields: id (KEY), category, question, answer, round and show number
    # note that the type of all but show number is string, for show number the type is Int32 and do not make
    # this filed searchable
    fields = [
        SimpleField(name="id", type=SearchFieldDataType.String, key=True),
        SearchField(name="category", type=SearchFieldDataType.String,
                    searchable=True, filterable=True, sortable=True, facetable=True),
        # TODO add the rest of fields to the index
    ]
    cors_options = CorsOptions(allowed_origins=["*"], max_age_in_seconds=60)
    scoring_profiles = []

    index = SearchIndex(
        name=index_name,
        fields=fields,
        scoring_profiles=scoring_profiles,
        cors_options=cors_options)

    print("\nCreating an index\n\t" + index.name)
    index_client = SearchIndexClient(endpoint, credential(key))
    # TODO create the index
    result = None  # TODO
    print(result)


def create_datasource():
    container = SearchIndexerDataContainer(name=container_name)
    data_source = SearchIndexerDataSourceConnection(
        name=data_source_name,
        type="azureblob",
        connection_string=connect_str,
        container=container
    )

    print("\nCreating a datasource\n\t" + data_source.name)
    indexer_client = SearchIndexerClient(endpoint, credential(key))
    # TODO create the data source
    result = None  # TODO
    print(result)


def create_indexer():
    indexer_client = SearchIndexerClient(endpoint, credential(key))

    indexer_schedule = IndexingSchedule(interval=datetime.timedelta(1))

    # we need this
    indexer_parameters_configuration = IndexingParametersConfiguration(
        parsing_mode='jsonArray',  # questions.json contains an array of objects
        query_timeout=None  # we need to specify it because our datasource is on the blob storage
    )

    # you can experiment with the parameters, but leave the configuration
    indexer_parameters = IndexingParameters(
        batch_size=50,
        max_failed_items=10,
        max_failed_items_per_batch=10,
        configuration=indexer_parameters_configuration
    )

    # use SearchIndexer to create a new indexer
    indexer = SearchIndexer(
        name="jeopardy-question-indexer",
        data_source_name=data_source_name,
        target_index_name=index_name,
        schedule=indexer_schedule,
        parameters=indexer_parameters
    )

    print("\nCreating a datasource\n\t" + indexer.name)
    # TODO create the indexer (it may take over 10 minutes)
    results = None  # TODO
    print(results)


def prepare_service():
    # TODO
    # fix 3 functions below (upload_data_to_blob_storage is already done), so the prepare_service function succeeds
    # run the program with only upload_data_to_blob_storage uncommented and when it succeeds you can comment it out
    # and then continue with the others

    # upload the questions.json file to the (only if it does not already exist) - THIS IS ALREADY DONE
    upload_data_to_blob_storage()

    # create an index (only if it does not already exist)
    # create_index()

    # create datasource (only if it does not already exist)
    # create_datasource()

    # create indexer (only if it does not already exist)
    # create_indexer()


def main():
    # run prepare service only once (but it needs to succeed)
    # prepare_service()

    # now we can search
    # the first is done, write the queries to answer other questions
    # TODO solve the other riddles
    how_many_to_get = 10
    search_client = SearchClient(endpoint, index_name, AzureKeyCredential(key))
    results = search_client.search(
        search_text="England",
        query_type='full',
        search_fields=['category', 'question', 'answer'],
        search_mode='any',
        include_total_count=True,
        highlight_fields='question,answer',
        highlight_pre_tag='<em>',
        highlight_post_tag='</em>',
        top=how_many_to_get
    )

    print(f"Found {results.get_count()} records with top {how_many_to_get} as follows:")
    for result in results:
        print(f'\t{result}')


if __name__ == "__main__":
    main()
