import pytest


@pytest.fixture(scope="module")
def es_index(es_client):
    index_name = "products"
    if not es_client.indices.exists(index=index_name):
        es_client.indices.create(index=index_name, body={
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 2
            }
        })
    yield index_name
    es_client.indices.delete(index=index_name)


@pytest.fixture(scope="module")
def with_document(es_index, es_client):
    # Define the document
    document = {
        "name": "Coffee maker",
        "price": 64,
        "in_stock": 10
    }
    response = es_client.index(index=es_index, body=document)
    return response['_id']


def test_create_product_document(es_index, es_client):
    # Define the document
    document = {
        "name": "Coffee maker",
        "price": 64,
        "in_stock": 10
    }
    response = es_client.index(index=es_index, body=document)
    assert response['result'] == 'created'
    assert response['_index'] == es_index


def test_create_product_document_with_id(es_index, es_client):
    document = {
        "name": "Coffee maker",
        "price": 64,
        "in_stock": 10
    }

    custom_id = "coffee_maker_001"

    response = es_client.index(index=es_index, id=custom_id, body=document)

    assert response['result'] == 'created'
    assert response['_index'] == es_index
    assert response['_id'] == custom_id

    retrieved_doc = es_client.get(index=es_index, id=custom_id)
    assert retrieved_doc['_source'] == document


def test_get_document_by_index(es_index, es_client, with_document):
    document_id = with_document

    # Retrieve the document
    retrieved_doc = es_client.get(index=es_index, id=document_id)

    # Assert that the document was retrieved successfully
    assert retrieved_doc['found'] == True
    assert retrieved_doc['_index'] == es_index
    assert retrieved_doc['_id'] == document_id

    # Assert the contents of the document
    assert retrieved_doc['_source']['name'] == "Coffee maker"
    assert retrieved_doc['_source']['price'] == 64
    assert retrieved_doc['_source']['in_stock'] == 10


def test_update_document(es_index, es_client, with_document):
    document_id = with_document

    # Define the update
    update_body = {
        "doc": {
            "price": 59,
            "in_stock": 5,
            "on_sale": True
        }
    }

    # Perform the update
    update_response = es_client.update(index=es_index, id=document_id, body=update_body)

    # Assert the update was successful
    assert update_response['result'] == 'updated'

    # Retrieve the updated document
    updated_doc = es_client.get(index=es_index, id=document_id)

    # Assert the document was updated correctly
    assert updated_doc['_source']['name'] == "Coffee maker"  # Unchanged field
    assert updated_doc['_source']['price'] == 59  # Updated field
    assert updated_doc['_source']['in_stock'] == 5  # Updated field
    assert updated_doc['_source']['on_sale'] == True  # New field

    # Optional: Assert that the version has increased
    assert updated_doc['_version'] > 1


def test_scripted_update(es_index, es_client, with_document):
    document_id = with_document

    # Define the scripted update
    script_update = {
        "script": {
            "source": """
           if (ctx._source.in_stock > 0) {
               ctx._source.in_stock--;
           }
           """,
        }
    }

    # Perform the scripted update
    update_response = es_client.update(index=es_index, id=document_id, body=script_update)

    # Assert the update was successful
    assert update_response['result'] == 'updated'

    # Retrieve the updated document
    updated_doc = es_client.get(index=es_index, id=document_id)

    # Assert the document was updated correctly
    assert updated_doc['_source']['name'] == "Coffee maker"  # Unchanged
    assert updated_doc['_source']['in_stock'] == 9  # Decreased by 1

    # Optional: Assert that the version has increased
    assert updated_doc['_version'] > 1


def test_scripted_update_upsert(es_index, es_client, with_document):
    document_id = with_document

    # Define the scripted update with upsert
    script_update = {
        "script": {
            "source": """
           if (ctx._source.in_stock > 0) {
               ctx._source.in_stock--;
           }
           """
        },
        "upsert": {
            "name": "New Coffee Maker",
            "price": 75,
            "in_stock": 5
        }
    }

    # Perform the scripted update with upsert on existing document
    update_response = es_client.update(index=es_index, id=document_id, body=script_update)

    # Assert the update was successful
    assert update_response['result'] == 'updated'

    # Retrieve the updated document
    updated_doc = es_client.get(index=es_index, id=document_id)

    # Assert the document was updated correctly
    assert updated_doc['_source']['name'] == "Coffee maker"  # Unchanged
    assert updated_doc['_source']['in_stock'] == 9  # Decreased by 1
    assert updated_doc['_version'] > 1

    # Now, let's try an upsert on a non-existent document
    new_id = "new_coffee_maker_001"

    # Perform the scripted update with upsert on non-existent document
    new_update_response = es_client.update(index=es_index, id=new_id, body=script_update)

    # Assert the upsert was successful
    assert new_update_response['result'] == 'created'
