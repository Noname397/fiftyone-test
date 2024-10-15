# test_app.py
import pytest
from starlette.testclient import TestClient
from app_request import app  # Assuming the app is in a file named app.py

# Create a TestClient instance to interact with the app
client = TestClient(app)

def test_filepath_extraction():
    # Define the test URL
    test_url = "http://localhost:5151/media?filepath=s3://knkenkkas/adksa"
    
    # Send a GET request to the app
    response = client.get(test_url)
    
    # Assert the response status is 200 OK
    assert response.status_code == 200
    
    # Assert that the content is as expected
    assert response.text == "Retrieved filepath: s3://knkenkkas/adksa"
