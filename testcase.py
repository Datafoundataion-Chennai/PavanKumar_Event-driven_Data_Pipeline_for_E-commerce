import pytest
import pandas as pd
import streamlit as st
import json
import subprocess
import os
from unittest.mock import patch, MagicMock
from DashBoard import fetch_batch_data, fetch_realtime_events, fetch_all_events, main
from loggingModule import logger
from Streamlit_File_Upload import main as streamlit_main

def test_fetch_batch_data():
    mock_query = "SELECT * FROM test_table"
    mock_data = pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c']
    })
    
    with patch("DashBoard.client.query") as mock_query_method:
        mock_query_method.return_value.to_dataframe.return_value = mock_data
        result = fetch_batch_data(mock_query)
        
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert result.equals(mock_data)
        logger.info("test_fetch_batch_data passed.")

def test_fetch_realtime_events():
    mock_data = pd.DataFrame({
        'EventId': [101, 102],
        'UserId': [201, 202],
        'EventType': ['view_product', 'add_to_cart'],
        'ProductId': [301, 302],
        'Price': [29.99, 19.99],
        'TimeStamp': ['2025-03-20T12:00:00', '2025-03-20T12:01:00']
    })
    
    with patch("DashBoard.client.query") as mock_query_method:
        mock_query_method.return_value.to_dataframe.return_value = mock_data
        result = fetch_realtime_events(10)
        
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert result.equals(mock_data)
        logger.info("test_fetch_realtime_events passed.")

def test_fetch_all_events():
    mock_data = pd.DataFrame({
        'EventId': [111, 112],
        'EventType': ['purchase', 'login']
    })
    
    with patch("DashBoard.client.query") as mock_query_method:
        mock_query_method.return_value.to_dataframe.return_value = mock_data
        result = fetch_all_events()
        
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert result.equals(mock_data)
        logger.info("test_fetch_all_events passed.")

@pytest.mark.parametrize("button_name, session_key", [
    ("📊 Overview", "Overview"),
    ("📦 Order Summary", "Order Summary"),
    ("📈 Event Analytics", "Event Analytics"),
    ("⚡ Live Event Feed", "Live Event Feed"),
])
def test_button_click(button_name, session_key):
    with patch("streamlit.button") as mock_button:
        mock_button.return_value = True
        st.session_state.view_option = None
        main()
        assert st.session_state.view_option == session_key
        logger.info(f"test_button_click passed for {session_key}.")
def test_missing_file():
    """Test case where no file is uploaded."""
    with patch("streamlit.file_uploader") as mock_file_uploader, patch("streamlit.error") as mock_error:
        mock_file_uploader.return_value = None
        streamlit_main()
        mock_error.assert_not_called()
    logger.info("test_missing_file passed.")

def test_invalid_json_file():
    """Test case where an invalid JSON file is uploaded."""
    with patch("streamlit.file_uploader") as mock_file_uploader, patch("streamlit.error") as mock_error:
        mock_file = MagicMock()
        mock_file.read.return_value = b"{invalid json}"
        mock_file_uploader.return_value = mock_file

        streamlit_main()
        mock_error.assert_called_with("Invalid JSON file uploaded.")
    logger.info("test_invalid_json_file passed.")

def test_missing_client_email():
    """Test case where JSON file is missing 'client_email'."""
    with patch("streamlit.file_uploader") as mock_file_uploader, patch("streamlit.error") as mock_error:
        mock_file = MagicMock()
        mock_file.read.return_value = json.dumps({"project_id": "test-project"}).encode()
        mock_file_uploader.return_value = mock_file

        streamlit_main()
        mock_error.assert_called_with("No client_email found in service key JSON")
    logger.info("test_missing_client_email passed.")

def test_missing_project_id():
    """Test case where JSON file is missing 'project_id'."""
    with patch("streamlit.file_uploader") as mock_file_uploader, patch("streamlit.error") as mock_error:
        mock_file = MagicMock()
        mock_file.read.return_value = json.dumps({"client_email": "test@example.com"}).encode()
        mock_file_uploader.return_value = mock_file

        streamlit_main()
        mock_error.assert_called_with("No project_id found in service key JSON")
    logger.info("test_missing_project_id passed.")

def test_gcloud_command_failure():
    """Test case where gcloud command fails."""
    with patch("streamlit.file_uploader") as mock_file_uploader, \
         patch("streamlit.error") as mock_error, \
         patch("subprocess.run") as mock_subprocess:

        mock_file = MagicMock()
        mock_file.read.return_value = json.dumps({
            "client_email": "test@example.com",
            "project_id": "test-project"
        }).encode()
        mock_file_uploader.return_value = mock_file

        mock_subprocess.return_value = MagicMock(returncode=1, stderr="Error fetching IAM policy")

        streamlit_main()
        mock_error.assert_called_with("Error fetching IAM policy: Error fetching IAM policy")
    logger.info("test_gcloud_command_failure passed.")

@pytest.mark.parametrize("roles, expected_page", [
    (["roles/bigquery.admin"], "Dashboard.py"),
    (["roles/viewer"], "UserDashboard.py"),
])
def test_role_based_navigation(roles, expected_page):
    """Test navigation based on IAM roles."""
    with patch("streamlit.file_uploader") as mock_file_uploader, \
         patch("subprocess.run") as mock_subprocess, \
         patch("streamlit.switch_page") as mock_switch_page:

        mock_file = MagicMock()
        mock_file.read.return_value = json.dumps({
            "client_email": "test@example.com",
            "project_id": "test-project"
        }).encode()
        mock_file_uploader.return_value = mock_file

        mock_subprocess.return_value = MagicMock(returncode=0, stdout=json.dumps({
            "bindings": [{"role": roles[0], "members": ["user:test@example.com"]}]
        }))

        streamlit_main()
        mock_switch_page.assert_called_with(expected_page)
    logger.info(f"test_role_based_navigation passed for {expected_page}.")

if __name__ == "__main__":
    pytest.main()
