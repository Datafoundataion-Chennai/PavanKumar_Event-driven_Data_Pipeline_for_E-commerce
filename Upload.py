import streamlit as st
import json
import subprocess
import os
import DashBoard

def main():
    st.title("Role-Based Dashboard Access")
    
    json_key_path = st.text_input("Enter the path to your service account JSON key(without quotes):")
    
    if json_key_path:
        if not os.path.exists(json_key_path):
            st.error("File not found. Please enter a valid file path.")
            return
        
        try:
            with open(json_key_path, "r") as f:
                service_key = json.load(f)
            
            service_account_email = service_key.get("client_email")
            if not service_account_email:
                st.error("No client_email found in service key JSON")
                return
            
            project_id = service_key.get("project_id")
            if not project_id:
                st.error("No project_id found in service key JSON")
                return
            
            gcloud_command = f"gcloud projects get-iam-policy {project_id} --format=json"
            result = subprocess.run(gcloud_command, shell=True, capture_output=True, text=True)
            
            if result.returncode != 0:
                st.error(f"Error fetching IAM policy: {result.stderr}")
                return
            
            iam_policy = json.loads(result.stdout)
            
            roles = []
            for binding in iam_policy.get("bindings", []):
                if any(service_account_email in member for member in binding.get("members", [])):
                    roles.append(binding["role"])
           
            if roles:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_key_path
                st.write("### Assigned Roles:")
                for role in roles:
                    st.write(f"- {role}")
                
                if "roles/bigquery.admin" in roles:
                    DashBoard.main(role=1)
                else:
                    DashBoard.main(role=0)
            else:
                st.warning("No IAM roles found for this service account.")
        
        except json.JSONDecodeError:
            st.error("Invalid JSON file. Please check the contents and try again.")

if __name__ == "__main__":
    main()
