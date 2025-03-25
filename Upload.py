import streamlit as st
import json
import subprocess
import os
import DashBoard

def main():
    st.title("Role Based DashBoard Access")
    
    uploaded_file = st.file_uploader("Upload your service account JSON key", type=["json"])
    
    if uploaded_file is not None:
        try:
            service_key = json.load(uploaded_file)
            
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
            
            st.success(f"Service Account: {service_account_email}")
            if roles:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json.dumps(service_key)
                st.write("### Assigned Roles:")
                for role in roles:
                    st.write(f"- {role}")
                
                if "roles/bigquery.admin" in roles:
                    role=1
                    DashBoard.main(role)
                else:
                    role=0
                    DashBoard.main(role)
            else:
                st.warning("No IAM roles found for this service account.")
        
        except json.JSONDecodeError:
            st.error("Invalid JSON file uploaded.")

if __name__ == "__main__":
    main()
