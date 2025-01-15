import os
from datetime import datetime, timedelta
import logging
from google.cloud import bigquery
import requests
from typing import Dict, List, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClickUpToBigQuery:
    def __init__(self):
        self.api_token = os.environ.get('CLICKUP_API_TOKEN')
        self.workspace_id = os.environ.get('CLICKUP_WORKSPACE_ID')
        self.base_url = 'https://api.clickup.com/api/v2'
        self.client = bigquery.Client()
        
        self.headers = {
            'Authorization': self.api_token,
            'Content-Type': 'application/json'
        }

    def fetch_custom_fields(self, workspace_id: str) -> List[Dict]:
        """Fetch all custom fields for the workspace"""
        url = f"{self.base_url}/workspace/{workspace_id}/custom_fields"
        response = requests.get(url, headers=self.headers)
        return response.json().get('custom_fields', [])

    def fetch_custom_task_types(self, workspace_id: str) -> List[Dict]:
        """Fetch all custom task types for the workspace"""
        url = f"{self.base_url}/workspace/{workspace_id}/custom_task_types"
        response = requests.get(url, headers=self.headers)
        return response.json().get('custom_task_types', [])

    def fetch_tasks(self, workspace_id: str) -> List[Dict]:
        """Fetch all tasks with their custom fields"""
        tasks = []
        
        # First get all spaces
        spaces_url = f"{self.base_url}/team/{workspace_id}/space"
        spaces = requests.get(spaces_url, headers=self.headers).json().get('spaces', [])
        
        for space in spaces:
            # Get all lists (including those in folders)
            lists_url = f"{self.base_url}/space/{space['id']}/list"
            lists = requests.get(lists_url, headers=self.headers).json().get('lists', [])
            
            for list_obj in lists:
                # Get tasks for each list
                tasks_url = f"{self.base_url}/list/{list_obj['id']}/task"
                response = requests.get(tasks_url, headers=self.headers)
                list_tasks = response.json().get('tasks', [])
                
                for task in list_tasks:
                    # Flatten custom fields
                    custom_fields = task.get('custom_fields', [])
                    for field in custom_fields:
                        task[f"custom_field_{field['id']}"] = field.get('value')
                    
                    # Add context
                    task['space_id'] = space['id']
                    task['list_id'] = list_obj['id']
                    
                    tasks.append(task)
        
        return tasks

    def create_tables_if_not_exist(self):
        """Create BigQuery tables if they don't exist"""
        
        # Tasks table schema
        tasks_schema = [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("priority", "INTEGER"),
            bigquery.SchemaField("due_date", "TIMESTAMP"),
            bigquery.SchemaField("space_id", "STRING"),
            bigquery.SchemaField("list_id", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ]
        
        # Add custom fields to schema
        custom_fields = self.fetch_custom_fields(self.workspace_id)
        for field in custom_fields:
            field_type = "STRING"  # Default type
            if field['type'] == 'number':
                field_type = "FLOAT"
            elif field['type'] == 'date':
                field_type = "TIMESTAMP"
            
            tasks_schema.append(
                bigquery.SchemaField(f"custom_field_{field['id']}", field_type)
            )

        # Create tables
        dataset_ref = self.client.dataset('clickup_data')
        
        table_ref = dataset_ref.table('tasks')
        table = bigquery.Table(table_ref, schema=tasks_schema)
        self.client.create_table(table, exists_ok=True)

    def sync_data(self):
        """Main sync function"""
        try:
            logger.info("Starting ClickUp to BigQuery sync")
            
            # Create tables if they don't exist
            self.create_tables_if_not_exist()
            
            # Fetch and load tasks
            tasks = self.fetch_tasks(self.workspace_id)
            
            if tasks:
                # Load to BigQuery
                table_ref = self.client.dataset('clickup_data').table('tasks')
                
                # Configure the load job
                job_config = bigquery.LoadJobConfig()
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
                
                # Load the data
                job = self.client.load_table_from_json(
                    tasks,
                    table_ref,
                    job_config=job_config
                )
                
                job.result()  # Wait for the job to complete
                
                logger.info(f"Loaded {len(tasks)} tasks to BigQuery")
            
            logger.info("Sync completed successfully")
            
        except Exception as e:
            logger.error(f"Error during sync: {str(e)}")
            raise 