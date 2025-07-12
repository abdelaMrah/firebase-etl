import firebase_admin
from firebase_admin import credentials, firestore
import json
import pandas as pd
from typing import List, Dict, Optional

class FirebaseUserService:
    def __init__(self, service_account_path: str = 'service-account.json'):
        """
        Initialize Firebase service with service account credentials
        """
        self.cred = credentials.Certificate(service_account_path)
        
        # Initialize Firebase app if not already initialized
        if not firebase_admin._apps:
            firebase_admin.initialize_app(self.cred)
        
        # Initialize Firestore client
        self.db = firestore.client()
        self.users_collection = 'users'
    
    def get_all_users_raw(self) -> pd.DataFrame:
        """
        Extract all users from the users collection as pandas DataFrame
        Returns raw data for ETL processing
        """
        try:
            users_ref = self.db.collection(self.users_collection)
            docs = users_ref.stream()
            users = []
            for doc in docs:
                user_data = doc.to_dict()
                user_data['id'] = doc.id  # Add document ID
                users.append(user_data)
            
            # Convert to DataFrame for ETL processing
            df = pd.DataFrame(users)
            return df
        except Exception as e:
            print(f"Error fetching all users: {e}")
            return pd.DataFrame()  # Return empty DataFrame on error
    
    def get_all_users(self) -> List[Dict]:
        """
        Extract all users from the users collection (legacy method)
        """
        try:
            users_ref = self.db.collection(self.users_collection)
            docs = users_ref.stream()
            users = []
            for doc in docs:
                user_data = doc.to_dict()
                user_data['id'] = doc.id  # Add document ID
                users.append(user_data)
            
            return users
        except Exception as e:
            print(f"Error fetching all users: {e}")
            return []
    
    def get_user_by_id_raw(self, user_id: str) -> pd.DataFrame:
        """
        Get a specific user by ID as DataFrame
        """
        try:
            doc_ref = self.db.collection(self.users_collection).document(user_id)
            doc = doc_ref.get()
            
            if doc.exists:
                user_data = doc.to_dict()
                user_data['id'] = doc.id
                return pd.DataFrame([user_data])
            else:
                print(f"User with ID {user_id} not found")
                return pd.DataFrame()
        except Exception as e:
            print(f"Error fetching user {user_id}: {e}")
            return pd.DataFrame()
    
    def get_user_by_id(self, user_id: str) -> Optional[Dict]:
        """
        Get a specific user by ID (legacy method)
        """
        try:
            doc_ref = self.db.collection(self.users_collection).document(user_id)
            doc = doc_ref.get()
            
            if doc.exists:
                user_data = doc.to_dict()
                user_data['id'] = doc.id
                return user_data
            else:
                print(f"User with ID {user_id} not found")
                return None
        except Exception as e:
            print(f"Error fetching user {user_id}: {e}")
            return None
    
    def get_users_with_filter_raw(self, field: str, operator: str, value) -> pd.DataFrame:
        """
        Get users with specific filter criteria as DataFrame
        Example: get_users_with_filter_raw('age', '>=', 18)
        """
        try:
            users_ref = self.db.collection(self.users_collection)
            query = users_ref.where(field, operator, value)
            docs = query.stream()
            
            users = []
            for doc in docs:
                user_data = doc.to_dict()
                user_data['id'] = doc.id
                users.append(user_data)
            
            return pd.DataFrame(users)
        except Exception as e:
            print(f"Error fetching users with filter: {e}")
            return pd.DataFrame()
    
    def get_users_with_filter(self, field: str, operator: str, value) -> List[Dict]:
        """
        Get users with specific filter criteria (legacy method)
        """
        try:
            users_ref = self.db.collection(self.users_collection)
            query = users_ref.where(field, operator, value)
            docs = query.stream()
            
            users = []
            for doc in docs:
                user_data = doc.to_dict()
                user_data['id'] = doc.id
                users.append(user_data)
            
            return users
        except Exception as e:
            print(f"Error fetching users with filter: {e}")
            return []
    
    def export_users_to_json(self, filename: str = 'users_export.json') -> bool:
        """
        Export all users to a JSON file
        """
        try:
            users = self.get_all_users()
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(users, f, indent=2, ensure_ascii=False, default=str)
            print(f"Users exported to {filename}")
            return True
        except Exception as e:
            print(f"Error exporting users: {e}")
            return False
    
    def export_users_to_csv(self, filename: str = 'users_export.csv') -> bool:
        """
        Export all users to a CSV file using pandas
        """
        try:
            df = self.get_all_users_raw()
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"Users exported to {filename}")
            return True
        except Exception as e:
            print(f"Error exporting users to CSV: {e}")
            return False
    
    def get_dataframe_info(self) -> Dict:
        """
        Get information about the users DataFrame for ETL analysis
        """
        try:
            df = self.get_all_users_raw()
            info = {
                'shape': df.shape,
                'columns': df.columns.tolist(),
                'dtypes': df.dtypes.to_dict(),
                'null_counts': df.isnull().sum().to_dict(),
                'memory_usage': df.memory_usage(deep=True).sum()
            }
            return info
        except Exception as e:
            print(f"Error getting DataFrame info: {e}")
            return {}

# Example usage for ETL
if __name__ == "__main__":
    # Initialize the service
    firebase_service = FirebaseUserService()
    
    # Get all users as DataFrame (raw data for ETL)
    users_df = firebase_service.get_all_users_raw()
    print(f"DataFrame shape: {users_df.shape}")
    print(f"Columns: {users_df.columns.tolist()}")
    
    # Get DataFrame info for ETL analysis
    df_info = firebase_service.get_dataframe_info()
    print(f"DataFrame info: {df_info}")
    
    # Export to CSV for further ETL processing
    firebase_service.export_users_to_csv()
    
    # Example: Filter users and get as DataFrame
    # active_users_df = firebase_service.get_users_with_filter_raw('status', '==', 'active')
    # print(f"Active users: {active_users_df.shape[0]}")
