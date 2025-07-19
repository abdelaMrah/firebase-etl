import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from firebase_admin import auth
import pandas as pd
import os
from typing import Dict, Any, List
import json

class FirebaseUserService:
    def __init__(self):
        """
        Initialize Firebase Realtime Database client using service account
        """
        try:
            # Initialize Firebase Admin SDK if not already initialized
            if not firebase_admin._apps:
                # Path to your service account key file
                key_path = os.getenv('FIREBASE_SERVICE_ACCOUNT_KEY_PATH', './service-account.json')
                database_url = os.getenv('FIREBASE_DATABASE_URL', 'https://kastudio-6a436-default-rtdb.firebaseio.com/')
                
                print(f"🔐 Using service account key: {key_path}")
                print(f"🔗 Database URL: {database_url}")
                
                if not os.path.exists(key_path):
                    raise FileNotFoundError(f"Service account key file not found: {key_path}")
                
                # Load credentials
                cred = credentials.Certificate(key_path)
                
                # Initialize Firebase Admin with Realtime Database URL
                firebase_admin.initialize_app(cred, {
                    'databaseURL': database_url
                })
                
                print("✓ Firebase Admin SDK initialized successfully")
            else:
                print("✓ Firebase app already initialized")
            
            # Get mode configuration
            self.mode = os.getenv('MODE', 'prod').lower()
            self.dev_user_limit = int(os.getenv('DEV_USER_LIMIT', '1000'))
            
            print(f"🔧 Running in {self.mode.upper()} mode")
            if self.mode == 'dev':
                print(f"📊 Development mode: limiting to {self.dev_user_limit} users")
            
            print("✓ Firebase Realtime Database client ready")
            
        except Exception as e:
            print(f"❌ Error initializing Firebase Realtime Database: {e}")
            raise

    def _determine_provider_and_email(self, uid: str, user_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Détermine le provider et l'email pour un utilisateur
        
        Logique:
        - Si email présent dans user_info ou Auth -> provider = 'CREDENTIALS'
        - Si pas d'email -> provider = 'google.com'
        """
        result = {
            'email': None,
            'provider': 'google.com',  # Default pour utilisateurs sans email
            'email_verified': False
        }
        
        # Check email in user_info first
        email_from_db = user_info.get('email')
        
        if email_from_db:
            result['email'] = email_from_db
            result['provider'] = 'CREDENTIALS'
            result['email_verified'] = user_info.get('emailVerified', False)
            if self.mode == 'dev':
                print(f"📧 Email found in database for {uid}: {email_from_db}")
        else:
            # Try to get email from Firebase Auth
            try:
                user_auth_record = auth.get_user(uid)
                
                if user_auth_record.email:
                    result['email'] = user_auth_record.email
                    result['provider'] = 'CREDENTIALS'
                    result['email_verified'] = user_auth_record.email_verified
                    if self.mode == 'dev':
                        print(f"📧 Email retrieved from Auth for {uid}: {user_auth_record.email}")
                    
                    # Check if user has Google provider
                    if hasattr(user_auth_record, 'provider_data'):
                        google_providers = [p for p in user_auth_record.provider_data if p.provider_id == 'google.com']
                        if google_providers:
                            result['provider'] = 'google.com'
                            if self.mode == 'dev':
                                print(f"🔍 Google provider detected for {uid}")
                            
                else:
                    # No email found, assume Google provider
                    result['provider'] = 'google.com'
                    if self.mode == 'dev':
                        print(f"❌ No email found for {uid}, setting provider to google.com")
                    
            except Exception as auth_error:
                if self.mode == 'dev':
                    print(f"⚠️  Could not retrieve auth info for {uid}: {auth_error}")
                # Keep default: no email, provider = google.com
        
        return result

    def get_all_users_raw(self) -> pd.DataFrame:
        """
        Fetch all users from Firebase Realtime Database (/Users path)
        Returns raw data as DataFrame for further processing
        
        In dev mode, limits to DEV_USER_LIMIT users
        """
        try:
            print("🔍 Fetching users from Firebase Realtime Database...")
            
            # Get reference to Users node (following your test.py example)
            ref = db.reference('/Users')
            users_data = ref.get()
            
            if not users_data:
                print("❌ No users found in Firebase Realtime Database at /Users path")
                return pd.DataFrame()
            
            total_users_in_db = len(users_data)
            print(f"✓ Found {total_users_in_db} total users in database")
            
            # Apply dev mode limitation
            if self.mode == 'dev' and total_users_in_db > self.dev_user_limit:
                print(f"🔧 Development mode: limiting to first {self.dev_user_limit} users")
                # Get first N users (convert to list to get ordered subset)
                users_items = list(users_data.items())[:self.dev_user_limit]
                users_data = dict(users_items)
                print(f"📊 Processing {len(users_data)} users (limited from {total_users_in_db})")
            else:
                print(f"📊 Processing all {len(users_data)} users")
            
            # Convert to list of dictionaries for DataFrame
            users_list = []
            processed_count = 0
            
            for uid, user_info in users_data.items():
                if isinstance(user_info, dict):
                    # Create a copy of user data
                    user_record = user_info.copy()
                    
                    # Add the UID as 'id' field
                    user_record['id'] = uid
                    user_record['uid'] = uid  # Keep original UID as well
                    
                    # Determine provider and email using the new logic
                    provider_info = self._determine_provider_and_email(uid, user_info)
                    
                    # Update user record with provider information
                    if provider_info['email']:
                        user_record['email'] = provider_info['email']
                    user_record['provider'] = provider_info['provider']
                    user_record['emailVerified'] = provider_info['email_verified']
                    
                    # Add additional metadata
                    user_record['hasEmail'] = provider_info['email'] is not None
                    user_record['authSource'] = 'database' if user_info.get('email') else 'auth' if provider_info['email'] else 'none'
                    
                    users_list.append(user_record)
                    processed_count += 1
                    
                    # Progress indicator for dev mode
                    if self.mode == 'dev' and processed_count % 100 == 0:
                        print(f"📝 Processed {processed_count}/{len(users_data)} users...")
                    
                else:
                    # Handle case where user_info is not a dict
                    if self.mode == 'dev':
                        print(f"⚠️  Skipping user {uid}: data is not a dictionary")
                    continue
            
            if not users_list:
                print("❌ No valid user records found")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(users_list)
            
            # Summary statistics
            total_users = len(df)
            users_with_email = df['hasEmail'].sum()
            users_without_email = total_users - users_with_email
            credentials_users = (df['provider'] == 'CREDENTIALS').sum()
            google_users = (df['provider'] == 'google.com').sum()
            
            print(f"✅ Successfully fetched {total_users} users from Firebase")
            if self.mode == 'dev' and total_users_in_db > self.dev_user_limit:
                print(f"🔧 Limited from {total_users_in_db} total users (dev mode)")
            print(f"📊 Users with email: {users_with_email}")
            print(f"📊 Users without email: {users_without_email}")
            print(f"📊 CREDENTIALS provider: {credentials_users}")
            print(f"📊 Google provider: {google_users}")
            print(f"📝 Available columns: {df.columns.tolist()}")
            
            # Show sample of first user (without sensitive data)
            if len(df) > 0:
                sample_user = df.iloc[0].to_dict()
                # Mask email for display
                if 'email' in sample_user and sample_user['email']:
                    sample_user['email'] = sample_user['email'][:3] + '***'
                print(f"📝 Sample user structure: {list(sample_user.keys())}")
                print(f"📝 Sample user provider: {sample_user.get('provider', 'N/A')}")
            
            return df
            
        except Exception as e:
            print(f"❌ Error fetching users: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def get_user_by_id(self, user_id: str) -> Dict[str, Any]:
        """
        Fetch a specific user by ID from Firebase Realtime Database
        """
        try:
            if self.mode == 'dev':
                print(f"🔍 Fetching user: {user_id}")
            
            user_ref = db.reference(f'/Users/{user_id}')
            user_data = user_ref.get()
            
            if user_data:
                if isinstance(user_data, dict):
                    user_record = user_data.copy()
                    user_record['id'] = user_id
                    user_record['uid'] = user_id
                    
                    # Apply provider logic
                    provider_info = self._determine_provider_and_email(user_id, user_data)
                    if provider_info['email']:
                        user_record['email'] = provider_info['email']
                    user_record['provider'] = provider_info['provider']
                    user_record['emailVerified'] = provider_info['email_verified']
                    
                    return user_record
                else:
                    return {'id': user_id, 'uid': user_id, 'data': user_data, 'provider': 'google.com'}
            else:
                if self.mode == 'dev':
                    print(f"❌ User {user_id} not found")
                return {}
                
        except Exception as e:
            print(f"❌ Error fetching user {user_id}: {e}")
            return {}

    def get_users_by_path(self, path: str, limit: int = None) -> pd.DataFrame:
        """
        Fetch users from a specific path in the database
        
        Args:
            path: Database path to fetch from
            limit: Optional limit for number of users (overrides dev mode limit)
        """
        try:
            print(f"🔍 Fetching data from path: {path}")
            
            ref = db.reference(path)
            data = ref.get()
            
            if not data:
                print(f"❌ No data found at path: {path}")
                return pd.DataFrame()
            
            # Apply limit
            effective_limit = limit if limit is not None else (self.dev_user_limit if self.mode == 'dev' else None)
            
            if effective_limit and len(data) > effective_limit:
                print(f"🔧 Limiting to {effective_limit} records from {len(data)} total")
                data_items = list(data.items())[:effective_limit]
                data = dict(data_items)
            
            users_list = []
            
            if isinstance(data, dict):
                for key, value in data.items():
                    if isinstance(value, dict):
                        record = value.copy()
                        if 'id' not in record:
                            record['id'] = key
                        if 'uid' not in record:
                            record['uid'] = key
                            
                        # Apply provider logic
                        provider_info = self._determine_provider_and_email(key, value)
                        if provider_info['email']:
                            record['email'] = provider_info['email']
                        record['provider'] = provider_info['provider']
                        record['emailVerified'] = provider_info['email_verified']
                        
                        users_list.append(record)
            
            df = pd.DataFrame(users_list)
            print(f"✅ Successfully fetched {len(df)} records from {path}")
            
            return df
            
        except Exception as e:
            print(f"❌ Error fetching data from {path}: {e}")
            return pd.DataFrame()

    def export_raw_data(self, output_filename: str = None) -> str:
        """
        Export raw Firebase data to JSON file (like in your test.py)
        """
        try:
            print("📦 Exporting raw Firebase data...")
            
            ref = db.reference('/Users')
            users_data = ref.get()
            
            if not users_data:
                print("❌ No data to export")
                return ""
            
            # Apply dev mode limitation for export as well
            if self.mode == 'dev' and len(users_data) > self.dev_user_limit:
                print(f"🔧 Development mode: exporting only first {self.dev_user_limit} users")
                users_items = list(users_data.items())[:self.dev_user_limit]
                users_data = dict(users_items)
            
            # Generate filename if not provided
            if not output_filename:
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                mode_suffix = f"_{self.mode}" if self.mode == 'dev' else ""
                output_filename = f"firebase_users_raw{mode_suffix}_{timestamp}.json"
            
            # Save to JSON file
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(users_data, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Raw data exported to: {output_filename}")
            print(f"📊 Exported {len(users_data)} users")
            
            return output_filename
            
        except Exception as e:
            print(f"❌ Error exporting data: {e}")
            return ""

    def debug_database_structure(self):
        """
        Debug method to explore database structure
        """
        try:
            print("🔍 Exploring database structure...")
            print(f"🔧 Current mode: {self.mode.upper()}")
            
            # Check root
            root_ref = db.reference('/')
            root_data = root_ref.get()
            
            if root_data:
                print(f"📋 Root keys: {list(root_data.keys())}")
                
                # Check Users specifically
                if 'Users' in root_data:
                    users_count = len(root_data['Users']) if isinstance(root_data['Users'], dict) else 0
                    print(f"👥 Total users in database: {users_count}")
                    
                    if self.mode == 'dev' and users_count > self.dev_user_limit:
                        print(f"🔧 In dev mode, will process only {self.dev_user_limit} users")
                    
                    if users_count > 0:
                        sample_user_id = list(root_data['Users'].keys())[0]
                        sample_user = root_data['Users'][sample_user_id]
                        print(f"📝 Sample user structure: {list(sample_user.keys()) if isinstance(sample_user, dict) else 'Not a dict'}")
                        
                        # Check email presence
                        has_email = 'email' in sample_user if isinstance(sample_user, dict) else False
                        print(f"📧 Sample user has email: {has_email}")
                else:
                    print("❌ No 'Users' node found")
            else:
                print("❌ Database appears to be empty")
                
        except Exception as e:
            print(f"❌ Error exploring database: {e}")
