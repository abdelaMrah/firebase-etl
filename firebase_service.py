import os
import json
import pandas as pd
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from firebase_admin import auth
from typing import Dict, Any, List
import traceback
import datetime

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

    def get_raw_users(self) -> pd.DataFrame:
        """
        Récupère les données brutes des utilisateurs depuis Firebase Realtime Database (/Users)
        Retourne les données telles qu'elles sont stockées dans la base
        """
        try:
            print("🔍 Fetching raw users from Firebase Realtime Database...")
            
            # Get reference to Users node
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
                users_items = list(users_data.items())[:self.dev_user_limit]
                users_data = dict(users_items)
                print(f"📊 Processing {len(users_data)} users (limited from {total_users_in_db})")
            else:
                print(f"📊 Processing all {len(users_data)} users")
            
            # Convert to list of dictionaries for DataFrame
            users_list = []
            
            for uid, user_info in users_data.items():
                if isinstance(user_info, dict):
                    # Create a copy of user data
                    user_record = user_info.copy()
                    
                    # Add the UID as identifier
                    user_record['uid'] = uid
                    user_record['id'] = uid
                    
                    users_list.append(user_record)
                else:
                    # Skip invalid user records
                    if self.mode == 'dev':
                        print(f"⚠️  Skipping invalid user record for {uid}: {type(user_info)}")
            
            if not users_list:
                print("❌ No valid user records found")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(users_list)
            
            print(f"✅ Successfully fetched {len(df)} raw users from Firebase Realtime Database")
            if self.mode == 'dev' and total_users_in_db > self.dev_user_limit:
                print(f"🔧 Limited from {total_users_in_db} total users (dev mode)")
            
            print(f"📝 Available columns: {df.columns.tolist()}")
            
            return df
            
        except Exception as e:
            print(f"❌ Error fetching raw users: {e}")
            traceback.print_exc()
            return pd.DataFrame()

    def get_auth_users(self, uids: List[str] = None) -> pd.DataFrame:
        """
        Récupère les données d'authentification des utilisateurs depuis Firebase Auth
        
        Args:
            uids: Liste optionnelle d'UIDs spécifiques. Si None, récupère tous les utilisateurs auth
        """
        try:
            print("🔍 Fetching auth users from Firebase Authentication...")
            
            auth_users_list = []
            
            if uids:
                # Récupérer des utilisateurs spécifiques
                print(f"📋 Fetching auth data for {len(uids)} specific users...")
                
                for uid in uids:
                    try:
                        user_record = auth.get_user(uid)
                        auth_user_data = self._extract_auth_user_data(user_record)
                        auth_users_list.append(auth_user_data)
                        
                    except Exception as e:
                        if self.mode == 'dev':
                            print(f"⚠️  Could not fetch auth data for {uid}: {e}")
                        # Ajouter un enregistrement vide pour maintenir la cohérence
                        auth_users_list.append({
                            'uid': uid,
                            'email': None,
                            'email_verified': False,
                            'provider': None,
                            'created_at': None,
                            'last_sign_in': None,
                            'disabled': None,
                            'auth_error': str(e)
                        })
            else:
                # Récupérer tous les utilisateurs auth (pagination automatique)
                print("📋 Fetching all auth users...")
                
                page = auth.list_users()
                processed_count = 0
                
                while True:
                    for user_record in page.users:
                        # Apply dev mode limitation
                        if self.mode == 'dev' and processed_count >= self.dev_user_limit:
                            break
                            
                        auth_user_data = self._extract_auth_user_data(user_record)
                        auth_users_list.append(auth_user_data)
                        processed_count += 1
                    
                    # Check if we should continue
                    if self.mode == 'dev' and processed_count >= self.dev_user_limit:
                        print(f"🔧 Development mode: stopped at {self.dev_user_limit} users")
                        break
                        
                    if not page.has_next_page:
                        break
                        
                    page = page.get_next_page()
            
            if not auth_users_list:
                print("❌ No auth users found")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(auth_users_list)
            
            print(f"✅ Successfully fetched {len(df)} auth users from Firebase Authentication")
            print(f"📝 Available columns: {df.columns.tolist()}")
            
            # Statistiques
            if len(df) > 0:
                users_with_email = df['email'].notna().sum()
                verified_emails = df['email_verified'].sum()
                disabled_users = df['disabled'].sum() if 'disabled' in df.columns else 0
                
                print(f"📊 Users with email: {users_with_email}")
                print(f"📊 Verified emails: {verified_emails}")
                print(f"📊 Disabled users: {disabled_users}")
            
            return df
            
        except Exception as e:
            print(f"❌ Error fetching auth users: {e}")
            traceback.print_exc()
            return pd.DataFrame()

    def _extract_auth_user_data(self, user_record) -> Dict[str, Any]:
        """
        Extrait les données pertinentes d'un enregistrement Firebase Auth
        """
        auth_data = {
            'uid': user_record.uid,
            'email': user_record.email,
            'email_verified': user_record.email_verified,
            'disabled': user_record.disabled,
            'provider': None,
            'created_at': None,
            'last_sign_in': None
        }
        
        # Extraire le provider principal
        if hasattr(user_record, 'provider_data') and user_record.provider_data:
            auth_data['provider'] = user_record.provider_data[0].provider_id
        
        # Extraire les métadonnées temporelles
        if hasattr(user_record, 'user_metadata'):
            if user_record.user_metadata.creation_timestamp:
                creation_timestamp = user_record.user_metadata.creation_timestamp / 1000
                created_date = datetime.datetime.fromtimestamp(creation_timestamp, tz=datetime.timezone.utc)
                auth_data['created_at'] = created_date.isoformat()
            
            if user_record.user_metadata.last_sign_in_timestamp:
                last_sign_in_timestamp = user_record.user_metadata.last_sign_in_timestamp / 1000
                last_sign_in_date = datetime.datetime.fromtimestamp(last_sign_in_timestamp, tz=datetime.timezone.utc)
                auth_data['last_sign_in'] = last_sign_in_date.isoformat()
        
        return auth_data

    def export_raw_data(self, data: pd.DataFrame, filename: str = None) -> str:
        """
        Exporte les données vers un fichier JSON
        
        Args:
            data: DataFrame à exporter
            filename: Nom du fichier (optionnel)
        """
        try:
            if data.empty:
                print("❌ No data to export")
                return ""
            
            # Générer le nom de fichier si non fourni
            if not filename:
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                mode_suffix = f"_{self.mode}" if self.mode == 'dev' else ""
                filename = f"firebase_data{mode_suffix}_{timestamp}.json"
            
            # Convertir DataFrame en dict et sauvegarder
            data_dict = data.to_dict('records')
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data_dict, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Data exported to: {filename}")
            print(f"📊 Exported {len(data)} records")
            
            return filename
            
        except Exception as e:
            print(f"❌ Error exporting data: {e}")
            return ""
