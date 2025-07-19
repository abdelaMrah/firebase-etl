import os
import pandas as pd
from google.cloud import datastore
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

class DatastoreUserService:
    def __init__(self, project_id: Optional[str] = None):
        """
        Initialise le service Datastore
        """
        self.project_id = project_id or os.getenv('FIREBASE_PROJECT_ID')
        self.client = None
        self.logger = logging.getLogger(__name__)
        
        if not self.project_id:
            raise ValueError("Project ID must be provided or set in FIREBASE_PROJECT_ID environment variable")
    
    def connect(self) -> bool:
        """
        Établit la connexion au Datastore
        """
        try:
            # Initialiser le client Datastore
            self.client = datastore.Client(project=self.project_id)
            
            # Test de connexion simple
            query = self.client.query()
            query.keys_only()
            list(query.fetch(limit=1))
            
            self.logger.info(f"✅ Connected to Datastore project: {self.project_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to Datastore: {e}")
            return False
    
    def list_kinds(self) -> List[str]:
        """
        Liste tous les kinds disponibles dans Datastore
        """
        try:
            if not self.client:
                if not self.connect():
                    return []
            
            # Note: Datastore ne fournit pas d'API directe pour lister les kinds
            # Cette méthode essaie quelques noms communs
            common_kinds = [
                'User', 'Users', 'user', 'users', 
                'Person', 'Account', 'Profile', 'Member',
                'UserProfile', 'Customer', 'Client'
            ]
            found_kinds = []
            
            print("🔍 Searching for available kinds...")
            for kind in common_kinds:
                try:
                    query = self.client.query(kind=kind)
                    query.keys_only()
                    entities = list(query.fetch(limit=1))
                    if entities:
                        found_kinds.append(kind)
                        print(f"✅ Found kind: '{kind}' with data")
                except Exception as e:
                    print(f"❌ Kind '{kind}' not found or error: {e}")
                    continue
            
            return found_kinds
            
        except Exception as e:
            self.logger.error(f"❌ Error listing kinds: {e}")
            return []
    
    def get_sample_entity(self, kind_name: str, limit: int = 1) -> Dict[str, Any]:
        """
        Récupère un échantillon d'entité pour analyser la structure
        """
        try:
            if not self.client:
                if not self.connect():
                    return {}
            
            query = self.client.query(kind=kind_name)
            entities = list(query.fetch(limit=limit))
            
            if entities:
                sample = dict(entities[0])
                if hasattr(entities[0], 'key') and entities[0].key:
                    sample['_key_info'] = {
                        'kind': entities[0].key.kind,
                        'id': entities[0].key.id,
                        'name': entities[0].key.name
                    }
                return sample
            else:
                return {}
                
        except Exception as e:
            self.logger.error(f"❌ Error getting sample entity: {e}")
            return {}
    
    def get_all_users_raw(self, kind_name: str = 'User') -> pd.DataFrame:
        """
        Récupère tous les utilisateurs depuis Datastore
        """
        try:
            if not self.client:
                if not self.connect():
                    return pd.DataFrame()
            
            print(f"🔍 Fetching all entities of kind: '{kind_name}'")
            
            # Créer une requête pour récupérer tous les utilisateurs
            query = self.client.query(kind=kind_name)
            
            users_data = []
            
            # Récupérer toutes les entités avec pagination pour éviter les timeouts
            page_size = 1000
            cursor = None
            total_fetched = 0
            
            while True:
                query_iter = query.fetch(limit=page_size, start_cursor=cursor)
                page = next(query_iter.pages)
                entities = list(page)
                
                if not entities:
                    break
                
                total_fetched += len(entities)
                print(f"📥 Fetched {total_fetched} entities so far...")
                
                for entity in entities:
                    # Convertir l'entité Datastore en dictionnaire
                    user_data = dict(entity)
                    
                    # Ajouter l'ID de l'entité
                    if hasattr(entity, 'key') and entity.key:
                        if entity.key.name:
                            user_data['id'] = entity.key.name
                        elif entity.key.id:
                            user_data['id'] = str(entity.key.id)
                        else:
                            user_data['id'] = f"auto_{len(users_data)}"
                    else:
                        user_data['id'] = f"unknown_{len(users_data)}"
                    
                    # Convertir les dates Datastore en timestamps
                    for key, value in user_data.items():
                        if isinstance(value, datetime):
                            user_data[key] = value.isoformat()
                    
                    users_data.append(user_data)
                
                # Obtenir le curseur pour la page suivante
                cursor = query_iter.next_page_token
                if not cursor:
                    break
            
            print(f"📥 Total entities retrieved: {len(users_data)}")
            
            if users_data:
                df = pd.DataFrame(users_data)
                print(f"✅ Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
                print(f"Columns: {df.columns.tolist()}")
                return df
            else:
                print("❌ No users found in Datastore")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"❌ Error fetching users from Datastore: {e}")
            print(f"❌ Error details: {e}")
            return pd.DataFrame()
    
    def count_entities(self, kind_name: str) -> int:
        """
        Compte le nombre d'entités d'un kind donné
        """
        try:
            if not self.client:
                if not self.connect():
                    return 0
            
            # Utiliser une requête keys_only pour compter
            query = self.client.query(kind=kind_name)
            query.keys_only()
            
            # Compter en parcourant (pas d'API count directe)
            count = 0
            for _ in query.fetch():
                count += 1
            
            return count
            
        except Exception as e:
            self.logger.error(f"❌ Error counting entities: {e}")
            return 0
    
    def explore_datastore(self) -> Dict[str, Any]:
        """
        Explore le Datastore pour trouver les données utilisateur
        """
        try:
            print("🔍 Exploring Datastore structure...")
            
            # Essayer de lister quelques kinds communs avec des variations
            exploration_kinds = [
                'User', 'Users', 'user', 'users',
                'Person', 'People', 'person', 'people',
                'Account', 'Accounts', 'account', 'accounts',
                'Profile', 'Profiles', 'profile', 'profiles',
                'Member', 'Members', 'member', 'members',
                'Customer', 'Customers', 'customer', 'customers',
                'Client', 'Clients', 'client', 'clients',
                'UserProfile', 'UserProfiles',
                'UserAccount', 'UserAccounts'
            ]
            
            found_data = {}
            
            for kind in exploration_kinds:
                try:
                    count = self.count_entities(kind)
                    if count > 0:
                        sample = self.get_sample_entity(kind)
                        found_data[kind] = {
                            'count': count,
                            'sample_fields': list(sample.keys()) if sample else []
                        }
                        print(f"✅ Found '{kind}': {count} entities")
                        if sample:
                            print(f"   Sample fields: {list(sample.keys())[:10]}")
                except Exception as e:
                    continue
            
            return found_data
            
        except Exception as e:
            self.logger.error(f"❌ Error exploring Datastore: {e}")
            return {}