import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, ValidationError
from enum import Enum
import json
import uuid

class UserStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    BANNED = "BANNED"

class UserModel(BaseModel):
    id: str
    email: str
    emailVerified: bool = False
    password: Optional[str] = None
    uid: Optional[str] = None
    provider: str = "CREDENTIALS"
    profilePic: Optional[str] = None
    phoneNumber: Optional[str] = None
    phoneVerified: bool = False
    name: Optional[str] = None
    city: Optional[str] = None
    birthdate: Optional[datetime] = None
    photo: Optional[str] = None
    createdAt: datetime
    updatedAt: datetime
    status: UserStatus = UserStatus.ACTIVE
    interests: Optional[List[str]] = None
    lastConnexion: Optional[datetime] = None

class UserTransformerService:
    """
    Service pour transformer les données brutes de Firebase vers le modèle UserModel
    """
    
    def __init__(self):
        self.transformation_errors = []
        self.successful_transformations = 0
        self.failed_transformations = 0
        self.deduplication_stats = {}
        self.join_stats = {}

    def _reset_counters(self):
        """Reset les compteurs de transformation"""
        self.transformation_errors = []
        self.successful_transformations = 0
        self.failed_transformations = 0
        self.deduplication_stats = {}
        self.join_stats = {}
    
    def _safe_isna(self, value: Any) -> bool:
        """
        Vérifie si une valeur est NaN/None de manière sécurisée
        """
        try:
            return pd.isna(value) or value is None
        except (ValueError, TypeError):
            return value is None
    
    def _clean_nan_values(self, value: Any) -> Any:
        """
        Nettoie les valeurs NaN de pandas et les convertit en None
        """
        if self._safe_isna(value):
            return None
        
        if isinstance(value, float) and np.isnan(value):
            return None
        
        # Gestion spéciale des arrays pandas
        if isinstance(value, (list, np.ndarray)):
            return [self._clean_nan_values(item) for item in value if not self._safe_isna(item)]
        
        return value
    
    def _parse_datetime(self, value: Any) -> Optional[datetime]:
        """
        Parse différents formats de datetime pour PostgreSQL
        """
        if not value or self._safe_isna(value):
            return None
        
        try:
            # Si c'est déjà un datetime
            if isinstance(value, datetime):
                return value
            
            # Si c'est un timestamp Unix
            if isinstance(value, (int, float)):
                # Gérer les timestamps en millisecondes ou secondes
                if value > 1e10:  # Timestamp en millisecondes
                    return datetime.fromtimestamp(value / 1000)
                else:  # Timestamp en secondes
                    return datetime.fromtimestamp(value)
            
            # Si c'est une string
            if isinstance(value, str):
                # Format ISO
                if 'T' in value or '+' in value or 'Z' in value:
                    return datetime.fromisoformat(value.replace('Z', '+00:00'))
                
                # Essayer différents formats
                formats = [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d',
                    '%d/%m/%Y',
                    '%m/%d/%Y',
                    '%Y-%m-%dT%H:%M:%S.%f'
                ]
                
                for fmt in formats:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
            
            return None
            
        except Exception:
            return None
    
    def _parse_interests(self, value: Any) -> Optional[List[str]]:
        """
        Parse interests pour PostgreSQL (array format)
        """
        if not value or self._safe_isna(value):
            return None
        
        try:
            # Si c'est déjà une liste
            if isinstance(value, list):
                return [str(item) for item in value if item and not self._safe_isna(item)]
            
            # Si c'est une string JSON
            if isinstance(value, str):
                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, list):
                        return [str(item) for item in parsed if item]
                except:
                    # Si ce n'est pas du JSON, traiter comme string délimitée
                    if ',' in value:
                        return [item.strip() for item in value.split(',') if item.strip()]
                    elif value.strip():
                        return [value.strip()]
            
            return None
            
        except Exception:
            return None
    
    def _normalize_status(self, value: Any) -> UserStatus:
        """
        Normalise le status pour PostgreSQL enum
        """
        if not value or self._safe_isna(value):
            return UserStatus.ACTIVE
        
        try:
            status_str = str(value).upper().strip()
            
            # Mapping des valeurs courantes
            status_mapping = {
                'ACTIVE': UserStatus.ACTIVE,
                'INACTIVE': UserStatus.INACTIVE,
                'BANNED': UserStatus.BANNED,
                'DISABLED': UserStatus.INACTIVE,
                'SUSPENDED': UserStatus.BANNED,
                'BLOCKED': UserStatus.BANNED,
                '1': UserStatus.ACTIVE,
                '0': UserStatus.INACTIVE,
                'TRUE': UserStatus.ACTIVE,
                'FALSE': UserStatus.INACTIVE
            }
            
            return status_mapping.get(status_str, UserStatus.ACTIVE)
            
        except Exception:
            return UserStatus.ACTIVE
    
    def _clean_string_field(self, value: Any) -> Optional[str]:
        """
        Nettoie les champs string pour PostgreSQL
        """
        if not value or self._safe_isna(value):
            return None
        
        try:
            cleaned = str(value).strip()
            return cleaned if cleaned else None
        except:
            return None

    def enrich_user_data_with_auth(self, raw_user: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrichit les données d'un utilisateur avec les informations d'authentification
        Optimisé pour PostgreSQL
        """
        enriched_user = raw_user.copy()
        
        # Email: priorité aux données auth
        if 'auth_email' in raw_user and raw_user['auth_email']:
            enriched_user['email'] = raw_user['auth_email']
            enriched_user['email_source'] = 'auth'
        elif 'email' in raw_user and raw_user['email']:
            enriched_user['email_source'] = 'raw'
        else:
            enriched_user['email_source'] = 'none'
        
        # Email verified: priorité aux données auth
        if 'auth_email_verified' in raw_user:
            enriched_user['emailVerified'] = bool(raw_user['auth_email_verified'])
            enriched_user['email_verified_source'] = 'auth'
        elif 'emailVerified' in raw_user:
            enriched_user['email_verified_source'] = 'raw'
        else:
            enriched_user['emailVerified'] = False
            enriched_user['email_verified_source'] = 'default'
        
        # Provider: priorité aux données auth avec conversion password -> CREDENTIALS
        if 'auth_provider' in raw_user and raw_user['auth_provider']:
            provider = raw_user['auth_provider']
            # Convertir 'password' en 'CREDENTIALS'
            if provider == 'password':
                enriched_user['provider'] = 'CREDENTIALS'
            else:
                enriched_user['provider'] = provider
            enriched_user['provider_source'] = 'auth'
        elif 'provider' in raw_user and raw_user['provider']:
            provider = raw_user['provider']
            # Convertir 'password' en 'CREDENTIALS'
            if provider == 'password':
                enriched_user['provider'] = 'CREDENTIALS'
            else:
                enriched_user['provider'] = provider
            enriched_user['provider_source'] = 'raw'
        else:
            enriched_user['provider'] = 'CREDENTIALS'
            enriched_user['provider_source'] = 'default'
        
        # Dates: utiliser auth si disponible
        if 'auth_created_at' in raw_user and raw_user['auth_created_at']:
            enriched_user['createdAt'] = raw_user['auth_created_at']
            enriched_user['created_at_source'] = 'auth'
        elif 'createdAt' in raw_user:
            enriched_user['created_at_source'] = 'raw'
        else:
            enriched_user['created_at_source'] = 'default'
        
        # Last sign in depuis auth
        if 'auth_last_sign_in' in raw_user and raw_user['auth_last_sign_in']:
            enriched_user['lastConnexion'] = raw_user['auth_last_sign_in']
            enriched_user['last_connexion_source'] = 'auth'
        elif 'lastConnexion' in raw_user:
            enriched_user['last_connexion_source'] = 'raw'
        else:
            enriched_user['last_connexion_source'] = 'none'
        
        # Statut disabled depuis auth
        if 'auth_disabled' in raw_user:
            if raw_user['auth_disabled']:
                enriched_user['status'] = 'INACTIVE'
                enriched_user['status_source'] = 'auth_disabled'
            elif 'status' in raw_user:
                enriched_user['status_source'] = 'raw'
            else:
                enriched_user['status'] = 'ACTIVE'
                enriched_user['status_source'] = 'default'
        
        # Marquer si l'utilisateur a des données auth
        enriched_user['has_auth_data'] = bool('auth_email' in raw_user and raw_user['auth_email'])
        
        return enriched_user

    def _prepare_for_postgres(self, user_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prépare un dictionnaire utilisateur pour l'insertion PostgreSQL
        """
        postgres_user = {}
        
        # Champs requis avec nettoyage
        postgres_user['id'] = self._clean_string_field(user_dict.get('id')) or str(uuid.uuid4())[:20]
        postgres_user['email'] = self._clean_string_field(user_dict.get('email', ''))
        
        # Champs booléens
        postgres_user['emailVerified'] = bool(user_dict.get('emailVerified', False))
        postgres_user['phoneVerified'] = bool(user_dict.get('phoneVerified', False))
        
        # Champs string optionnels
        string_fields = ['password', 'uid', 'provider', 'profilePic', 'phoneNumber', 'name', 'city', 'photo']
        for field in string_fields:
            postgres_user[field] = self._clean_string_field(user_dict.get(field))
        
        # Provider: nettoyer et convertir 'password' en 'CREDENTIALS'
        provider = postgres_user.get('provider')
        if provider == 'password':
            postgres_user['provider'] = 'CREDENTIALS'
            print(f"🔧 Converted provider 'password' to 'CREDENTIALS' for user {postgres_user.get('id', 'unknown')}")
        elif not provider:
            postgres_user['provider'] = 'CREDENTIALS'
        
        # Champs datetime avec gestion des erreurs PostgreSQL
        postgres_user['createdAt'] = self._parse_datetime(user_dict.get('createdAt')) or datetime.now()
        postgres_user['updatedAt'] = self._parse_datetime(user_dict.get('updatedAt')) or datetime.now()
        postgres_user['birthdate'] = self._parse_datetime(user_dict.get('birthdate'))
        postgres_user['lastConnexion'] = self._parse_datetime(user_dict.get('lastConnexion'))
        
        # Status enum
        postgres_user['status'] = self._normalize_status(user_dict.get('status'))
        
        # Interests array pour PostgreSQL
        postgres_user['interests'] = self._parse_interests(user_dict.get('interests'))
        
        # Validation finale - s'assurer que les champs requis sont présents
        if not postgres_user['email']:
            raise ValueError(f"Email is required for user {postgres_user['id']}")
        
        return postgres_user

    def detect_and_remove_duplicates(self, df: pd.DataFrame, 
                                   duplicate_column: str = 'email', 
                                   sort_column: str = 'createdAt',
                                   keep: str = 'last') -> pd.DataFrame:
        """
        Détecte et supprime les doublons avec statistiques
        """
        initial_count = len(df)
        
        if duplicate_column not in df.columns:
            print(f"⚠️  Column '{duplicate_column}' not found for duplicate detection")
            return df
        
        # Identifier les doublons
        duplicates_mask = df.duplicated(subset=[duplicate_column], keep=False)
        duplicates_found = duplicates_mask.sum()
        
        if duplicates_found == 0:
            print("✅ No duplicates found")
            self.deduplication_stats = {
                'initial_count': initial_count,
                'duplicates_found': 0,
                'removed_count': 0,
                'final_count': initial_count
            }
            return df
        
        print(f"🔍 Found {duplicates_found} duplicate entries based on '{duplicate_column}'")
        
        # Trier pour garder le bon enregistrement
        if sort_column in df.columns:
            df_sorted = df.sort_values(sort_column, ascending=True)
        else:
            df_sorted = df
        
        # Supprimer les doublons
        df_cleaned = df_sorted.drop_duplicates(subset=[duplicate_column], keep=keep)
        
        removed_count = initial_count - len(df_cleaned)
        
        print(f"🧹 Removed {removed_count} duplicate entries, keeping '{keep}' occurrence")
        
        # Statistiques de déduplication
        self.deduplication_stats = {
            'initial_count': initial_count,
            'duplicates_found': duplicates_found,
            'removed_count': removed_count,
            'final_count': len(df_cleaned),
            'duplicate_column': duplicate_column,
            'keep_strategy': keep
        }
        
        return df_cleaned

    def transform_single_user(self, raw_user: Dict[str, Any]) -> Optional[UserModel]:
        """
        Transforme un utilisateur brut en UserModel
        """
        try:
            # Nettoyer les valeurs NaN
            cleaned_user = {k: self._clean_nan_values(v) for k, v in raw_user.items()}
            
            # Préparer pour PostgreSQL
            postgres_ready_user = self._prepare_for_postgres(cleaned_user)
            
            # Créer le UserModel
            user_model = UserModel(**postgres_ready_user)
            
            self.successful_transformations += 1
            return user_model
            
        except Exception as e:
            self.failed_transformations += 1
            error_info = {
                'user_id': raw_user.get('id', raw_user.get('uid', 'unknown')),
                'error': str(e),
                'user_data': raw_user
            }
            self.transformation_errors.append(error_info)
            return None

    def join_users_and_auth_data(self, raw_users_df: pd.DataFrame, auth_users_df: pd.DataFrame) -> pd.DataFrame:
        """
        Joint les données des utilisateurs (raw_users) avec les données d'authentification (auth_users)
        basé sur l'UID
        """
        print(f"🔗 Starting join between raw users ({len(raw_users_df)}) and auth users ({len(auth_users_df)})...")
        
        if raw_users_df.empty:
            print("❌ Raw users DataFrame is empty")
            return pd.DataFrame()
        
        if auth_users_df.empty:
            print("⚠️  Auth users DataFrame is empty, proceeding with raw users only")
            self.join_stats = {
                'raw_users_count': len(raw_users_df),
                'auth_users_count': 0,
                'matched_users': 0,
                'unmatched_raw_users': len(raw_users_df),
                'unmatched_auth_users': 0,
                'final_count': len(raw_users_df)
            }
            return raw_users_df
        
        # Nettoyer les UIDs pour la jointure
        raw_users_clean = raw_users_df.copy()
        auth_users_clean = auth_users_df.copy()
        
        # S'assurer que les colonnes UID existent et sont propres
        if 'uid' not in raw_users_clean.columns:
            print("⚠️  No 'uid' column in raw users, trying 'id' column...")
            if 'id' in raw_users_clean.columns:
                raw_users_clean['uid'] = raw_users_clean['id']
            else:
                print("❌ No UID column found in raw users")
                return raw_users_df
        
        if 'uid' not in auth_users_clean.columns:
            print("❌ No 'uid' column in auth users")
            return raw_users_df
        
        # Nettoyer les UIDs
        raw_users_clean['uid'] = raw_users_clean['uid'].apply(self._clean_string_field)
        auth_users_clean['uid'] = auth_users_clean['uid'].apply(self._clean_string_field)
        
        # Supprimer les lignes avec UID null
        raw_users_clean = raw_users_clean.dropna(subset=['uid'])
        auth_users_clean = auth_users_clean.dropna(subset=['uid'])
        
        print(f"📊 After cleaning UIDs: Raw users: {len(raw_users_clean)}, Auth users: {len(auth_users_clean)}")
        
        # Préfixer les colonnes auth pour éviter les conflits (sauf uid)
        auth_columns_to_rename = {col: f"auth_{col}" for col in auth_users_clean.columns if col != 'uid'}
        auth_users_renamed = auth_users_clean.rename(columns=auth_columns_to_rename)
        
        print(f"📝 Auth columns renamed: {list(auth_columns_to_rename.values())}")
        
        # Effectuer la jointure LEFT JOIN (garder tous les raw users)
        joined_df = raw_users_clean.merge(
            auth_users_renamed,
            on='uid',
            how='left',
            suffixes=('', '_auth_duplicate')
        )
        
        # Statistiques de jointure
        matched_users = joined_df['auth_email'].notna().sum()
        unmatched_raw_users = len(joined_df) - matched_users
        unmatched_auth_users = len(auth_users_clean) - matched_users
        
        self.join_stats = {
            'raw_users_count': len(raw_users_df),
            'auth_users_count': len(auth_users_df),
            'raw_users_with_uid': len(raw_users_clean),
            'auth_users_with_uid': len(auth_users_clean),
            'matched_users': matched_users,
            'unmatched_raw_users': unmatched_raw_users,
            'unmatched_auth_users': unmatched_auth_users,
            'final_count': len(joined_df),
            'join_type': 'LEFT JOIN on uid'
        }
        
        print(f"✅ Join completed successfully!")
        print(f"📊 Join Statistics:")
        print(f"   - Raw users processed: {len(raw_users_clean)}")
        print(f"   - Auth users processed: {len(auth_users_clean)}")
        print(f"   - Matched users: {matched_users}")
        print(f"   - Raw users without auth data: {unmatched_raw_users}")
        print(f"   - Auth users without raw data: {unmatched_auth_users}")
        print(f"   - Final dataset size: {len(joined_df)}")
        
        return joined_df
    
    def transform_users_dataframe(self, raw_users_df: pd.DataFrame, auth_users_df: pd.DataFrame = None, remove_duplicates: bool = True) -> pd.DataFrame:
        """
        Transforme un DataFrame d'utilisateurs bruts en DataFrame prêt pour PostgreSQL
        """
        self._reset_counters()
        
        print(f"🔄 Starting transformation of {len(raw_users_df)} users for PostgreSQL...")
        
        # Step 1: Jointure avec les données auth si disponibles
        if auth_users_df is not None and not auth_users_df.empty:
            print("\n=== Joining raw users with auth data ===")
            df_joined = self.join_users_and_auth_data(raw_users_df, auth_users_df)
        else:
            print("⚠️  No auth data provided, proceeding with raw users only")
            df_joined = raw_users_df.copy()
            self.join_stats = {
                'raw_users_count': len(raw_users_df),
                'auth_users_count': 0,
                'matched_users': 0,
                'final_count': len(raw_users_df)
            }
        
        # Step 2: Clean DataFrame - replace NaN with None
        print("\n🧹 Cleaning NaN values...")
        df_cleaned = df_joined.copy()
        
        for col in df_cleaned.columns:
            try:
                df_cleaned[col] = df_cleaned[col].apply(lambda x: self._clean_nan_values(x))
            except Exception as e:
                print(f"⚠️  Warning: Could not clean column {col}: {e}")
                df_cleaned[col] = df_cleaned[col].where(pd.notna(df_cleaned[col]), None)
        
        # Step 3: Remove duplicates if requested
        if remove_duplicates:
            print("\n=== Detecting and removing duplicates ===")
            df_cleaned = self.detect_and_remove_duplicates(df_cleaned)
        
        # Step 4: Transform users pour PostgreSQL
        print(f"\n=== Transforming users for PostgreSQL ===")
        transformed_users = []
        
        for idx, row in df_cleaned.iterrows():
            raw_user = row.to_dict()
            
            # Enrichir avec les données auth si disponibles
            if auth_users_df is not None:
                enriched_user = self.enrich_user_data_with_auth(raw_user)
            else:
                enriched_user = raw_user
            
            user_model = self.transform_single_user(enriched_user)
            
            if user_model:
                # Convertir en dict - SANS les métadonnées pour PostgreSQL
                user_dict = user_model.dict()
                
                # Ne PAS ajouter les métadonnées dans le dict final
                # Les statistiques sont gardées dans transformation_report
                
                # Validation finale pour PostgreSQL
                if user_dict.get('email'):  # S'assurer qu'on a un email valide
                    transformed_users.append(user_dict)
                else:
                    print(f"⚠️  Skipping user {user_dict.get('id', 'unknown')} - no valid email")
            
            # Progress indicator
            if (idx + 1) % 100 == 0 or (idx + 1) == len(df_cleaned):
                print(f"📝 Processed {idx + 1}/{len(df_cleaned)} users... (Success: {self.successful_transformations}, Failed: {self.failed_transformations})")
        
        result_df = pd.DataFrame(transformed_users)
        
        if not result_df.empty:
            # Final cleaning pour PostgreSQL - supprime les métadonnées
            print("\n🔧 Final PostgreSQL preparation...")
            result_df = self._finalize_dataframe_for_postgres(result_df)
        
        print(f"✅ Transformation completed: {len(result_df)} users ready for PostgreSQL")
        
        return result_df

    def _finalize_dataframe_for_postgres(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Finalise le DataFrame pour l'insertion PostgreSQL
        Supprime les colonnes de métadonnées qui ne sont pas dans le schéma de la table
        """
        df_final = df.copy()
        
        # Colonnes autorisées dans la table User PostgreSQL
        allowed_columns = [
            'id', 'email', 'emailVerified', 'password', 'uid', 'provider', 
            'profilePic', 'phoneNumber', 'phoneVerified', 'name', 'city', 
            'birthdate', 'photo', 'createdAt', 'updatedAt', 'status', 
            'interests', 'lastConnexion'
        ]
        
        # Colonnes de métadonnées à supprimer avant insertion PostgreSQL
        metadata_columns = [
            'email_source', 'has_auth_data', 'email_verified_source', 
            'provider_source', 'created_at_source', 'last_connexion_source', 
            'status_source'
        ]
        
        # Supprimer les colonnes de métadonnées
        columns_to_drop = [col for col in metadata_columns if col in df_final.columns]
        if columns_to_drop:
            print(f"🧹 Removing metadata columns: {columns_to_drop}")
            df_final = df_final.drop(columns=columns_to_drop)
        
        # Garder seulement les colonnes autorisées
        final_columns = [col for col in df_final.columns if col in allowed_columns]
        df_final = df_final[final_columns]
        
        print(f"✅ Final columns for PostgreSQL: {df_final.columns.tolist()}")
        
        # Convertir les enums en strings
        if 'status' in df_final.columns:
            df_final['status'] = df_final['status'].apply(lambda x: x.value if hasattr(x, 'value') else str(x))
        
        # Gérer les arrays pour PostgreSQL
        if 'interests' in df_final.columns:
            df_final['interests'] = df_final['interests'].apply(self._format_interests_for_postgres)
        
        # S'assurer que les datetime sont dans le bon format
        datetime_columns = ['createdAt', 'updatedAt', 'birthdate', 'lastConnexion']
        for col in datetime_columns:
            if col in df_final.columns:
                df_final[col] = pd.to_datetime(df_final[col], errors='coerce')
        
        # Validation finale - supprimer les lignes sans email valide
        if 'email' in df_final.columns:
            initial_count = len(df_final)
            df_final = df_final[df_final['email'].notna() & (df_final['email'] != '')]
            removed_count = initial_count - len(df_final)
            if removed_count > 0:
                print(f"⚠️  Removed {removed_count} users without valid email")
        
        return df_final

    def _format_interests_for_postgres(self, interests: Any) -> Optional[str]:
        """
        Formate les interests pour PostgreSQL array
        """
        if not interests or self._safe_isna(interests):
            return None
        
        try:
            if isinstance(interests, list):
                # Convertir en format PostgreSQL array
                clean_interests = [str(item).strip() for item in interests if item and not self._safe_isna(item)]
                if clean_interests:
                    return '{' + ','.join(f'"{item}"' for item in clean_interests) + '}'
            return None
        except:
            return None

    def get_join_stats(self) -> Dict[str, Any]:
        """Retourne les statistiques de jointure"""
        return self.join_stats

    def get_transformation_report(self) -> Dict[str, Any]:
        """
        Retourne un rapport détaillé de la transformation AVEC métadonnées
        """
        total = self.successful_transformations + self.failed_transformations
        success_rate = (self.successful_transformations / total * 100) if total > 0 else 0
        
        # Calculer les statistiques de source depuis les données transformées
        source_stats = {}
        if hasattr(self, 'last_enriched_users'):
            email_sources = {}
            auth_data_count = 0
            for user in self.last_enriched_users:
                source = user.get('email_source', 'unknown')
                email_sources[source] = email_sources.get(source, 0) + 1
                if user.get('has_auth_data', False):
                    auth_data_count += 1
            
            source_stats = {
                'email_sources': email_sources,
                'users_with_auth_data': auth_data_count,
                'total_users': len(self.last_enriched_users)
            }
        print(f"source_stats={source_stats}")
        return {
            'successful_transformations': self.successful_transformations,
            'failed_transformations': self.failed_transformations,
            'success_rate': success_rate,
            'errors': self.transformation_errors,
            'deduplication_stats': self.deduplication_stats,
            'join_stats': self.join_stats,
            'source_stats': source_stats
        }

    def export_transformed_users(self, users_df: pd.DataFrame, filename: str = 'transformed_users.csv') -> bool:
        """Exporte les utilisateurs transformés"""
        try:
            users_df.to_csv(filename, index=False)
            print(f"✅ Exported {len(users_df)} users to {filename}")
            return True
        except Exception as e:
            print(f"❌ Error exporting users: {e}")
            return False
    
    def validate_required_fields(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Valide les champs requis pour PostgreSQL"""
        required_fields = ['id', 'email']
        
        missing_fields = [field for field in required_fields if field not in df.columns]
        null_values = {}
        
        for field in required_fields:
            if field in df.columns:
                null_count = df[field].isnull().sum()
                if null_count > 0:
                    null_values[field] = null_count
        
        is_valid = len(missing_fields) == 0 and len(null_values) == 0
        
        return {
            'is_valid': is_valid,
            'missing_required_fields': missing_fields,
            'null_values_in_required_fields': null_values,
            'total_rows': len(df)
        }
    
    def get_deduplication_stats(self) -> Dict[str, Any]:
        """Retourne les statistiques de déduplication"""
        return self.deduplication_stats