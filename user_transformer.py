import pandas as pd
from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, ValidationError
from enum import Enum
import numpy as np
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
    
    def _safe_isna(self, value: Any) -> bool:
        """
        Vérifie si une valeur est NaN/None de manière sécurisée
        """
        try:
            if value is None:
                return True
            if isinstance(value, (list, np.ndarray)):
                # Pour les arrays, vérifier si tous les éléments sont NaN
                try:
                    return bool(pd.isna(value).all())
                except:
                    return False
            return bool(pd.isna(value))
        except (ValueError, TypeError):
            # Si pd.isna() échoue, considérer comme non-NaN
            return False
    
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
            try:
                # Si c'est un array avec tous des NaN
                if pd.isna(value).all():
                    return None
                # Si c'est un array mixte, nettoyer les éléments
                return [self._clean_nan_values(item) for item in value if not self._safe_isna(item)]
            except:
                return None
        return value
    
    def _parse_datetime(self, value: Any) -> Optional[datetime]:
        """
        Parse différents formats de datetime avec gestion robuste des NaT
        """
        value = self._clean_nan_values(value)
        if value is None:
            return None
        
        # Vérification spéciale pour NaT de pandas
        if pd.isna(value):
            return None
            
        # Si c'est déjà un NaT, retourner None
        if hasattr(value, '_value') and pd.isna(value):
            return None
            
        if isinstance(value, datetime):
            return value
        
        if isinstance(value, str):
            value_str = str(value).strip().lower()
            if value_str in ['nat', 'none', 'null', '', 'nan']:
                return None
                
            try:
                # Try different datetime formats
                formats = [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%dT%H:%M:%S',
                    '%Y-%m-%dT%H:%M:%S.%f',
                    '%Y-%m-%dT%H:%M:%SZ',
                    '%Y-%m-%d'
                ]
                for fmt in formats:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
                
                # If no format matches, try pandas to_datetime
                parsed = pd.to_datetime(value, errors='coerce')
                if pd.isna(parsed):
                    return None
                return parsed.to_pydatetime()
            except:
                return None
        
        # Handle Firebase Timestamp objects
        if hasattr(value, 'seconds'):
            try:
                return datetime.fromtimestamp(value.seconds)
            except:
                return None
        
        # Handle Unix timestamps
        if isinstance(value, (int, float)) and not np.isnan(value) and value > 0:
            try:
                # Check if it's in milliseconds or seconds
                if value > 1e10:  # milliseconds
                    return datetime.fromtimestamp(value / 1000)
                else:  # seconds
                    return datetime.fromtimestamp(value)
            except:
                return None
        
        return None
    
    def _parse_interests(self, value: Any) -> Optional[List[str]]:
        """
        Parse les intérêts depuis différents formats
        """
        value = self._clean_nan_values(value)
        if value is None:
            return None
        
        if isinstance(value, list):
            # Clean any NaN values from the list
            cleaned_list = []
            for item in value:
                if not self._safe_isna(item):
                    cleaned_list.append(str(item))
            return cleaned_list if cleaned_list else None
        
        if isinstance(value, str):
            value = value.strip()
            if not value or value.lower() in ['nan', 'none', 'null']:
                return None
            # Try to parse comma-separated values
            if ',' in value:
                items = [item.strip() for item in value.split(',') if item.strip()]
                return items if items else None
            else:
                return [value] if value else None
        
        return None
    
    def _normalize_status(self, value: Any) -> UserStatus:
        """
        Normalise le statut utilisateur
        """
        value = self._clean_nan_values(value)
        if value is None:
            return UserStatus.ACTIVE
        
        status_str = str(value).upper().strip()
        
        # Map different status variations
        status_mapping = {
            'ACTIVE': UserStatus.ACTIVE,
            'ACTIF': UserStatus.ACTIVE,
            'ENABLED': UserStatus.ACTIVE,
            'INACTIVE': UserStatus.INACTIVE,
            'INACTIF': UserStatus.INACTIVE,
            'DISABLED': UserStatus.INACTIVE,
            'BANNED': UserStatus.BANNED,
            'BANNI': UserStatus.BANNED,
            'BLOCKED': UserStatus.BANNED
        }
        
        return status_mapping.get(status_str, UserStatus.ACTIVE)
    
    def _clean_string_field(self, value: Any) -> Optional[str]:
        """
        Nettoie les champs string en gérant les valeurs NaN et les arrays
        """
        value = self._clean_nan_values(value)
        if value is None:
            return None
        
        # Si c'est un array/list, prendre le premier élément non-null
        if isinstance(value, (list, np.ndarray)):
            try:
                for item in value:
                    if not self._safe_isna(item):
                        value = item
                        break
                else:
                    return None
            except:
                return None
        
        # Convert to string and strip whitespace
        try:
            str_value = str(value).strip()
        except:
            return None
        
        # Return None for empty strings or common null representations
        if not str_value or str_value.lower() in ['nan', 'null', 'none', '', 'nat']:
            return None
        
        return str_value
    
    def detect_and_remove_duplicates(self, df: pd.DataFrame, 
                                   duplicate_column: str = 'email', 
                                   sort_column: str = 'createdAt',
                                   keep: str = 'last') -> pd.DataFrame:
        """
        Détecte et supprime les doublons dans le DataFrame
        """
        initial_count = len(df)
        
        # Clean the duplicate column first
        if duplicate_column in df.columns:
            df[duplicate_column] = df[duplicate_column].apply(self._clean_string_field)
            # Remove rows where the duplicate column is None
            df = df.dropna(subset=[duplicate_column])
        
        # Detect duplicates
        duplicates = df[df.duplicated([duplicate_column], keep=False)]
        
        if not duplicates.empty:
            print(f"⚠️  Found {len(duplicates)} duplicate {duplicate_column} values:")
            
            # Store duplication stats
            duplicate_stats = {}
            for value in duplicates[duplicate_column].unique():
                dupes = duplicates[duplicates[duplicate_column] == value]
                duplicate_count = len(dupes)
                duplicate_stats[value] = {
                    'count': duplicate_count,
                    'ids': dupes['id'].tolist() if 'id' in dupes.columns else []
                }
                print(f"  - {value}: {duplicate_count} records")
                if 'id' in dupes.columns:
                    print(f"    IDs: {dupes['id'].tolist()}")
            
            self.deduplication_stats = {
                'duplicates_found': len(duplicates),
                'unique_duplicate_values': len(duplicate_stats),
                'duplicate_details': duplicate_stats
            }
            
            if keep != 'all':
                print(f"🧹 Removing duplicates, keeping {keep} record per {duplicate_column}...")
                
                # Try to sort by the specified column
                if sort_column in df.columns:
                    print(f"Sorting by '{sort_column}' column...")
                    df_copy = df.copy()
                    df_copy['_sort_parsed'] = df_copy[sort_column].apply(self._parse_datetime)
                    
                    valid_dates = df_copy['_sort_parsed'].notna().sum()
                    print(f"Successfully parsed {valid_dates} out of {len(df_copy)} dates")
                    
                    df_copy = df_copy.sort_values('_sort_parsed', na_position='first')
                    df_deduplicated = df_copy.drop_duplicates([duplicate_column], keep=keep)
                    df_deduplicated = df_deduplicated.drop('_sort_parsed', axis=1)
                else:
                    print(f"Column '{sort_column}' not found, using original order")
                    df_deduplicated = df.drop_duplicates([duplicate_column], keep=keep)
                
                final_count = len(df_deduplicated)
                removed_count = initial_count - final_count
                
                print(f"✓ After deduplication: {final_count} unique records")
                print(f"✓ Removed {removed_count} duplicate records")
                
                self.deduplication_stats.update({
                    'initial_count': initial_count,
                    'final_count': final_count,
                    'removed_count': removed_count,
                    'deduplication_method': f"keep_{keep}_by_{sort_column}"
                })
                
                return df_deduplicated
            else:
                print("Keeping all duplicates as requested")
                return df
        else:
            print(f"✓ No duplicates found in '{duplicate_column}' column")
            self.deduplication_stats = {
                'duplicates_found': 0,
                'initial_count': initial_count,
                'final_count': initial_count,
                'removed_count': 0
            }
            return df
    
    def transform_single_user(self, raw_user: Dict[str, Any]) -> Optional[UserModel]:
        """
        Transforme un utilisateur brut en UserModel
        """
        try:
            # Mapping des champs avec transformation et nettoyage des NaN
            transformed_data = {
                'id': self._clean_string_field(raw_user.get('id', '')),
                'email': self._clean_string_field(raw_user.get('email', '')),
                'emailVerified': bool(raw_user.get('emailVerified', False)),
                'password': self._clean_string_field(raw_user.get('password')),
                'uid': self._clean_string_field(raw_user.get('uid')),
                'provider': self._clean_string_field(raw_user.get('provider', 'CREDENTIALS')),
                'profilePic': self._clean_string_field(raw_user.get('profilePic') or raw_user.get('profile_pic')),
                'phoneNumber': self._clean_string_field(raw_user.get('phoneNumber') or raw_user.get('phone_number')),
                'phoneVerified': bool(raw_user.get('phoneVerified', False)),
                'name': self._clean_string_field(raw_user.get('name') or raw_user.get('displayName')),
                'city': self._clean_string_field(raw_user.get('city')),
                'birthdate': self._parse_datetime(raw_user.get('birthDate') or raw_user.get('birth_date')),
                'photo': self._clean_string_field(raw_user.get('photo') or raw_user.get('photoURL')),
                'createdAt': self._parse_datetime(raw_user.get('createdAt') or raw_user.get('created_at')) or datetime.now(),
                'updatedAt': self._parse_datetime(raw_user.get('updatedAt') or raw_user.get('updated_at')) or datetime.now(),
                'status': self._normalize_status(raw_user.get('status')),
                'interests': self._parse_interests(raw_user.get('interests')),
                'lastConnexion': self._parse_datetime(raw_user.get('lastConnexion') or raw_user.get('last_connexion'))
            }
            
            # Special handling for users without email (Google provider)
            if not transformed_data['email'] and raw_user.get('provider') == 'google.com':
                transformed_data['email'] = f"google_user_{raw_user.get('uid', 'unknown')}@placeholder.com"
                print(f"⚠️  Generated placeholder email for Google user: {transformed_data['email']}")
            
            # Ensure required fields have values
            if not transformed_data['id']:
                transformed_data['id'] = str(uuid.uuid4())[:20]
            
            if not transformed_data['email']:
                raise ValueError("Email is required but missing")
            
            if not transformed_data['provider']:
                transformed_data['provider'] = 'CREDENTIALS'
            
            user_model = UserModel(**transformed_data)
            self.successful_transformations += 1
            return user_model
            
        except ValidationError as e:
            self.failed_transformations += 1
            error_info = {
                'user_id': raw_user.get('id', 'unknown'),
                'error': str(e),
                'provider': raw_user.get('provider', 'unknown'),
                'has_email': bool(raw_user.get('email')),
                'raw_data_keys': list(raw_user.keys())
            }
            self.transformation_errors.append(error_info)
            print(f"❌ Validation error for user {error_info['user_id']}: {str(e)}")
            return None
            
        except Exception as e:
            self.failed_transformations += 1
            error_info = {
                'user_id': raw_user.get('id', 'unknown'),
                'error': f"Unexpected error: {str(e)}",
                'provider': raw_user.get('provider', 'unknown'),
                'has_email': bool(raw_user.get('email')),
                'raw_data_keys': list(raw_user.keys())
            }
            self.transformation_errors.append(error_info)
            print(f"❌ Transformation error for user {error_info['user_id']}: {str(e)}")
            return None
    
    def transform_users_dataframe(self, df: pd.DataFrame, remove_duplicates: bool = True) -> pd.DataFrame:
        """
        Transforme un DataFrame d'utilisateurs bruts en DataFrame de UserModel
        """
        self._reset_counters()
        
        print(f"🔄 Starting transformation of {len(df)} users...")
        
        # Step 1: Clean DataFrame - replace NaN with None de manière sécurisée
        print("🧹 Cleaning NaN values...")
        df_cleaned = df.copy()
        
        # Nettoyage colonne par colonne pour éviter les erreurs d'ambiguïté
        for col in df_cleaned.columns:
            try:
                # Appliquer le nettoyage sur chaque cellule individuellement
                df_cleaned[col] = df_cleaned[col].apply(lambda x: self._clean_nan_values(x))
            except Exception as e:
                print(f"⚠️  Warning: Could not clean column {col}: {e}")
                # En cas d'erreur, essayer un nettoyage basique
                df_cleaned[col] = df_cleaned[col].where(pd.notna(df_cleaned[col]), None)
        
        # Step 2: Remove duplicates if requested
        if remove_duplicates:
            print("\n=== Detecting and removing duplicates ===")
            df_cleaned = self.detect_and_remove_duplicates(df_cleaned)
        
        # Step 3: Transform users
        print(f"\n=== Transforming users to UserModel ===")
        transformed_users = []
        
        for idx, row in df_cleaned.iterrows():
            raw_user = row.to_dict()
            user_model = self.transform_single_user(raw_user)
            
            if user_model:
                user_dict = user_model.dict()
                transformed_users.append(user_dict)
            
            # Progress indicator
            if (idx + 1) % 100 == 0 or (idx + 1) == len(df_cleaned):
                print(f"📝 Processed {idx + 1}/{len(df_cleaned)} users... (Success: {self.successful_transformations}, Failed: {self.failed_transformations})")
        
        result_df = pd.DataFrame(transformed_users)
        
        print(f"✅ Transformation completed: {len(result_df)} users successfully transformed")
        
        return result_df
    
    def transform_users_list(self, users_list: List[Dict[str, Any]]) -> List[UserModel]:
        """
        Transforme une liste d'utilisateurs bruts en liste de UserModel
        """
        self._reset_counters()
        
        transformed_users = []
        for raw_user in users_list:
            user_model = self.transform_single_user(raw_user)
            if user_model:
                transformed_users.append(user_model)
        
        return transformed_users
    
    def _reset_counters(self):
        """Reset les compteurs de transformation"""
        self.transformation_errors = []
        self.successful_transformations = 0
        self.failed_transformations = 0
        self.deduplication_stats = {}
    
    def get_transformation_report(self) -> Dict[str, Any]:
        """
        Retourne un rapport détaillé de la transformation
        """
        total = self.successful_transformations + self.failed_transformations
        success_rate = (self.successful_transformations / total * 100) if total > 0 else 0
        
        return {
            'successful_transformations': self.successful_transformations,
            'failed_transformations': self.failed_transformations,
            'success_rate': success_rate,
            'errors': self.transformation_errors,
            'deduplication_stats': self.deduplication_stats
        }
    
    def export_transformed_users(self, users_df: pd.DataFrame, filename: str = 'transformed_users.csv') -> bool:
        """
        Exporte les utilisateurs transformés vers un fichier CSV
        """
        try:
            users_df.to_csv(filename, index=False, encoding='utf-8')
            print(f"✅ Transformed users exported to: {filename}")
            return True
        except Exception as e:
            print(f"❌ Error exporting transformed users: {e}")
            return False
    
    def validate_required_fields(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Valide la présence des champs requis dans le DataFrame
        """
        required_fields = ['id', 'email']
        missing_fields = []
        null_value_fields = []
        
        for field in required_fields:
            if field not in df.columns:
                missing_fields.append(field)
            else:
                # Check for null/NaN values in required fields using safe method
                null_count = 0
                for val in df[field]:
                    if self._safe_isna(val):
                        null_count += 1
                
                if null_count > 0:
                    null_value_fields.append(f"{field} ({null_count} null values)")
        
        is_valid = len(missing_fields) == 0 and len(null_value_fields) == 0
        
        return {
            'is_valid': is_valid,
            'missing_required_fields': missing_fields,
            'null_values_in_required_fields': null_value_fields,
            'total_records': len(df)
        }
    
    def get_deduplication_stats(self) -> Dict[str, Any]:
        """
        Retourne les statistiques de déduplication
        """
        return self.deduplication_stats