import pandas as pd
from typing import Optional, List, Dict, Any
from datetime import datetime
import json
from pydantic import BaseModel, ValidationError
from enum import Enum

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
    
    def _parse_datetime(self, value: Any) -> Optional[datetime]:
        """
        Parse différents formats de datetime
        """
        if value is None:
            return None
        
        if isinstance(value, datetime):
            return value
        
        if isinstance(value, str):
            try:
                # Try ISO format first (handle Z timezone)
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            except ValueError:
                try:
                    # Try common formats
                    for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y']:
                        try:
                            return datetime.strptime(value, fmt)
                        except ValueError:
                            continue
                except:
                    pass
        
        # Handle Firebase Timestamp objects
        if hasattr(value, 'seconds'):
            return datetime.fromtimestamp(value.seconds)
        
        return None
    
    def _parse_interests(self, value: Any) -> Optional[List[str]]:
        """
        Parse les intérêts depuis différents formats
        """
        if value is None:
            return None
        
        if isinstance(value, list):
            return [str(item) for item in value]
        
        if isinstance(value, str):
            try:
                # Try to parse as JSON array
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return [str(item) for item in parsed]
            except json.JSONDecodeError:
                # Split by comma if it's a comma-separated string
                return [item.strip() for item in value.split(',') if item.strip()]
        
        return None
    
    def _normalize_status(self, value: Any) -> UserStatus:
        """
        Normalise le statut utilisateur
        """
        if value is None:
            return UserStatus.ACTIVE
        
        status_str = str(value).upper()
        
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
    
    def detect_and_remove_duplicates(self, df: pd.DataFrame, 
                                   duplicate_column: str = 'email', 
                                   sort_column: str = 'createdAt',
                                   keep: str = 'last') -> pd.DataFrame:
        """
        Détecte et supprime les doublons dans le DataFrame
        
        Args:
            df: DataFrame source
            duplicate_column: Colonne à utiliser pour détecter les doublons (par défaut 'email')
            sort_column: Colonne à utiliser pour trier avant déduplication (par défaut 'createdAt')
            keep: 'first', 'last' ou 'all' pour garder le premier, dernier ou tous les doublons
        """
        initial_count = len(df)
        
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
                    # Parse the sort column as datetime if it contains date-like data
                    df_copy = df.copy()
                    df_copy['_sort_parsed'] = df_copy[sort_column].apply(self._parse_datetime)
                    
                    # Count successful parses
                    valid_dates = df_copy['_sort_parsed'].notna().sum()
                    print(f"Successfully parsed {valid_dates} out of {len(df_copy)} dates")
                    
                    # Sort by parsed date
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
                
                # Update stats
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
            # Mapping des champs avec transformation
            transformed_data = {
                'id': raw_user.get('id', ''),
                'email': raw_user.get('email', ''),
                'emailVerified': bool(raw_user.get('emailVerified', False)),
                'password': raw_user.get('password'),
                'uid': raw_user.get('uid'),
                'provider': raw_user.get('provider', 'CREDENTIALS'),
                'profilePic': raw_user.get('profilePic') or raw_user.get('profile_pic'),
                'phoneNumber': raw_user.get('phoneNumber') or raw_user.get('phone_number'),
                'phoneVerified': bool(raw_user.get('phoneVerified', False)),
                'name': raw_user.get('name') or raw_user.get('displayName'),
                'city': raw_user.get('city'),
                'birthdate': self._parse_datetime(raw_user.get('birthdate') or raw_user.get('birth_date')),
                'photo': raw_user.get('photo') or raw_user.get('photoURL'),
                'createdAt': self._parse_datetime(raw_user.get('createdAt') or raw_user.get('created_at')) or datetime.now(),
                'updatedAt': self._parse_datetime(raw_user.get('updatedAt') or raw_user.get('updated_at')) or datetime.now(),
                'status': self._normalize_status(raw_user.get('status')),
                'interests': self._parse_interests(raw_user.get('interests')),
                'lastConnexion': self._parse_datetime(raw_user.get('lastConnexion') or raw_user.get('last_connexion'))
            }
            
            # Remove None values for optional fields
            transformed_data = {k: v for k, v in transformed_data.items() if v is not None or k in ['id', 'email', 'createdAt', 'updatedAt']}
            
            user_model = UserModel(**transformed_data)
            self.successful_transformations += 1
            return user_model
            
        except ValidationError as e:
            self.failed_transformations += 1
            error_info = {
                'user_id': raw_user.get('id', 'unknown'),
                'error': str(e),
                'raw_data': raw_user
            }
            self.transformation_errors.append(error_info)
            return None
        except Exception as e:
            self.failed_transformations += 1
            error_info = {
                'user_id': raw_user.get('id', 'unknown'),
                'error': f"Unexpected error: {str(e)}",
                'raw_data': raw_user
            }
            self.transformation_errors.append(error_info)
            return None
    
    def transform_users_dataframe(self, df: pd.DataFrame, remove_duplicates: bool = True) -> pd.DataFrame:
        """
        Transforme un DataFrame d'utilisateurs bruts en DataFrame de UserModel
        
        Args:
            df: DataFrame source
            remove_duplicates: Si True, supprime les doublons avant transformation
        """
        self._reset_counters()
        
        # Step 1: Remove duplicates if requested
        if remove_duplicates:
            print("\n=== Detecting and removing duplicates ===")
            df = self.detect_and_remove_duplicates(df)
        
        # Step 2: Transform users
        transformed_users = []
        
        for _, row in df.iterrows():
            raw_user = row.to_dict()
            user_model = self.transform_single_user(raw_user)
            
            if user_model:
                # Convert UserModel to dict for DataFrame
                user_dict = user_model.dict()
                transformed_users.append(user_dict)
        
        return pd.DataFrame(transformed_users)
    
    def transform_users_list(self, users_list: List[Dict[str, Any]]) -> List[UserModel]:
        """
        Transforme une liste d'utilisateurs bruts en liste de UserModel
        """
        transformed_users = []
        self._reset_counters()
        
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
        Retourne un rapport de transformation complet
        """
        report = {
            'successful_transformations': self.successful_transformations,
            'failed_transformations': self.failed_transformations,
            'total_processed': self.successful_transformations + self.failed_transformations,
            'success_rate': (self.successful_transformations / (self.successful_transformations + self.failed_transformations)) * 100 if (self.successful_transformations + self.failed_transformations) > 0 else 0,
            'errors': self.transformation_errors,
            'deduplication_stats': self.deduplication_stats
        }
        return report
    
    def export_transformed_users(self, users_df: pd.DataFrame, filename: str = 'transformed_users.csv') -> bool:
        """
        Exporte les utilisateurs transformés vers un fichier CSV
        """
        try:
            users_df.to_csv(filename, index=False, encoding='utf-8')
            json_filename = filename.replace('.csv', '.json')
            users_df.to_json(json_filename, orient='records', lines=True, force_ascii=False)
            print(f"Transformed users exported to {filename} and {json_filename}")
            return True
        except Exception as e:
            print(f"Error exporting transformed users: {e}")
            return False
    
    def validate_required_fields(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Valide les champs requis avant transformation
        """
        required_fields = ['id', 'email']
        missing_fields = []
        
        for field in required_fields:
            if field not in df.columns:
                missing_fields.append(field)
        
        # Check for null values in required fields
        null_counts = {}
        for field in required_fields:
            if field in df.columns:
                null_count = df[field].isnull().sum()
                if null_count > 0:
                    null_counts[field] = null_count
        
        return {
            'missing_required_fields': missing_fields,
            'null_values_in_required_fields': null_counts,
            'total_rows': len(df),
            'is_valid': len(missing_fields) == 0 and len(null_counts) == 0
        }
    
    def get_deduplication_stats(self) -> Dict[str, Any]:
        """
        Retourne les statistiques de déduplication
        """
        return self.deduplication_stats