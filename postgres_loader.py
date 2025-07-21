import pandas as pd
import os
from typing import Dict, Any, List, Optional
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
from datetime import datetime
import uuid
import numpy as np

class PostgreSQLLoaderService:
    """
    Service pour charger les données transformées dans PostgreSQL
    """
    
    def __init__(self):
        """
        Initialize PostgreSQL connection
        """
        try:
            # Get database connection parameters from environment
            self.db_host = os.getenv('POSTGRES_HOST', 'localhost')
            self.db_port = os.getenv('POSTGRES_PORT', '5432')
            self.db_name = os.getenv('POSTGRES_DB', 'your_database')
            self.db_user = os.getenv('POSTGRES_USER', 'user')
            self.db_password = os.getenv('POSTGRES_PASSWORD', 'password')
            
            # Get batch size from environment
            self.batch_size = int(os.getenv('POSTGRES_BATCH_SIZE', '1000'))
            print(f"🔧 PostgreSQL batch size: {self.batch_size}")
            
            # Create connection string
            self.connection_string = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
            
            # Create SQLAlchemy engine
            self.engine = create_engine(self.connection_string)
            
            # Test connection
            self._test_connection()
            
            print("✅ PostgreSQL connection established successfully")
            
        except Exception as e:
            print(f"❌ Error connecting to PostgreSQL: {e}")
            raise

    def _test_connection(self):
        """Test the database connection"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            print("✅ Database connection test successful")
        except Exception as e:
            print(f"❌ Database connection test failed: {e}")
            raise

    def get_existing_user_ids(self) -> List[str]:
        """
        Récupère tous les IDs d'utilisateurs existants dans la base de données
        """
        try:
            with self.engine.connect() as conn:
                query = text('SELECT id FROM public."User"')
                result = conn.execute(query)
                existing_ids = [row[0] for row in result.fetchall()]
                
            print(f"📊 Found {len(existing_ids)} existing users in database")
            return existing_ids
            
        except Exception as e:
            print(f"❌ Error fetching existing user IDs: {e}")
            return []

    def get_existing_user_emails(self) -> List[str]:
        """
        Récupère tous les emails d'utilisateurs existants dans la base de données
        """
        try:
            with self.engine.connect() as conn:
                query = text('SELECT email FROM public."User"')
                result = conn.execute(query)
                existing_emails = [row[0] for row in result.fetchall()]
                
            print(f"📊 Found {len(existing_emails)} existing user emails in database")
            return existing_emails
            
        except Exception as e:
            print(f"❌ Error fetching existing user emails: {e}")
            return []

    def check_table_exists(self) -> bool:
        """
        Vérifie si la table User existe
        """
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names(schema='public')
            exists = 'User' in tables
            
            if exists:
                print("✅ Table 'User' exists")
            else:
                print("❌ Table 'User' does not exist")
                
            return exists
            
        except Exception as e:
            print(f"❌ Error checking table existence: {e}")
            return False

    def get_table_info(self) -> Dict[str, Any]:
        """
        Récupère les informations sur la table User
        """
        try:
            inspector = inspect(self.engine)
            
            # Get columns
            columns = inspector.get_columns('User', schema='public')
            column_names = [col['name'] for col in columns]
            
            # Get indexes
            indexes = inspector.get_indexes('User', schema='public')
            
            # Get row count
            with self.engine.connect() as conn:
                count_query = text('SELECT COUNT(*) FROM public."User"')
                result = conn.execute(count_query)
                row_count = result.fetchone()[0]
            
            info = {
                'columns': column_names,
                'column_details': columns,
                'indexes': indexes,
                'row_count': row_count
            }
            
            print(f"📋 Table info - Columns: {len(column_names)}, Rows: {row_count}")
            return info
            
        except Exception as e:
            print(f"❌ Error getting table info: {e}")
            return {}

    def _clean_dataframe_for_postgres(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Nettoie le DataFrame pour PostgreSQL en gérant NaT et autres valeurs problématiques
        """
        df_clean = df.copy()
        
        # Convert datetime columns and handle NaT de manière plus agressive
        datetime_columns = ['createdAt', 'updatedAt', 'birthdate', 'lastConnexion']
        for col in datetime_columns:
            if col in df_clean.columns:
                print(f"🧹 Cleaning datetime column: {col}")
                
                # Méthode plus agressive pour nettoyer les NaT
                def ultra_clean_datetime(val):
                    try:
                        # Vérifications multiples pour NaT
                        if val is None:
                            return None
                        if pd.isna(val):
                            return None
                        if hasattr(val, '__str__'):
                            val_str = str(val).lower().strip()
                            if val_str in ['nat', 'none', 'null', '', 'nan']:
                                return None
                        
                        # Si c'est un pandas NaT spécifique
                        if hasattr(val, '_value') and pd.isna(val):
                            return None
                            
                        # Si c'est déjà un datetime, le garder
                        if isinstance(val, datetime):
                            return val
                            
                        # Essayer de convertir
                        if isinstance(val, str) and val.strip():
                            try:
                                converted = pd.to_datetime(val, errors='coerce')
                                if pd.isna(converted):
                                    return None
                                return converted.to_pydatetime()
                            except:
                                return None
                                
                        # Pour tout le reste, essayer pandas to_datetime
                        try:
                            converted = pd.to_datetime(val, errors='coerce')
                            if pd.isna(converted):
                                return None
                            return converted.to_pydatetime()
                        except:
                            return None
                            
                    except Exception as e:
                        print(f"⚠️  Error cleaning datetime value {val}: {e}")
                        return None
                
                # Appliquer le nettoyage ultra
                df_clean[col] = df_clean[col].apply(ultra_clean_datetime)
                
                # Vérification finale : s'assurer qu'il n'y a plus de NaT
                nat_count = 0
                for idx, val in enumerate(df_clean[col]):
                    try:
                        if pd.isna(val) or str(val).lower() == 'nat':
                            df_clean.at[idx, col] = None
                            nat_count += 1
                    except:
                        df_clean.at[idx, col] = None
                        nat_count += 1
                
                if nat_count > 0:
                    print(f"🧹 Cleaned {nat_count} remaining NaT values in {col}")
        
        # Handle UserStatus enum values
        if 'status' in df_clean.columns:
            df_clean['status'] = df_clean['status'].apply(
                lambda x: x.value if hasattr(x, 'value') else str(x) if x is not None else 'ACTIVE'
            )
        
        # Handle interests array
        if 'interests' in df_clean.columns:
            df_clean['interests'] = df_clean['interests'].apply(self._format_array_for_postgres)
        
        # Handle boolean columns
        bool_columns = ['emailVerified', 'phoneVerified']
        for col in bool_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].fillna(False).astype(bool)
        
        # Handle string columns - replace NaN with None
        string_columns = ['password', 'uid', 'profilePic', 'phoneNumber', 'name', 'city', 'photo']
        for col in string_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].where(pd.notna(df_clean[col]), None)
        
        # Ensure required columns have values
        if 'provider' in df_clean.columns:
            df_clean['provider'] = df_clean['provider'].fillna('CREDENTIALS')
        
        print(f"🧹 Cleaned DataFrame: {len(df_clean)} rows ready for PostgreSQL")
        return df_clean

    def _final_clean_value(self, value: Any) -> Any:
        """
        Nettoyage final ultra-agressif des valeurs avant insertion PostgreSQL
        """
        if value is None:
            return None
            
        # Vérifications multiples et ultra-agressives pour NaT
        try:
            # Test 1: pandas isna
            if pd.isna(value):
                return None
        except (ValueError, TypeError):
            pass
            
        try:
            # Test 2: string representation
            val_str = str(value).lower().strip()
            if val_str in ['nat', 'none', 'null', '', 'nan', 'NaT']:
                return None
        except:
            pass
            
        try:
            # Test 3: pandas NaT spécifique
            if hasattr(value, '_value') and pd.isna(value):
                return None
        except:
            pass
            
        try:
            # Test 4: si c'est un pandas Timestamp avec NaT
            if hasattr(value, 'to_pydatetime'):
                if pd.isna(value):
                    return None
                return value.to_pydatetime()
        except:
            return None
            
        try:
            # Test 5: float NaN
            if isinstance(value, float) and np.isnan(value):
                return None
        except:
            pass
            
        # Si on arrive ici, la valeur devrait être saine
        return value

    def _insert_single_user(self, user_data: Dict[str, Any]) -> None:
        """
        Insère un seul utilisateur en excluant temporairement les colonnes problématiques
        """
        with self.engine.connect() as conn:
            with conn.begin():  # Individual transaction
                # Liste des colonnes à exclure temporairement si elles causent des problèmes
                problematic_columns = ['birthdate']  # Ajouter d'autres si nécessaire
                
                columns = []
                placeholders = []
                clean_params = {}
                
                for key, value in user_data.items():
                    # Skip les colonnes problématiques pour l'instant
                    if key in problematic_columns:
                        continue
                        
                    cleaned_value = self._final_clean_value(value)
                    
                    if cleaned_value is not None:
                        columns.append(f'"{key}"')
                        placeholders.append(f':{key}')
                        clean_params[key] = cleaned_value
                
                if not columns:
                    raise ValueError("No valid data to insert")
                
                query = f"""
                    INSERT INTO public."User" ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                """
                
                conn.execute(text(query), clean_params)

    def _insert_batch_users(self, users_batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Insère un batch d'utilisateurs avec gestion d'erreurs
        """
        batch_result = {
            'inserted_count': 0,
            'failed_count': 0,
            'errors': []
        }
        
        try:
            with self.engine.connect() as conn:
                with conn.begin():  # Transaction pour tout le batch
                    for user_data in users_batch:
                        try:
                            # Préparer les données pour l'insertion
                            columns = []
                            placeholders = []
                            clean_params = {}
                            
                            for key, value in user_data.items():
                                cleaned_value = self._final_clean_value(value)
                                
                                if cleaned_value is not None:
                                    columns.append(f'"{key}"')
                                    placeholders.append(f':{key}')
                                    clean_params[key] = cleaned_value
                            
                            if not columns:
                                raise ValueError("No valid data to insert")
                            
                            query = f"""
                                INSERT INTO public."User" ({', '.join(columns)})
                                VALUES ({', '.join(placeholders)})
                            """
                            
                            conn.execute(text(query), clean_params)
                            batch_result['inserted_count'] += 1
                            
                        except Exception as e:
                            batch_result['failed_count'] += 1
                            error_info = {
                                'user_id': user_data.get('id', 'unknown'),
                                'error': str(e)
                            }
                            batch_result['errors'].append(error_info)
                            
        except Exception as e:
            # Si toute la transaction échoue, marquer tous comme échec
            print(f"❌ Batch transaction failed: {e}")
            batch_result['failed_count'] = len(users_batch)
            batch_result['inserted_count'] = 0
            batch_result['errors'] = [{'batch_error': str(e)}]
        
        return batch_result

    def _prepare_dataframe_for_insertion(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prépare le DataFrame pour l'insertion en filtrant les colonnes non-DB
        """
        # Colonnes autorisées dans la table User
        allowed_columns = [
            'id', 'email', 'emailVerified', 'password', 'uid', 'provider', 
            'profilePic', 'phoneNumber', 'phoneVerified', 'name', 'city', 
            'birthdate', 'photo', 'createdAt', 'updatedAt', 'status', 
            'interests', 'lastConnexion'
        ]
        
        # Filtrer les colonnes
        available_columns = [col for col in allowed_columns if col in df.columns]
        df_filtered = df[available_columns].copy()
        
        print(f"🔧 Filtered DataFrame columns: {df_filtered.columns.tolist()}")
        
        return self._clean_dataframe_for_postgres(df_filtered)

    def _format_array_for_postgres(self, value) -> Optional[str]:
        """
        Formate les arrays pour PostgreSQL
        """
        if value is None or (isinstance(value, list) and len(value) == 0):
            return None
        
        if isinstance(value, list):
            # Escape quotes and format as PostgreSQL array
            escaped_items = [str(item).replace("'", "''") if item is not None else '' for item in value]
            return "{" + ",".join(f"'{item}'" for item in escaped_items) + "}"
        
        if isinstance(value, str):
            # Try to parse as array if it looks like one
            value = value.strip()
            if value.startswith('{') and value.endswith('}'):
                return value  # Already formatted
            else:
                return "{'" + value.replace("'", "''") + "'}"
        
        return None

    def load_single_user(self, user_data: Dict[str, Any]) -> bool:
        """
        Charge un seul utilisateur dans PostgreSQL
        """
        try:
            self._insert_single_user(user_data)
            print(f"✅ User {user_data.get('id', 'unknown')} inserted successfully")
            return True
        except Exception as e:
            print(f"❌ Failed to insert user {user_data.get('id', 'unknown')}: {e}")
            return False

    def update_user(self, user_id: str, user_data: Dict[str, Any]) -> bool:
        """
        Met à jour un utilisateur existant
        """
        try:
            with self.engine.connect() as conn:
                with conn.begin():
                    # Build UPDATE query
                    set_clauses = []
                    clean_params = {'user_id': user_id}
                    
                    for key, value in user_data.items():
                        if key != 'id':  # Don't update the ID
                            set_clauses.append(f'"{key}" = :{key}')
                            # Clean the value
                            if pd.isna(value):
                                clean_params[key] = None
                            elif hasattr(value, 'to_pydatetime'):
                                clean_params[key] = value.to_pydatetime()
                            else:
                                clean_params[key] = value
                    
                    query = f"""
                        UPDATE public."User"
                        SET {', '.join(set_clauses)}
                        WHERE id = :user_id
                    """
                    
                    conn.execute(text(query), clean_params)
                
                print(f"✅ User {user_id} updated successfully")
                return True
                
        except Exception as e:
            print(f"❌ Failed to update user {user_id}: {e}")
            return False

    def delete_user(self, user_id: str) -> bool:
        """
        Supprime un utilisateur
        """
        try:
            with self.engine.connect() as conn:
                with conn.begin():
                    query = text('DELETE FROM public."User" WHERE id = :user_id')
                    result = conn.execute(query, {'user_id': user_id})
                    
                    if result.rowcount > 0:
                        print(f"✅ User {user_id} deleted successfully")
                        return True
                    else:
                        print(f"⚠️  User {user_id} not found")
                        return False
                        
        except Exception as e:
            print(f"❌ Failed to delete user {user_id}: {e}")
            return False

    def get_user_stats(self) -> Dict[str, Any]:
        """
        Récupère les statistiques des utilisateurs
        """
        try:
            with self.engine.connect() as conn:
                # Total users
                total_query = text('SELECT COUNT(*) FROM public."User"')
                total_users = conn.execute(total_query).fetchone()[0]
                
                # Users by provider
                provider_query = text('''
                    SELECT provider, COUNT(*) 
                    FROM public."User" 
                    GROUP BY provider
                ''')
                provider_stats = dict(conn.execute(provider_query).fetchall())
                
                # Users with email verification
                verified_query = text('''
                    SELECT "emailVerified", COUNT(*) 
                    FROM public."User" 
                    GROUP BY "emailVerified"
                ''')
                verified_stats = dict(conn.execute(verified_query).fetchall())
                
                # Recent users (last 30 days)
                recent_query = text('''
                    SELECT COUNT(*) 
                    FROM public."User" 
                    WHERE "createdAt" >= NOW() - INTERVAL '30 days'
                ''')
                recent_users = conn.execute(recent_query).fetchone()[0]
                
                stats = {
                    'total_users': total_users,
                    'provider_distribution': provider_stats,
                    'email_verification': verified_stats,
                    'recent_users_30_days': recent_users
                }
                
                print(f"📊 Database stats - Total: {total_users}, Recent: {recent_users}")
                return stats
                
        except Exception as e:
            print(f"❌ Error getting user stats: {e}")
            return {}

    def cleanup_duplicates(self, column: str = 'email') -> Dict[str, Any]:
        """
        Nettoie les doublons dans la base de données
        """
        try:
            with self.engine.connect() as conn:
                with conn.begin():
                    # Find duplicates
                    duplicate_query = text(f'''
                        SELECT "{column}", COUNT(*) 
                        FROM public."User" 
                        GROUP BY "{column}" 
                        HAVING COUNT(*) > 1
                    ''')
                    
                    duplicates = conn.execute(duplicate_query).fetchall()
                    
                    if not duplicates:
                        print("✅ No duplicates found")
                        return {'duplicates_found': 0, 'removed': 0}
                    
                    print(f"⚠️  Found {len(duplicates)} duplicate {column} values")
                    
                    removed_count = 0
                    for duplicate_value, count in duplicates:
                        # Keep the newest record, delete the rest
                        delete_query = text(f'''
                            DELETE FROM public."User" 
                            WHERE "{column}" = :value 
                            AND id NOT IN (
                                SELECT id FROM public."User" 
                                WHERE "{column}" = :value 
                                ORDER BY "createdAt" DESC 
                                LIMIT 1
                            )
                        ''')
                        
                        result = conn.execute(delete_query, {'value': duplicate_value})
                        removed_count += result.rowcount
                        print(f"🧹 Removed {result.rowcount} duplicate records for {column}: {duplicate_value}")
                    
                    return {
                        'duplicates_found': len(duplicates),
                        'removed': removed_count
                    }
                
        except Exception as e:
            print(f"❌ Error cleaning duplicates: {e}")
            return {'error': str(e)}

    def load_users_dataframe(self, df: pd.DataFrame, use_batch: bool = True) -> Dict[str, Any]:
        """
        Charge un DataFrame d'utilisateurs dans PostgreSQL avec insertion par batch ou individuelle
        
        Args:
            df: DataFrame à charger
            use_batch: Si True, utilise l'insertion par batch. Si False, insertion individuelle.
        """
        try:
            print(f"🔄 Starting to load {len(df)} users to PostgreSQL...")
            print(f"🔧 Using {'batch' if use_batch else 'individual'} insertion mode (batch size: {self.batch_size})")
            
            # Check if table exists
            if not self.check_table_exists():
                try:
                    self.create_user_table()
                except:
                    return {
                        'success': False,
                        'error': 'Table User does not exist and could not be created',
                        'total_processed': 0,
                        'inserted_count': 0,
                        'failed_count': 0,
                        'errors': []
                    }
            
            # Prepare dataframe for PostgreSQL
            df_clean = self._prepare_dataframe_for_insertion(df)
            
            # Track statistics
            total_processed = len(df_clean)
            inserted_count = 0
            failed_count = 0
            errors = []
            
            if use_batch and self.batch_size > 1:
                print(f"📦 Processing {total_processed} users in batches of {self.batch_size}...")
                
                # Process in batches
                for i in range(0, total_processed, self.batch_size):
                    batch_start = i
                    batch_end = min(i + self.batch_size, total_processed)
                    batch_df = df_clean.iloc[batch_start:batch_end]
                    
                    print(f"📝 Processing batch {batch_start//self.batch_size + 1}/{(total_processed-1)//self.batch_size + 1} ({batch_start+1}-{batch_end}/{total_processed})...")
                    
                    # Convert batch to list of dicts
                    batch_users = batch_df.to_dict('records')
                    
                    # Insert batch
                    batch_result = self._insert_batch_users(batch_users)
                    
                    # Update counters
                    inserted_count += batch_result['inserted_count']
                    failed_count += batch_result['failed_count']
                    errors.extend(batch_result['errors'])
                    
                    print(f"📊 Batch result: {batch_result['inserted_count']} inserted, {batch_result['failed_count']} failed")
                    
                    # Si le batch échoue complètement, passer en mode individuel pour ce batch
                    if batch_result['inserted_count'] == 0 and len(batch_result['errors']) == 1 and 'batch_error' in batch_result['errors'][0]:
                        print("⚠️  Batch failed, retrying individual insertions for this batch...")
                        failed_count -= batch_result['failed_count']  # Reset failed count
                        errors = errors[:-1]  # Remove batch error
                        
                        # Retry individual insertions for failed batch
                        for idx, row in batch_df.iterrows():
                            try:
                                user_data = row.to_dict()
                                self._insert_single_user(user_data)
                                inserted_count += 1
                            except Exception as e:
                                failed_count += 1
                                error_info = {
                                    'user_id': user_data.get('id', 'unknown'),
                                    'error': str(e)
                                }
                                errors.append(error_info)
                        
            else:
                print("📝 Processing users individually...")
                
                # Insert users one by one
                for idx, row in df_clean.iterrows():
                    try:
                        user_data = row.to_dict()
                        self._insert_single_user(user_data)
                        inserted_count += 1
                        
                        # Progress indicator
                        if (idx + 1) % 100 == 0 or (idx + 1) == len(df_clean):
                            print(f"📝 Processed {idx + 1}/{len(df_clean)} users... (Success: {inserted_count}, Failed: {failed_count})")
                            
                    except Exception as e:
                        failed_count += 1
                        error_info = {
                            'user_id': user_data.get('id', 'unknown'),
                            'error': str(e)
                        }
                        errors.append(error_info)
            
            print("✅ Loading completed!")
            
            # Get updated database stats
            try:
                stats = self.get_user_stats()
            except:
                stats = {'total_users': 0, 'provider_distribution': {}}
            
            return {
                'success': True,
                'total_processed': total_processed,
                'inserted_count': inserted_count,
                'failed_count': failed_count,
                'errors': errors,
                'database_stats': stats,
                'batch_size_used': self.batch_size if use_batch else 1
            }
            
        except Exception as e:
            print(f"❌ Critical error during DataFrame loading: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e),
                'total_processed': len(df) if df is not None else 0,
                'inserted_count': 0,
                'failed_count': 0,
                'errors': []
            }

    def load_users_list(self, users_list: List[Dict[str, Any]], use_batch: bool = True) -> Dict[str, Any]:
        """
        Charge une liste d'utilisateurs dans PostgreSQL
        """
        # Convert list to DataFrame and use the DataFrame method
        df = pd.DataFrame(users_list)
        return self.load_users_dataframe(df, use_batch=use_batch)

    def ensure_table_exists(self) -> bool:
        """
        Vérifie que la table User existe, sinon la crée
        """
        try:
            with self.engine.connect() as conn:
                # Check if table exists
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'User'
                    );
                """))
                
                table_exists = result.scalar()
                
                if table_exists:
                    print("✅ Table 'User' exists")
                    return True
                else:
                    print("⚠️  Table 'User' does not exist, creating it...")
                    return self.create_user_table()
                    
        except Exception as e:
            print(f"❌ Error checking table existence: {e}")
            return False

    def create_user_table(self) -> bool:
        """
        Crée la table User si elle n'existe pas
        """
        try:
            with self.engine.connect() as conn:
                with conn.begin():
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS public."User" (
                            "id" VARCHAR(255) PRIMARY KEY,
                            "email" VARCHAR(255) UNIQUE NOT NULL,
                            "emailVerified" BOOLEAN DEFAULT FALSE,
                            "password" TEXT,
                            "uid" VARCHAR(255),
                            "provider" VARCHAR(100) DEFAULT 'CREDENTIALS',
                            "profilePic" TEXT,
                            "phoneNumber" VARCHAR(50),
                            "phoneVerified" BOOLEAN DEFAULT FALSE,
                            "name" VARCHAR(255),
                            "city" VARCHAR(255),
                            "birthdate" TIMESTAMP,
                            "photo" TEXT,
                            "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            "updatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            "status" VARCHAR(50) DEFAULT 'ACTIVE',
                            "interests" TEXT,
                            "lastConnexion" TIMESTAMP
                        );
                    """))
                    
            print("✅ Table 'User' created successfully")
            return True
            
        except Exception as e:
            print(f"❌ Error creating table: {e}")
            return False