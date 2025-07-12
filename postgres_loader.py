import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Boolean, DateTime, JSON
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Dict, Any, Optional
import json
from datetime import datetime
from user_transformer import UserModel, UserStatus

class PostgreSQLLoaderService:
    """
    Service pour charger les données transformées dans PostgreSQL en utilisant SQLAlchemy
    """
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 5434,
                 database: str = "KAStudioDb",
                 username: str = "user",
                 password: str = "password",
                 table_name: str = "User_clone"):
        
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.table_name = table_name
        
        # Connection string for SQLAlchemy
        self.connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        self.engine = None
        self.metadata = None
        
        # Initialize load report
        self.load_report = {
            'total_processed': 0,
            'successful_loads': 0,
            'failed_loads': 0,
            'success_rate': 0.0,
            'errors': []
        }
    
    def connect(self) -> bool:
        """
        Établit la connexion à PostgreSQL existante
        """
        try:
            self.engine = create_engine(
                self.connection_string,
                pool_pre_ping=True,  # Vérifier la connexion avant utilisation
                pool_recycle=3600    # Recycler les connexions après 1h
            )
            
            # Test connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT current_database(), current_user"))
                db_info = result.fetchone()
                print(f"Successfully connected to PostgreSQL")
                print(f"Database: {db_info[0]}, User: {db_info[1]}")
            
            # Initialize metadata
            self.metadata = MetaData()
            return True
            
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            return False
    
    def check_table_exists(self) -> bool:
        """
        Vérifie si la table existe dans la base de données
        """
        try:
            with self.engine.connect() as conn:
                # CORRECTION: Utiliser des guillemets dans la requête d'existence
                result = conn.execute(text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = '{self.table_name}'
                    );
                """))
                return result.scalar()
        except Exception as e:
            print(f"Error checking table existence: {e}")
            return False
    
    def get_table_schema(self) -> Optional[Dict[str, Any]]:
        """
        Récupère le schéma de la table
        """
        try:
            with self.engine.connect() as conn:
                # Get column information
                # CORRECTION: Utiliser le nom exact de la table
                columns_result = conn.execute(text(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = '{self.table_name}' 
                    AND table_schema = 'public'
                    ORDER BY ordinal_position;
                """))
                
                columns = []
                for row in columns_result.fetchall():
                    columns.append({
                        'name': row[0],
                        'type': row[1],
                        'nullable': row[2] == 'YES'
                    })
                
                if columns:
                    return {
                        'table_name': self.table_name,
                        'columns': [col['name'] for col in columns],
                        'column_details': columns
                    }
                else:
                    return None
                    
        except Exception as e:
            print(f"Error getting table schema: {e}")
            return None
    
    def prepare_dataframe_for_existing_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prépare le DataFrame pour l'insertion dans la table existante
        """
        # Get existing table schema
        schema = self.get_table_schema()
        if not schema:
            print("Warning: Could not retrieve table schema")
            return df
        
        existing_columns = schema['columns']
        prepared_df = df.copy()
        
        # Column mapping pour s'adapter au schéma existant
        column_mapping = {
            'emailVerified': 'email_verified',
            'profilePic': 'profile_pic', 
            'phoneNumber': 'phone_number',
            'phoneVerified': 'phone_verified',
            'createdAt': 'created_at',
            'updatedAt': 'updated_at',
            'lastConnexion': 'last_connexion'
        }
        
        # Apply column mapping
        for old_col, new_col in column_mapping.items():
            if old_col in prepared_df.columns and new_col in existing_columns:
                prepared_df = prepared_df.rename(columns={old_col: new_col})
        
        # Keep only columns that exist in the target table
        df_columns = prepared_df.columns.tolist()
        columns_to_keep = [col for col in df_columns if col in existing_columns]
        prepared_df = prepared_df[columns_to_keep]
        
        # Convert datetime columns
        datetime_columns = ['birthdate', 'created_at', 'updated_at', 'last_connexion']
        for col in datetime_columns:
            if col in prepared_df.columns:
                prepared_df[col] = pd.to_datetime(prepared_df[col], errors='coerce')
        
        # Handle interests - convert to JSON string if column exists
        if 'interests' in prepared_df.columns and 'interests' in existing_columns:
            prepared_df['interests'] = prepared_df['interests'].apply(
                lambda x: json.dumps(x) if x is not None and isinstance(x, list) else None
            )
        
        # Add ETL timestamp if column exists
        if 'etl_loaded_at' in existing_columns:
            prepared_df['etl_loaded_at'] = datetime.now()
        
        # Handle None/NaN values
        prepared_df = prepared_df.where(pd.notna(prepared_df), None)
        
        print(f"DataFrame prepared with {len(prepared_df.columns)} columns matching table schema")
        return prepared_df
    
    def _reset_load_report(self):
        """
        Remet à zéro le rapport de chargement
        """
        self.load_report = {
            'total_processed': 0,
            'successful_loads': 0,
            'failed_loads': 0,
            'success_rate': 0.0,
            'errors': []
        }
    
    def get_load_report(self) -> Dict[str, Any]:
        """
        Retourne le rapport de chargement actuel
        """
        return self.load_report.copy()
    
    def load_users_with_sqlalchemy(self, users_df: pd.DataFrame, method: str = 'append', chunk_size: int = 1000) -> bool:
        """
        Charge les utilisateurs en utilisant SQLAlchemy
        
        Args:
            users_df: DataFrame with user data
            method: 'replace', 'append'
            chunk_size: Nombre d'enregistrements par chunk
        """
        try:
            self._reset_load_report()
            
            # Prepare data
            users_df_clean = self._prepare_dataframe_for_loading(users_df)
            print(f"DataFrame prepared with {len(users_df_clean.columns)} columns matching table schema")
            
            total_records = len(users_df_clean)
            print(f"Loading {total_records} records in chunks of {chunk_size}...")
            
            # Use to_sql with proper table name quoting
            users_df_clean.to_sql(
                name=self.table_name,
                con=self.engine,
                if_exists=method,
                index=False,
                chunksize=chunk_size,
                method='multi'
            )
            
            # Update load report
            self.load_report['total_processed'] = total_records
            self.load_report['successful_loads'] = total_records
            self.load_report['success_rate'] = 100.0
            
            return True
            
        except Exception as e:
            error_msg = f"SQLAlchemy Error loading users: {str(e)}"
            print(error_msg)
            self.load_report['errors'].append({
                'method': method,
                'error': error_msg,
                'record_count': len(users_df)
            })
            return False
    
    def _prepare_dataframe_for_loading(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prépare le DataFrame pour le chargement en base
        """
        df_clean = df.copy()
        
        # Handle datetime columns - convert to proper datetime objects
        datetime_columns = ['createdAt', 'updatedAt', 'birthdate', 'lastConnexion']
        for col in datetime_columns:
            if col in df_clean.columns:
                # Convert to datetime, handling various formats
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce', utc=True)
        
        # Handle enum values (convert to string)
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':
                # Convert enum objects to their string values
                df_clean[col] = df_clean[col].apply(
                    lambda x: x.value if hasattr(x, 'value') else (str(x) if x is not None else None)
                )
                # Replace 'nan' strings with None
                df_clean[col] = df_clean[col].replace(['nan', 'None'], None)
        
        # Handle boolean columns explicitly
        bool_columns = ['emailVerified', 'phoneVerified']
        for col in bool_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(bool)
        
        # Handle list columns (like interests) - convert to JSON strings or None
        list_columns = ['interests']
        for col in list_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].apply(
                    lambda x: None if x is None or pd.isna(x) else (x if isinstance(x, list) else None)
                )
        
        # Replace pandas NaT and NaN with None for PostgreSQL compatibility
        df_clean = df_clean.where(pd.notnull(df_clean), None)
        
        return df_clean

    def upsert_users_with_sqlalchemy(self, users_df: pd.DataFrame, chunk_size: int = 1000) -> bool:
        """
        Upsert des utilisateurs en utilisant SQLAlchemy avec ON CONFLICT
        """
        try:
            self._reset_load_report()
            
            # Prepare data
            users_df_clean = self._prepare_dataframe_for_loading(users_df)
            print(f"DataFrame prepared with {len(users_df_clean.columns)} columns matching table schema")
            
            total_records = len(users_df_clean)
            print(f"Upserting {total_records} records in chunks of {chunk_size}...")
            
            with self.engine.begin() as conn:  # Use begin() for automatic transaction management
                for i in range(0, total_records, chunk_size):
                    chunk = users_df_clean.iloc[i:i + chunk_size]
                    
                    try:
                        # Build column list
                        columns = list(chunk.columns)
                        
                        # Build UPDATE SET clause for ON CONFLICT (exclude id from updates)
                        update_clauses = []
                        for col in columns:
                            if col != 'id':  # Don't update the primary key
                                update_clauses.append(f'"{col}" = EXCLUDED."{col}"')
                        
                        update_set = ", ".join(update_clauses)
                        
                        # Build the upsert query with proper quoting
                        quoted_columns = [f'"{col}"' for col in columns]
                        # CORRECTION: Utiliser des placeholders simples, pas doubles
                        placeholder_columns = [f':{col}' for col in columns]  # Changed from %() to :
                        
                        upsert_query = f"""
                        INSERT INTO "{self.table_name}" ({', '.join(quoted_columns)})
                        VALUES ({', '.join(placeholder_columns)})
                        ON CONFLICT (id) 
                        DO UPDATE SET {update_set}
                        """
                        
                        print(f"Debug - Upsert query: {upsert_query[:200]}...")  # Debug output
                        
                        # Execute for each row in chunk
                        for _, row in chunk.iterrows():
                            row_dict = row.to_dict()
                            
                            # Final cleanup of values for PostgreSQL
                            for key, value in row_dict.items():
                                if pd.isna(value) or value == 'None' or value == 'nan':
                                    row_dict[key] = None
                                elif isinstance(value, pd.Timestamp):
                                    # Convert pandas Timestamp to Python datetime
                                    row_dict[key] = value.to_pydatetime()
                            
                            print(f"Debug - Executing with data: {list(row_dict.keys())}")  # Debug
                            conn.execute(text(upsert_query), row_dict)
                            
                        self.load_report['successful_loads'] += len(chunk)
                        print(f"✓ Processed chunk {i//chunk_size + 1}: {len(chunk)} records")
                        
                    except Exception as chunk_error:
                        error_msg = f"Error in chunk {i//chunk_size + 1}: {str(chunk_error)}"
                        print(f"❌ Chunk error: {error_msg}")
                        self.load_report['errors'].append({
                            'method': 'upsert',
                            'error': str(chunk_error),
                            'record_count': len(chunk),
                            'chunk': i//chunk_size + 1
                        })
                        self.load_report['failed_loads'] += len(chunk)
                        raise  # Re-raise to rollback transaction
            
            # Update report
            self.load_report['total_processed'] = total_records
            self.load_report['success_rate'] = (self.load_report['successful_loads'] / total_records) * 100 if total_records > 0 else 0
            
            print(f"✅ Upsert completed successfully: {self.load_report['successful_loads']}/{total_records} records")
            return self.load_report['successful_loads'] > 0
            
        except Exception as e:
            error_msg = f"Error during upsert: {str(e)}"
            print(error_msg)
            self.load_report['errors'].append({
                'method': 'upsert',
                'error': str(e),
                'record_count': len(users_df)
            })
            return False
    
    def get_users_count(self) -> int:
        """
        Retourne le nombre d'utilisateurs dans la table
        """
        try:
            with self.engine.connect() as conn:
                # IMPORTANT: Utiliser des guillemets pour préserver la casse
                result = conn.execute(text(f'SELECT COUNT(*) FROM "{self.table_name}"'))
                count = result.scalar()
            return count
        except Exception as e:
            print(f"Error getting users count: {e}")
            return 0
    
    def validate_data_integrity(self) -> Dict[str, Any]:
        """
        Valide l'intégrité des données chargées
        """
        try:
            with self.engine.connect() as conn:
                # CORRECTION: Guillemets autour du nom de table
                duplicate_check = conn.execute(text(f"""
                    SELECT COUNT(*) as total_count, COUNT(DISTINCT id) as unique_count
                    FROM "{self.table_name}"
                """)).fetchone()
                
                # Check for invalid emails
                try:
                    invalid_emails = conn.execute(text(f"""
                        SELECT COUNT(*) FROM "{self.table_name}"
                        WHERE email IS NULL OR email = '' OR email NOT LIKE '%@%'
                    """)).scalar()
                except:
                    invalid_emails = "N/A (email column not found)"
                
                return {
                    'total_records': duplicate_check[0],
                    'unique_records': duplicate_check[1],
                    'has_duplicates': duplicate_check[0] != duplicate_check[1],
                    'invalid_emails': invalid_emails
                }
        except Exception as e:
            print(f"Error validating data integrity: {e}")
            return {}
    
    def close_connection(self):
        """
        Ferme la connexion SQLAlchemy
        """
        if self.engine:
            self.engine.dispose()
            print("Database connection closed")