from firebase_service import FirebaseUserService
from user_transformer import UserTransformerService
from postgres_loader import PostgreSQLLoaderService
import os
import uuid
from dotenv import load_dotenv
from sqlalchemy import text

def generate_new_unique_id(existing_ids: list) -> str:
    """
    Génère un nouvel ID unique qui n'existe pas dans la liste des IDs existants
    """
    while True:
        new_id = str(uuid.uuid4())[:20]  # Génère un ID de 20 caractères
        if new_id not in existing_ids:
            return new_id

def main():
    # Load environment variables from .env file
    load_dotenv()
    
    # Initialize services
    firebase_service = FirebaseUserService()
    transformer_service = UserTransformerService()
    
    # Configuration pour votre base existante - utilise les bonnes variables d'environnement
    postgres_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5434)),  # Note: port 5434 selon votre .env
        'database': os.getenv('DB_NAME', 'KAStudioDb'),
        'username': os.getenv('DB_USER', 'user'),
        'password': os.getenv('DB_PASSWORD', 'password'),
        'table_name': 'User_clone'  # Nom exact de votre table
    }
    
    # Debug: Print config (remove password for security)
    debug_config = postgres_config.copy()
    debug_config['password'] = '*' * len(debug_config['password']) if debug_config['password'] else 'None'
    print(f"Database config: {debug_config}")
    
    postgres_loader = PostgreSQLLoaderService(**postgres_config)
    
    print("=== Extracting raw users from Firebase ===")
    raw_users_df = firebase_service.get_all_users_raw()
    print(f"Raw users extracted: {len(raw_users_df)}")
    
    if not raw_users_df.empty:
        # Debug: Print available columns
        print(f"\n=== Available columns in Firebase data ===")
        print(f"Columns: {raw_users_df.columns.tolist()}")
        
        print("\n=== Validating data before transformation ===")
        validation_report = transformer_service.validate_required_fields(raw_users_df)
        print(f"Validation report: {validation_report}")
        
        if validation_report['is_valid']:
            print("\n=== Transforming users (with deduplication) ===")
            # La déduplication est maintenant gérée dans le transformer
            transformed_users_df = transformer_service.transform_users_dataframe(
                raw_users_df, 
                remove_duplicates=True  # Active la déduplication
            )
            
            print(f"Transformed users: {len(transformed_users_df)}")
            print("\n=== Transformation Report ===")
            report = transformer_service.get_transformation_report()
            print(f"Success rate: {report['success_rate']:.2f}%")
            print(f"Successful: {report['successful_transformations']}")
            print(f"Failed: {report['failed_transformations']}")
            
            # Afficher les stats de déduplication
            if 'deduplication_stats' in report and report['deduplication_stats']:
                dedup_stats = report['deduplication_stats']
                print(f"\n=== Deduplication Stats ===")
                print(f"Duplicates found: {dedup_stats.get('duplicates_found', 0)}")
                print(f"Records removed: {dedup_stats.get('removed_count', 0)}")
                if dedup_stats.get('deduplication_method'):
                    print(f"Method used: {dedup_stats['deduplication_method']}")
            
            if report['errors']:
                print("\nTransformation errors:")
                for error in report['errors'][:5]:  # Show first 5 errors
                    print(f"- User {error['user_id']}: {error['error']}")
            
            if not transformed_users_df.empty:
                print("\n=== Sample transformed data ===")
                print(transformed_users_df.head())
                
                # Export transformed data
                transformer_service.export_transformed_users(
                    transformed_users_df, 
                    'transformed_users_output.csv'
                )
                
                # NEW: Load data into existing PostgreSQL database
                print("\n=== Loading data into PostgreSQL ===")
                if postgres_loader.connect():
                    # Check if table exists
                    if postgres_loader.check_table_exists():
                        print(f"✓ Table '{postgres_config['table_name']}' found in database")
                        
                        # Get table schema info
                        schema = postgres_loader.get_table_schema()
                        if schema:
                            print(f"✓ Table has {len(schema['columns'])} columns")
                        
                        # Check existing data to avoid conflicts (CHECK BOTH EMAIL AND ID)
                        print("\n=== Checking for existing data conflicts ===")
                        try:
                            with postgres_loader.engine.connect() as conn:
                                # Get existing emails AND IDs
                                existing_data_result = conn.execute(text(f"""
                                    SELECT id, email FROM "{postgres_config['table_name']}"
                                """))
                                existing_data = existing_data_result.fetchall()
                                
                                if existing_data:
                                    existing_emails = [row[1] for row in existing_data]
                                    existing_ids = [row[0] for row in existing_data]
                                    
                                    print(f"✓ Found {len(existing_data)} existing records in database")
                                    print(f"Existing emails: {existing_emails[:5]}...")  # Show first 5
                                    print(f"Existing IDs: {existing_ids[:5]}...")  # Show first 5
                                    
                                    # NOUVELLE LOGIQUE: Gérer les conflits intelligemment
                                    print("\n=== Resolving conflicts intelligently ===")
                                    
                                    new_users_list = []
                                    id_changes = []
                                    
                                    for _, row in transformed_users_df.iterrows():
                                        user_id = row['id']
                                        user_email = row['email']
                                        
                                        # Cas 1: Email ET ID existent déjà -> Skip
                                        if user_email in existing_emails and user_id in existing_ids:
                                            print(f"  - SKIP: ID {user_id} and email {user_email} both exist")
                                            continue
                                        
                                        # Cas 2: Email existe mais pas l'ID -> Skip (éviter doublons email)
                                        elif user_email in existing_emails and user_id not in existing_ids:
                                            print(f"  - SKIP: Email {user_email} already exists")
                                            continue
                                        
                                        # Cas 3: ID existe mais pas l'email -> Générer nouveau ID
                                        elif user_id in existing_ids and user_email not in existing_emails:
                                            old_id = user_id
                                            new_id = generate_new_unique_id(existing_ids + [u['id'] for u in new_users_list])
                                            row_copy = row.copy()
                                            row_copy['id'] = new_id
                                            new_users_list.append(row_copy.to_dict())
                                            id_changes.append({'old_id': old_id, 'new_id': new_id, 'email': user_email})
                                            existing_ids.append(new_id)  # Éviter les conflits avec les nouveaux IDs
                                            print(f"  - ID_CHANGE: {old_id} -> {new_id} for email {user_email}")
                                        
                                        # Cas 4: Ni email ni ID n'existent -> OK
                                        else:
                                            new_users_list.append(row.to_dict())
                                            print(f"  - OK: New user {user_id} with email {user_email}")
                                    
                                    if id_changes:
                                        print(f"\n=== ID Changes Summary ===")
                                        for change in id_changes:
                                            print(f"  - {change['old_id']} -> {change['new_id']} ({change['email']})")
                                    
                                    if new_users_list:
                                        import pandas as pd
                                        transformed_users_df = pd.DataFrame(new_users_list)
                                        print(f"✓ {len(transformed_users_df)} records ready for insertion")
                                    else:
                                        print("ℹ️  No new records to insert after conflict resolution")
                                        postgres_loader.close_connection()
                                        return
                                else:
                                    print("✓ No existing records found, proceeding with all data")
                                    
                        except Exception as e:
                            print(f"Warning: Could not check existing data: {e}")
                            print(f"Proceeding with simple append method...")
                        
                        # Get current count
                        initial_count = postgres_loader.get_users_count()
                        print(f"✓ Current records in table: {initial_count}")
                        
                        # Load data using SQLAlchemy - maintenant avec append simple car les conflits sont résolus
                        print("📦 Loading data using SQLAlchemy...")
                        
                        # Utiliser load_users_with_sqlalchemy au lieu de simple_load_users
                        success = postgres_loader.load_users_with_sqlalchemy(
                            transformed_users_df, 
                            method='append', 
                            chunk_size=500
                        )
                        
                        if success:
                            load_report = postgres_loader.get_load_report()
                            final_count = postgres_loader.get_users_count()
                            
                            print(f"✓ Data loading completed")
                            print(f"  - Records processed: {load_report['total_processed']}")
                            print(f"  - Success rate: {load_report['success_rate']:.2f}%")
                            print(f"  - Final count in database: {final_count}")
                            print(f"  - New records added: {final_count - initial_count}")
                            
                            # Validate data integrity
                            print("\n=== Validating data integrity ===")
                            integrity_report = postgres_loader.validate_data_integrity()
                            print(f"✓ Integrity check completed: {integrity_report}")
                        else:
                            print("❌ Failed to load data")
                            load_report = postgres_loader.get_load_report()
                            if load_report['errors']:
                                print("Load errors:")
                                for error in load_report['errors']:
                                    print(f"  - {error}")
                            
                    else:
                        print(f"❌ Table '{postgres_config['table_name']}' not found in database")
                        print("Available tables:")
                        try:
                            with postgres_loader.engine.connect() as conn:
                                result = conn.execute(text("""
                                    SELECT table_name FROM information_schema.tables 
                                    WHERE table_schema = 'public'
                                    ORDER BY table_name;
                                """))
                                tables = [row[0] for row in result.fetchall()]
                                for table in tables:
                                    print(f"  - {table}")
                        except Exception as e:
                            print(f"  Error listing tables: {e}")
                    
                    postgres_loader.close_connection()
                else:
                    print("❌ Failed to connect to PostgreSQL")
        else:
            print("Data validation failed. Please check the required fields.")
            print("Missing fields:", validation_report.get('missing_required_fields', []))
            print("Null values:", validation_report.get('null_values_in_required_fields', {}))
    else:
        print("No users found in Firebase.")

if __name__ == "__main__":
    main()