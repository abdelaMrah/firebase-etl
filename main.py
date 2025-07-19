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
    
    try:
        # Initialize services
        print("=== Initializing Services ===")
        firebase_service = FirebaseUserService()
        transformer_service = UserTransformerService()
        postgres_service = PostgreSQLLoaderService()
        
        # Debug database structure (optional)
        firebase_service.debug_database_structure()
        
        print("\n=== Extracting raw users from Firebase ===")
        
        # Get all users as raw data
        raw_users_df = firebase_service.get_all_users_raw()
        
        print(f"Raw users extracted: {len(raw_users_df)}")
        
        if not raw_users_df.empty:
            print(f"\n=== Raw Data Info ===")
            print(f"Columns: {raw_users_df.columns.tolist()}")
            print(f"Data types: {raw_users_df.dtypes.to_dict()}")
            
            # Export raw data for backup
            raw_backup_file = firebase_service.export_raw_data()
            print(f"Raw data backed up to: {raw_backup_file}")
            
            # Show sample (first few rows)
            print(f"\n=== Sample Raw Data ===")
            print(raw_users_df.head())
            
            # ====== TRANSFORMATION PHASE ======
            print(f"\n=== Starting Data Transformation ===")
            
            # Step 1: Validate required fields
            validation_result = transformer_service.validate_required_fields(raw_users_df)
            print(f"📋 Field validation result: {validation_result}")
            
            if not validation_result['is_valid']:
                print("❌ Data validation failed!")
                if validation_result['missing_required_fields']:
                    print(f"Missing required fields: {validation_result['missing_required_fields']}")
                if validation_result['null_values_in_required_fields']:
                    print(f"Null values in required fields: {validation_result['null_values_in_required_fields']}")
                
                # Try to fix missing required fields
                if 'id' in validation_result['missing_required_fields']:
                    print("🔧 Generating missing IDs...")
                    raw_users_df['id'] = raw_users_df.apply(lambda row: row.get('uid') or str(uuid.uuid4())[:20], axis=1)
                
                if 'email' in validation_result['missing_required_fields']:
                    print("❌ Cannot proceed without email field")
                    return
            
            # Step 2: Transform users with deduplication
            print(f"\n🔄 Transforming {len(raw_users_df)} users...")
            transformed_users_df = transformer_service.transform_users_dataframe(
                raw_users_df, 
                remove_duplicates=True
            )
            
            print(f"✅ Transformation completed!")
            print(f"Transformed users: {len(transformed_users_df)}")
            
            # Step 3: Get transformation report
            transformation_report = transformer_service.get_transformation_report()
            print(f"\n=== Transformation Report ===")
            print(f"✅ Successful transformations: {transformation_report['successful_transformations']}")
            print(f"❌ Failed transformations: {transformation_report['failed_transformations']}")
            print(f"📊 Success rate: {transformation_report['success_rate']:.2f}%")
            
            if transformation_report['deduplication_stats']:
                dedup_stats = transformation_report['deduplication_stats']
                print(f"🧹 Duplicates removed: {dedup_stats.get('removed_count', 0)}")
                if dedup_stats.get('duplicates_found', 0) > 0:
                    print(f"🔍 Duplicate details: {dedup_stats.get('unique_duplicate_values', 0)} unique duplicate values")
            
            # Show transformation errors if any
            if transformation_report['errors']:
                print(f"\n⚠️  Transformation Errors ({len(transformation_report['errors'])}):")
                for i, error in enumerate(transformation_report['errors'][:5]):  # Show first 5 errors
                    print(f"  {i+1}. User ID: {error['user_id']} - Error: {error['error']}")
                if len(transformation_report['errors']) > 5:
                    print(f"  ... and {len(transformation_report['errors']) - 5} more errors")
            
            # Step 4: Show transformed data info
            if not transformed_users_df.empty:
                print(f"\n=== Transformed Data Info ===")
                print(f"Columns: {transformed_users_df.columns.tolist()}")
                print(f"Data types: {transformed_users_df.dtypes.to_dict()}")
                
                # Show sample transformed data
                print(f"\n=== Sample Transformed Data ===")
                print(transformed_users_df.head())
                
                # Step 5: Export transformed data
                print(f"\n📦 Exporting transformed data...")
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                transformed_filename = f"transformed_users_{timestamp}.csv"
                
                export_success = transformer_service.export_transformed_users(
                    transformed_users_df, 
                    transformed_filename
                )
                
                if export_success:
                    print(f"✅ Transformed data exported successfully")
                
                # ====== LOADING PHASE ======
                print(f"\n=== Starting Data Loading to PostgreSQL ===")
                
                # Step 6: Load to PostgreSQL
                try:
                    # Initialize PostgreSQL service here if not already done
                    if 'postgres_service' not in locals():
                        postgres_service = PostgreSQLLoaderService()
                    
                    # Check table info
                    table_info = postgres_service.get_table_info()
                    print(f"📋 Target table has {table_info.get('row_count', 0)} existing records")
                    
                    # Check if we need to generate new IDs for conflicts
                    existing_ids = postgres_service.get_existing_user_ids()
                    print(f"Found {len(existing_ids)} existing users in PostgreSQL")
                    
                    # Generate new IDs for any conflicts
                    conflict_count = 0
                    for idx, row in transformed_users_df.iterrows():
                        if row['id'] in existing_ids:
                            new_id = generate_new_unique_id(existing_ids)
                            transformed_users_df.at[idx, 'id'] = new_id
                            existing_ids.append(new_id)
                            conflict_count += 1
                    
                    if conflict_count > 0:
                        print(f"🔧 Resolved {conflict_count} ID conflicts by generating new IDs")
                    
                    # Load users to PostgreSQL
                    load_result = postgres_service.load_users_dataframe(transformed_users_df)
                    
                    if load_result['success']:
                        print(f"✅ Successfully loaded {load_result['inserted_count']} users to PostgreSQL")
                        if load_result['failed_count'] > 0:
                            print(f"⚠️  {load_result['failed_count']} users failed to load")
                            
                            # Show some failed user errors
                            if load_result['errors']:
                                print(f"\n⚠️  Loading Errors (first 3):")
                                for i, error in enumerate(load_result['errors'][:3]):
                                    print(f"  {i+1}. User ID: {error['user_id']} - Error: {error['error']}")
                            
                        # Show load statistics
                        print(f"\n=== Loading Summary ===")
                        print(f"📊 Total processed: {load_result['total_processed']}")
                        print(f"✅ Successfully inserted: {load_result['inserted_count']}")
                        print(f"❌ Failed insertions: {load_result['failed_count']}")
                        print(f"📈 Success rate: {(load_result['inserted_count'] / load_result['total_processed']) * 100:.2f}%")
                        
                        # Get final database stats
                        final_stats = postgres_service.get_user_stats()
                        print(f"\n=== Final Database Stats ===")
                        print(f"📊 Total users in database: {final_stats.get('total_users', 0)}")
                        if final_stats.get('provider_distribution'):
                            print(f"📊 Provider distribution: {final_stats['provider_distribution']}")
                        
                    else:
                        print(f"❌ Loading failed: {load_result['error']}")
                        
                except Exception as e:
                    print(f"❌ Error during loading phase: {e}")
                    import traceback
                    traceback.print_exc()

            else:
                print("❌ No users were successfully transformed")
                
        else:
            print("❌ No users found. Check your Firebase configuration.")
            
    except Exception as e:
        print(f"❌ Error in main execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()