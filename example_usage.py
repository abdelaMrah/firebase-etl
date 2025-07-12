from firebase_service import FirebaseUserService

def main():
    # Initialize the Firebase service
    service = FirebaseUserService()
    
    # Extract all users
    print("=== Extracting all users ===")
    # users = service.get_all_users()
    # print(f"Total users found: {len(users)}")
    
 
     
    # service.export_users_to_json('extracted_users.json')
    
    # Example: Get a specific user by ID (replace with actual ID)
    # if users:
    #     first_user_id = users[0]['id']
    #     print(f"\n=== Getting user by ID: {first_user_id} ===")
    #     user = service.get_user_by_id(first_user_id)
    #     if user:
    #         print(f"Found user: {user}")

    users = service.get_all_users_raw()
    print(f"\nTotal users found (raw): {len(users)}")
    print(users.head())  # Display first few rows of the DataFrame
    print("count of users:", users.shape[0])
    
if __name__ == "__main__":
    main()
