import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from firebase_admin import auth
import json
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, Dict, List

@dataclass
class IUser:
    id: str
    name: str
    email: Optional[str] = None
    profilePic: Optional[str] = None
    following: Dict[str, bool] = field(default_factory=dict)
    interests: List[str] = field(default_factory=list)

# Charger les identifiants d'administration
cred = credentials.Certificate("./service-account.json")

# Initialiser l'application Firebase Admin avec l'URL de Regitaltime Database
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://kastudio-6a436-default-rtdb.firebaseio.com/'
})

# D'abord, vérifions ce qui existe à la racine de la base de données
print("Vérification du contenu de la base de données...")
root_ref = db.reference('/Users')
try:
    root_data = root_ref.get()
    if root_data:
        print("Données trouvées à la racine :")
        print(f"Clés disponibles : {list(root_data.keys())}")
        print(f"Structure complète : {root_data}")
        
        # Sauvegarder toutes les données dans un fichier JSON
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"firebase_data_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(root_data, f, indent=2, ensure_ascii=False)
        
        print(f"\nToutes les données ont été sauvegardées dans : {filename}")
        
    else:
        print("Aucune donnée trouvée à la racine de la base de données.")
except Exception as e:
    print(f"Erreur lors de la lecture de la racine : {e}")

print("\n" + "="*50 + "\n")

# Maintenant, essayons d'accéder au nœud "Users" avec gestion d'erreur
try:
    ref = db.reference('/Users')
    users_data = ref.get()
    
    if users_data:
        print("Liste des utilisateurs :\n")
        
        # Créer un dictionnaire d'utilisateurs typés
        users: Dict[str, IUser] = {}
        
        for uid, info in users_data.items():
            # Récupérer l'email depuis la base de données
            email = info.get('email')
            
            # Si pas d'email, essayer de le récupérer depuis Firebase Auth
            if not email:
                try:
                    user_record = auth.get_user(uid)
                    email = user_record.email
                    print(f"Email récupéré depuis Auth pour {uid}: {email}")
                except Exception as auth_error:
                    print(f"Impossible de récupérer l'email depuis Auth pour {uid}: {auth_error}")
                    email = None
            
            # Créer une instance IUser avec les données Firebase
            user = IUser(
                id=uid,
                name=info.get('name', ''),
                email=email,
                profilePic=info.get('profilePic'),
                following=info.get('following', {}),
                interests=info.get('interests', [])
            )
            users[uid] = user
            
            # Afficher les informations de l'utilisateur
            print(f"UID: {user.id}")
            print(f"  Nom: {user.name}")
            print(f"  Email: {user.email}")
            print(f"  Photo de profil: {user.profilePic}")
            print(f"  Suivis: {user.following}")
            print(f"  Intérêts: {user.interests}")
            print()
        
        # Sauvegarder les données brutes
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        users_filename = f"users_data_{timestamp}.json"
        
        with open(users_filename, 'w', encoding='utf-8') as f:
            json.dump(users_data, f, indent=2, ensure_ascii=False)
        
        print(f"Les données des utilisateurs ont été sauvegardées dans : {users_filename}")
        
        # Sauvegarder les utilisateurs typés (convertis en dict pour JSON)
        typed_users_filename = f"typed_users_{timestamp}.json"
        users_dict = {uid: {
            'id': user.id,
            'name': user.name,
            'email': user.email,
            'profilePic': user.profilePic,
            'following': user.following,
            'interests': user.interests
        } for uid, user in users.items()}
        
        with open(typed_users_filename, 'w', encoding='utf-8') as f:
            json.dump(users_dict, f, indent=2, ensure_ascii=False)
        
        print(f"Les utilisateurs typés ont été sauvegardés dans : {typed_users_filename}")
        print(f"Nombre total d'utilisateurs traités: {len(users)}")
        
    else:
        print("Le nœud 'Users' existe mais est vide.")
        
except Exception as e:
    print(f"Erreur : {e}")
    print("Le nœud 'Users' n'existe pas encore dans la base de données.")
    print("Script terminé en mode lecture seule.")
