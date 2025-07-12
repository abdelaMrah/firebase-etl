import pandas as pd
import psycopg2
from psycopg2 import sql
from typing import Dict, Any
import logging
from urllib.parse import urlparse
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
log_level = os.getenv('LOG_LEVEL', 'INFO')
logging.basicConfig(level=getattr(logging, log_level))
logger = logging.getLogger(__name__)

def parse_database_url(database_url: str) -> Dict[str, Any]:
    """
    Parse une URL de base de données PostgreSQL
    
    Args:
        database_url: URL de connexion (ex: postgresql://user:pass@host:port/db)
        
    Returns:
        Dictionnaire avec les paramètres de connexion
    """
    parsed = urlparse(database_url)
    
    return {
        'host': parsed.hostname,
        'port': parsed.port or 5432,
        'database': parsed.path.lstrip('/'),
        'user': parsed.username,
        'password': parsed.password
    }

def connect_and_extract_users(db_config: Dict[str, Any]) -> pd.DataFrame:
    """
    Connexion à PostgreSQL et extraction des utilisateurs
    
    Args:
        db_config: Dictionnaire contenant les paramètres de connexion
        
    Returns:
        DataFrame contenant les données des utilisateurs
    """
    connection = None
    try:
        # Connexion à la base de données
        connection = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        
        logger.info("Connexion réussie à la base de données")
        
        # Requête SQL pour récupérer les utilisateurs
        query = """
        SELECT *
        FROM "User"
        """
        
        # Exécution de la requête et conversion en DataFrame
        df = pd.read_sql_query(query, connection)
        logger.info(f"Extraction réussie : {len(df)} utilisateurs récupérés")
        
        return df
        
    except psycopg2.Error as e:
        logger.error(f"Erreur PostgreSQL : {e}")
        raise
    except Exception as e:
        logger.error(f"Erreur générale : {e}")
        raise
    finally:
        if connection:
            connection.close()
            logger.info("Connexion fermée")

def execute_custom_query(db_config: Dict[str, Any], query: str) -> pd.DataFrame:
    """
    Exécute une requête SQL personnalisée
    
    Args:
        db_config: Paramètres de connexion
        query: Requête SQL à exécuter
        
    Returns:
        DataFrame avec les résultats
    """
    connection = None
    try:
        connection = psycopg2.connect(**db_config)
        df = pd.read_sql_query(query, connection)
        return df
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution de la requête : {e}")
        raise
    finally:
        if connection:
            connection.close()

def get_table_info(db_config: Dict[str, Any], table_name: str) -> pd.DataFrame:
    """
    Récupère les informations sur une table (colonnes, types, etc.)
    
    Args:
        db_config: Paramètres de connexion
        table_name: Nom de la table
        
    Returns:
        DataFrame avec les informations de la table
    """
    query = """
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_name = %s
    ORDER BY ordinal_position
    """
    
    connection = None
    try:
        connection = psycopg2.connect(**db_config)
        df = pd.read_sql_query(query, connection, params=[table_name])
        return df
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des infos table : {e}")
        raise
    finally:
        if connection:
            connection.close()

def test_connection(db_config: Dict[str, Any]) -> bool:
    """
    Test la connexion à la base de données
    
    Args:
        db_config: Paramètres de connexion
        
    Returns:
        True si la connexion réussit, False sinon
    """
    try:
        connection = psycopg2.connect(**db_config)
        connection.close()
        return True
    except Exception as e:
        logger.error(f"Test de connexion échoué : {e}")
        return False

def list_databases(db_config: Dict[str, Any]) -> pd.DataFrame:
    """
    Liste toutes les bases de données disponibles
    
    Args:
        db_config: Paramètres de connexion
        
    Returns:
        DataFrame avec la liste des bases de données
    """
    query = "SELECT datname FROM pg_database WHERE datistemplate = false"
    return execute_custom_query(db_config, query)

def display_users_formatted(df: pd.DataFrame) -> None:
    """
    Affiche les utilisateurs dans un format lisible
    
    Args:
        df: DataFrame contenant les utilisateurs
    """
    print(f"📊 LISTE DES UTILISATEURS ({len(df)} utilisateurs)")
    print("="*80)
    
    if df.empty:
        print("Aucun utilisateur trouvé.")
        return
    
    # Affichage formaté
    for index, user in df.iterrows():
        print(f"👤 Utilisateur #{index + 1}")
        print("-" * 40)
        for col in df.columns:
            value = user[col]
            if pd.isna(value):
                value = "N/A"
            print(f"   {col}: {value}")
        print()

def get_connection(db_config: Dict[str, Any]):
    """
    Crée une connexion à la base de données
    
    Args:
        db_config: Paramètres de connexion
        
    Returns:
        Connexion PostgreSQL
    """
    return psycopg2.connect(**db_config)

def prepare_users_data_with_cursor(db_config: Dict[str, Any]) -> pd.DataFrame:
    """
    Utilise un cursor pour préparer les données utilisateurs
    
    Args:
        db_config: Paramètres de connexion
        
    Returns:
        DataFrame avec les données préparées
    """
    connection = None
    cursor = None
    try:
        connection = get_connection(db_config)
        cursor = connection.cursor()
        
        # Requête pour récupérer les utilisateurs
        query = """
        SELECT *
        FROM "User"
        ORDER BY id
        """
        
        cursor.execute(query)
        
        # Récupération des noms de colonnes
        columns = [desc[0] for desc in cursor.description]
        
        # Récupération des données
        rows = cursor.fetchall()
        
        # Création du DataFrame
        df = pd.DataFrame(rows, columns=columns)
        
        logger.info(f"Données préparées avec cursor : {len(df)} lignes")
        
        return df
        
    except Exception as e:
        logger.error(f"Erreur lors de la préparation des données : {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def execute_batch_operations(db_config: Dict[str, Any], operations: list) -> None:
    """
    Exécute plusieurs opérations en lot avec cursor
    
    Args:
        db_config: Paramètres de connexion
        operations: Liste des requêtes SQL à exécuter
    """
    connection = None
    cursor = None
    try:
        connection = get_connection(db_config)
        cursor = connection.cursor()
        
        for operation in operations:
            cursor.execute(operation)
            print(f"✅ Opération exécutée : {operation[:50]}...")
        
        connection.commit()
        print("✅ Toutes les opérations ont été committées")
        
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error(f"Erreur lors des opérations batch : {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def get_table_stats_with_cursor(db_config: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    """
    Récupère les statistiques d'une table avec cursor
    
    Args:
        db_config: Paramètres de connexion
        table_name: Nom de la table
        
    Returns:
        Dictionnaire avec les statistiques
    """
    connection = None
    cursor = None
    try:
        connection = get_connection(db_config)
        cursor = connection.cursor()
        
        # Nombre de lignes
        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        row_count = cursor.fetchone()[0]
        
        # Informations sur les colonnes
        cursor.execute(f'SELECT * FROM "{table_name}" LIMIT 0')
        columns = [desc[0] for desc in cursor.description]
        
        # Taille de la table
        cursor.execute(f"""
        SELECT pg_size_pretty(pg_total_relation_size('"{table_name}"'))
        """)
        table_size = cursor.fetchone()[0]
        
        return {
            'row_count': row_count,
            'columns': columns,
            'column_count': len(columns),
            'table_size': table_size
        }
        
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des stats : {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def prepare_data_for_analysis(db_config: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    """
    Prépare les données pour l'analyse avec cursor
    
    Args:
        db_config: Paramètres de connexion
        table_name: Nom de la table
        
    Returns:
        Dictionnaire avec les données préparées
    """
    connection = None
    cursor = None
    try:
        connection = get_connection(db_config)
        cursor = connection.cursor()
        
        # Échantillon de données
        cursor.execute(f'SELECT * FROM "{table_name}" LIMIT 100')
        sample_data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        # Données complètes
        cursor.execute(f'SELECT * FROM "{table_name}"')
        all_data = cursor.fetchall()
        
        # Statistiques
        stats = get_table_stats_with_cursor(db_config, table_name)
        
        return {
            'sample_df': pd.DataFrame(sample_data, columns=columns),
            'full_df': pd.DataFrame(all_data, columns=columns),
            'stats': stats,
            'columns': columns
        }
        
    except Exception as e:
        logger.error(f"Erreur lors de la préparation pour l'analyse : {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def get_db_config_from_env() -> Dict[str, Any]:
    """
    Récupère la configuration de la base de données depuis les variables d'environnement
    
    Returns:
        Dictionnaire avec les paramètres de connexion
    """
    database_url = os.getenv('DATABASE_URL')
    
    if database_url:
        return parse_database_url(database_url)
    else:
        return {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', '5432')),
            'database': os.getenv('POSTGRES_DATABASE', 'postgres'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', '')
        }

# Pour tester votre fonction
if __name__ == "__main__":
    print("CONFIGURATION AVEC VARIABLES D'ENVIRONNEMENT")
    print("="*50)
    
    # Configuration depuis .env
    db_config_env = get_db_config_from_env()
    print(f"Configuration depuis .env: {db_config_env}")
    print()
    
    # Configurations à tester
    configs_to_test = [
        {
            'name': 'Configuration depuis .env',
            'config': db_config_env
        },
        {
            'name': 'Configuration manuelle - uniflat database',
            'config': {
                **db_config_env,
                'database': 'uniflat'
            }
        }
    ]
    
    print("Test des configurations...")
    print()
    
    working_config = None
    
    # Test de chaque configuration
    for config_info in configs_to_test:
        print(f"🔄 Test: {config_info['name']}")
        if test_connection(config_info['config']):
            print("✅ Connexion réussie!")
            working_config = config_info['config']
            break
        else:
            print("❌ Connexion échouée")
        print()
    
    if working_config:
        print("="*50)
        print("EXTRACTION DES DONNÉES")
        print("="*50)
        
        try:
            # Lister les bases de données disponibles
            print("Bases de données disponibles:")
            dbs = list_databases(working_config)
            print(dbs)
            print()
            
            # Lister les tables disponibles
            print("Tables disponibles dans la base:")
            tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
            """
            tables_df = execute_custom_query(working_config, tables_query)
            print(tables_df)
            print()
            
            # Tentative d'extraction des utilisateurs
            try:
                users_df = connect_and_extract_users(working_config)
                print(f"Nombre d'utilisateurs : {len(users_df)}")
                
                # Affichage formaté des utilisateurs
                display_users_formatted(users_df)
                
                # Affichage des colonnes disponibles
                print("\n" + "="*50)
                print("COLONNES DISPONIBLES")
                print("="*50)
                print("Colonnes dans la table 'user':")
                for col in users_df.columns:
                    print(f"  - {col}")
                
                # Affichage des premières lignes en format tableau
                print("\n" + "="*50)
                print("DONNÉES BRUTES (premières lignes)")
                print("="*50)
                print(users_df.head())
                
                # Statistiques générales
                print("\n" + "="*50)
                print("STATISTIQUES")
                print("="*50)
                print(f"Nombre total d'utilisateurs: {len(users_df)}")
                print(f"Colonnes: {list(users_df.columns)}")
                print(f"Types de données:")
                print(users_df.dtypes)
                
            except Exception as e:
                print(f"⚠️  Table 'user' non trouvée ou erreur: {e}")
                print("Essayons de trouver les tables avec 'user' dans le nom...")
                
                # Recherche de tables contenant 'user'
                user_tables_query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE '%user%'
                ORDER BY table_name
                """
                try:
                    user_tables = execute_custom_query(working_config, user_tables_query)
                    print("Tables contenant 'user':")
                    print(user_tables)
                except Exception as e2:
                    print(f"Erreur lors de la recherche de tables: {e2}")
                
                # Requête générique pour tester
                test_query = "SELECT version() as postgresql_version"
                version_df = execute_custom_query(working_config, test_query)
                print("Version PostgreSQL:")
                print(version_df)
                
        except Exception as e:
            print(f"Erreur lors de l'extraction : {e}")
    else:
        print("❌ AUCUNE CONFIGURATION N'A FONCTIONNÉ")
        print("\nVérifiez votre fichier .env:")
        print("="*50)
        print("1. Assurez-vous que le fichier .env existe")
        print("2. Vérifiez les variables d'environnement:")
        print(f"   DATABASE_URL: {os.getenv('DATABASE_URL', 'Non définie')}")
        print(f"   POSTGRES_HOST: {os.getenv('POSTGRES_HOST', 'Non définie')}")
        print(f"   POSTGRES_PORT: {os.getenv('POSTGRES_PORT', 'Non définie')}")
        print(f"   POSTGRES_DATABASE: {os.getenv('POSTGRES_DATABASE', 'Non définie')}")
        print(f"   POSTGRES_USER: {os.getenv('POSTGRES_USER', 'Non définie')}")
        print("3. Installez python-dotenv : pip install python-dotenv")
        
    print("\n" + "="*50)
    print("CONSEILS D'UTILISATION")
    print("="*50)
    print("1. Installez les dépendances :")
    print("   pip install psycopg2-binary pandas python-dotenv")
    print("2. Créez un fichier .env dans le répertoire du projet")
    print("3. Configurez vos variables d'environnement dans .env")
    print("4. Le fichier .env ne doit PAS être committé dans Git")
    print("5. Ajoutez .env dans votre .gitignore")
    
    if working_config:
        try:
            # Tentative d'extraction des utilisateurs avec cursor
            print("🔄 Extraction avec cursor...")
            users_df = prepare_users_data_with_cursor(working_config)
            print(f"Nombre d'utilisateurs : {len(users_df)}")
            
            # Affichage formaté des utilisateurs
            display_users_formatted(users_df)
            
            # Préparation des données pour l'analyse
            print("\n" + "="*50)
            print("PRÉPARATION DES DONNÉES POUR L'ANALYSE")
            print("="*50)
            
            try:
                analysis_data = prepare_data_for_analysis(working_config, "User")
                
                print("📊 Statistiques de la table:")
                stats = analysis_data['stats']
                print(f"  - Nombre de lignes: {stats['row_count']}")
                print(f"  - Nombre de colonnes: {stats['column_count']}")
                print(f"  - Taille de la table: {stats['table_size']}")
                print(f"  - Colonnes: {', '.join(stats['columns'])}")
                
                print("\n📋 Échantillon des données (10 premières lignes):")
                print(analysis_data['sample_df'].head(10))
                
                print("\n📈 Types de données:")
                print(analysis_data['full_df'].dtypes)
                
                print("\n📊 Statistiques descriptives:")
                print(analysis_data['full_df'].describe())
                
            except Exception as e:
                print(f"⚠️ Erreur lors de l'analyse : {e}")
            
        except Exception as e:
            print(f"⚠️  Table 'User' non trouvée ou erreur: {e}")
            print("Essayons de trouver les tables avec 'user' dans le nom...")
            
            # Recherche de tables contenant 'user'
            user_tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND (table_name ILIKE '%user%' OR table_name ILIKE '%User%')
            ORDER BY table_name
            """
            try:
                user_tables = execute_custom_query(working_config, user_tables_query)
                print("Tables contenant 'user':")
                print(user_tables)
                
                # Test avec chaque table trouvée
                for _, row in user_tables.iterrows():
                    table_name = row['table_name']
                    print(f"\n🔍 Test avec la table '{table_name}':")
                    try:
                        stats = get_table_stats_with_cursor(working_config, table_name)
                        print(f"  ✅ Table '{table_name}' - {stats['row_count']} lignes")
                        
                        # Essai d'extraction
                        sample_query = f'SELECT * FROM "{table_name}" LIMIT 3'
                        sample_df = execute_custom_query(working_config, sample_query)
                        print(f"  📋 Échantillon:")
                        print(sample_df)
                        
                    except Exception as e3:
                        print(f"  ❌ Erreur avec '{table_name}': {e3}")
                        
            except Exception as e2:
                print(f"Erreur lors de la recherche de tables: {e2}")