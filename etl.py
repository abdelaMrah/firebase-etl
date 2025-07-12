import psycopg2
import pandas as pd
from typing import Dict, Any, Optional
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PostgreSQLETL:
    def __init__(self, source_config: Dict[str, Any], target_config: Dict[str, Any]):
        """
        Initialise l'ETL avec les configurations des bases de données source et cible.
        
        Args:
            source_config: Configuration de la DB source (host, port, database, user, password)
            target_config: Configuration de la DB cible (host, port, database, user, password)
        """
        self.source_config = source_config
        self.target_config = target_config
        
    def extract_data(self, query: str) -> pd.DataFrame:
        """
        Extrait les données de la base de données source.
        
        Args:
            query: Requête SQL pour extraire les données
            
        Returns:
            DataFrame contenant les données extraites
        """
        try:
            # Connexion à la base de données source
            conn = psycopg2.connect(**self.source_config)
            logger.info("Connexion établie avec la base de données source")
            
            # Extraction des données
            df = pd.read_sql_query(query, conn)
            logger.info(f"Données extraites: {len(df)} lignes")
            
            conn.close()
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction: {e}")
            raise
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforme les données (fonction basique qui retourne les données d'entrée).
        
        Args:
            df: DataFrame contenant les données à transformer
            
        Returns:
            DataFrame transformé (identique à l'entrée pour l'instant)
        """
        logger.info("Transformation des données (aucune transformation appliquée)")
        return df
    
    def load_data(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace') -> None:
        """
        Charge les données dans la base de données cible.
        
        Args:
            df: DataFrame contenant les données à charger
            table_name: Nom de la table cible
            if_exists: Action si la table existe ('fail', 'replace', 'append')
        """
        try:
            # Connexion à la base de données cible
            conn = psycopg2.connect(**self.target_config)
            logger.info("Connexion établie avec la base de données cible")
            
            # Chargement des données
            df.to_sql(table_name, conn, if_exists=if_exists, index=False, method='multi')
            logger.info(f"Données chargées: {len(df)} lignes dans la table '{table_name}'")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement: {e}")
            raise
    
    def run_etl(self, extract_query: str, target_table: str, if_exists: str = 'replace') -> None:
        """
        Exécute le processus ETL complet.
        
        Args:
            extract_query: Requête SQL pour extraire les données
            target_table: Nom de la table cible
            if_exists: Action si la table existe ('fail', 'replace', 'append')
        """
        logger.info("Début du processus ETL")
        
        # Extract
        df = self.extract_data(extract_query)
        
        # Transform
        df_transformed = self.transform_data(df)
        
        # Load
        self.load_data(df_transformed, target_table, if_exists)
        
        logger.info("Processus ETL terminé avec succès")

# Exemple d'utilisation
if __name__ == "__main__":
    # Configuration des bases de données
    source_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'source_db',
        'user': 'username',
        'password': 'password'
    }
    
    target_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'target_db',
        'user': 'username',
        'password': 'password'
    }
    
    # Création de l'instance ETL
    etl = PostgreSQLETL(source_config, target_config)
    
    # Requête d'extraction
    query = "SELECT * FROM ma_table_source"
    
    # Exécution de l'ETL
    etl.run_etl(query, "ma_table_cible", if_exists='replace')