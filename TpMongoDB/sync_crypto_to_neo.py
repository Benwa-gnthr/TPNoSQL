import os
from pymongo import MongoClient
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

# 1. Connexion MongoDB
mongo_client = MongoClient(os.getenv("MONGO_URI"))
db_mongo = mongo_client["crypto_data"] # Vérifie le nom de ta db
col_clean = db_mongo["market_cap_clean"]

# 2. Connexion Neo4j
neo4j_uri = os.getenv("NEO4J_URI") # URL neo4j+s://...
neo4j_user = "neo4j"
neo4j_password = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

def sync_data():
    data = list(col_clean.find())
    
    with driver.session() as session:
        # On nettoie le graphe avant de remplir pour éviter les doublons
        session.run("MATCH (n) DETACH DELETE n")
        
        for coin in data:
            # Requête Cypher pour créer les nœuds et les liens
            query = """
            MERGE (c:Crypto {symbole: $symbole})
            SET c.nom = $nom, c.prix = $prix
            
            MERGE (cat:Categorie {nom: $categorie})
            MERGE (c)-[:APPARTIENT_A]->(cat)
            
            MERGE (t:Tendance {type: $tendance})
            MERGE (c)-[:A_POUR_ETAT]->(t)
            """
            session.run(query, 
                symbole=coin["symbole"],
                nom=coin["nom"],
                prix=coin["prix_usd"],
                categorie=coin["categorie"],
                tendance=coin["tendance"]
            )
            
    print(f"✅ {len(data)} cryptos synchronisées vers Neo4j !")

if __name__ == "__main__":
    sync_data()
    driver.close()