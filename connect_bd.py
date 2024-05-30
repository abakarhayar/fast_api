# connect_bd.py
import os
from dotenv import load_dotenv
import sshtunnel
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import sessionmaker

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

sshtunnel.SSH_TIMEOUT = 10
sshtunnel.TUNNEL_TIMEOUT = 10

db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_name = os.getenv('DB_NAME')
ssh_host = os.getenv('SSH_HOST')
ssh_username = os.getenv('SSH_USERNAME')
ssh_password = os.getenv('SSH_PASSWORD')

def get_db_session():
    try:
        tunnel = sshtunnel.SSHTunnelForwarder(
            (ssh_host),
            ssh_username=ssh_username, ssh_password=ssh_password,
            remote_bind_address=(db_host, 3306)
        )
        tunnel.start()
        database_url = f"mysql://{db_user}:{db_password}@127.0.0.1:{tunnel.local_bind_port}/{db_name}"
        engine = create_engine(database_url)
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session = Session()
        return session, tunnel
    except Exception as e:
        print("Erreur:", e)
        raise

def get_metadata():
    session, tunnel = get_db_session()
    try:
        metadata1 = MetaData()
        metadata1.reflect(bind=session.bind, only=['olympic_hosts'])
        olympic_hosts = Table('olympic_hosts', metadata1, autoload_with=session.bind)

        metadata2 = MetaData()
        metadata2.reflect(bind=session.bind, only=['olympic_athletes'])
        olympic_athletes = Table('olympic_athletes', metadata2, autoload_with=session.bind)

        return {
            "olympic_hosts": olympic_hosts,
            "olympic_athletes": olympic_athletes,
            "session": session,
            "tunnel": tunnel
        }
    except Exception as e:
        session.close()
        tunnel.stop()
        raise e
