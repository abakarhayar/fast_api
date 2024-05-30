import os
from dotenv import load_dotenv
import sshtunnel
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

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

class DatabaseConnection:
    def __init__(self):
        self.tunnel = None
        self.engine = None
        self.Session = None

    def start_tunnel(self):
        self.tunnel = sshtunnel.SSHTunnelForwarder(
            (ssh_host),
            ssh_username=ssh_username, ssh_password=ssh_password,
            remote_bind_address=(db_host, 3306)
        )
        self.tunnel.start()

    def setup_engine(self):
        database_url = f"mysql://{db_user}:{db_password}@127.0.0.1:{self.tunnel.local_bind_port}/{db_name}"
        self.engine = create_engine(database_url)
        self.Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=self.engine))

    def get_session(self):
        return self.Session()

    def close(self):
        if self.Session:
            self.Session.remove()
        if self.tunnel:
            self.tunnel.stop()

db_conn = DatabaseConnection()
db_conn.start_tunnel()
db_conn.setup_engine()

def get_metadata():
    session = db_conn.get_session()
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
            "session": session
        }
    except Exception as e:
        session.close()
        raise e
