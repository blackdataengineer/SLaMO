from piccolo.conf.apps import AppRegistry
from piccolo.engine.postgres import PostgresEngine
from dotenv import load_dotenv

load_dotenv(verbose = True)

# A list of paths to piccolo apps
# e.g. ['blog.piccolo_app']
APP_REGISTRY = AppRegistry(apps=['db.piccolo_app'])

DB = PostgresEngine(config={
    'host': 'localhost',
    'database': 'slamo',
    'user': 'postgres',
    'password': 'password'
})
