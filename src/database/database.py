from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection

class BackendDatabase:
    def __init__(self) -> None:
        """Creates a Database instance, creates a connection to the Mongo DB
        """
        with open('src/database/db_server.txt', 'r', encoding='utf8') as serverFile:
            serverName = serverFile.read().strip()

            self.client = MongoClient(serverName, 27017)

            self.current_db: Database | None = None

    def set_database(self, db_name: str) -> Database | None:
        """Set the database within the Mongo instance

        Args:
            db_name (str): The name of the database to use

        Returns:
            The database in use
        """
        self.current_db = self.client[db_name]

        return self.current_db

    def get_collection(self, collection_name: str, db_name: str | None = None) -> Collection | None:
        """Gets a collection object given the name of the collection and, optionally, the name of the database

        Args:
            collection_name (str): The name of the collection
            db_name (str | None, optional): Optional database name. Defaults to None.

        Returns:
            The collection or None if it does not exist
        """
        if db_name is not None:
            self.current_db = self.client[db_name]
            return self.current_db[collection_name] if self.current_db is not None else None
        elif self.current_db is not None:
            return self.current_db[collection_name]
        else:
            return None
