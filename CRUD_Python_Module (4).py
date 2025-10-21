from pymongo import MongoClient
from pymongo.errors import PyMongoError
from bson.objectid import ObjectId
from typing import Any, Dict, List, Optional

class AnimalShelter(object):
    """CRUD operations for Animal collection in MongoDB."""

    def __init__(
        self,
        user: str = "aacuser",
        passwd: str = "D@rkLumoo28",
        host: str = "127.0.0.1",
        port: int = 27017,
        db: str = "aac",
        col: str = "animals",
    ):
        # --- Initialize Connection ---
        self.client = MongoClient(
            host=host,
            port=port,
            username=user,
            password=passwd,
            authSource="admin",
            serverSelectionTimeoutMS=3000,
        )

        self.database   = self.client[db]
        self.collection = self.database[col]

    # --- Create (C) ---
    def create(self, data: Dict[str, Any]) -> bool:
        """Insert one document. Returns True on success, raises on bad input."""
        if not isinstance(data, dict) or not data:
            raise ValueError("create(): 'data' must be a non-empty dict.")
        try:
            self.collection.insert_one(data)
            return True
        except PyMongoError as e:
            # In production you’d log e
            return False

    # --- Read (R) ---
    def read(
        self,
        query: Optional[Dict[str, Any]],
        projection: Optional[Dict[str, int]] = None,
    ) -> List[Dict[str, Any]]:
        """Find documents matching query. Returns a list (possibly empty)."""
        if query is None:
            return []
        try:
            return list(self.collection.find(query, projection))
        except PyMongoError:
            return []

    # --- Update (U) ---
    def update(
        self,
        query: Dict[str, Any],
        new_values: Dict[str, Any],
        many: bool = False,
    ) -> int:
        """
        Update documents matching query with $set of new_values.
        Returns the number of documents modified.
        Set many=True to update_many; otherwise update_one.
        """
        if not isinstance(query, dict) or not query:
            raise ValueError("update(): 'query' must be a non-empty dict.")
        if not isinstance(new_values, dict) or not new_values:
            raise ValueError("update(): 'new_values' must be a non-empty dict.")

        try:
            update_doc = {"$set": new_values}
            if many:
                result = self.collection.update_many(query, update_doc)
            else:
                result = self.collection.update_one(query, update_doc)
            return int(result.modified_count)
        except PyMongoError:
            return 0

    # --- Delete (D) ---
    def delete(self, query: Dict[str, Any], many: bool = False) -> int:
        """
        Delete documents matching query.
        Returns the number of documents deleted.
        Set many=True to delete_many; otherwise delete_one.
        """
        if not isinstance(query, dict) or not query:
            raise ValueError("delete(): 'query' must be a non-empty dict.")

        try:
            if many:
                result = self.collection.delete_many(query)
            else:
                result = self.collection.delete_one(query)
            return int(result.deleted_count)
        except PyMongoError:
            return 0