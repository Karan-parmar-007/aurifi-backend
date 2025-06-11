# transaction_model.py
from app.utils.db import db
from datetime import datetime
from pymongo.errors import PyMongoError
from bson import ObjectId
from app.utils.logger import logger
from app.utils.timestamps import add_timestamps
from app.models.transaction_version_model import TransactionVersionModel

class TransactionModel:
    """MongoDB model class for handling transaction operations and data management"""
    
    def __init__(self):
        """Initialize the TransactionModel with the 'transactions' collection"""
        self.collection = db["transactions"]
        self.transaction_version_model = TransactionVersionModel()

    def get_transaction(self, transaction_id):
        """Get a transaction by its ID
        
        Args:
            transaction_id (str): ID of the transaction to retrieve
            
        Returns:
            dict|None: Transaction data as dictionary, or None if not found or error
        """
        try:
            transaction = self.collection.find_one({"_id": ObjectId(transaction_id)})
            if transaction:
                transaction["_id"] = str(transaction["_id"])
                transaction["user_id"] = str(transaction["user_id"])
            return transaction
        except PyMongoError as e:
            logger.error(f"Database error while getting transaction: {e}")
            return None

    # In transaction_model.py, update the create_transaction method
    def create_transaction(self, user_id, name, base_file_path, primary_asset_class=None, 
                        secondary_asset_class=None):
        """Create a new transaction in the database with initial parameters
        
        Args:
            user_id (str): ID of the user who owns the transaction
            name (str): Name of the transaction
            base_file_path (str): Base folder path for transaction files
            primary_asset_class (str, optional): Primary asset class
            secondary_asset_class (str, optional): Secondary asset class
            
        Returns:
            str|None: Inserted transaction ID as string, or None on error
        """
        try:
            transaction_data = {
                "user_id": ObjectId(user_id),
                "name": name,
                "base_file_path": base_file_path,
                "version_number": 0,
                "base_file": None,
                "preprocessed_file": None,
                "column_rename_file": None,
                "temp_changing_datatype_of_column": None,
                "changed_datatype_of_column": None,
                "are_all_steps_complete": False,
                "new_added_columns_datatype": {},
                "temp_rbi_rules_applied": None,
                "final_rbi_rules_applied": None,
                "cutoff_date": None  # Add this new field
            }
            
            # Add optional fields if provided
            if primary_asset_class is not None:
                transaction_data["primary_asset_class"] = primary_asset_class
            if secondary_asset_class is not None:
                transaction_data["secondary_asset_class"] = secondary_asset_class
                
            transaction_data = add_timestamps(transaction_data)
            result = self.collection.insert_one(transaction_data)
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Database error while creating transaction: {e}")
            return None

    # Add a new method to update the new_added_columns_datatype field
    def add_new_column_datatype(self, transaction_id, column_name, datatype):
        """Add a new column and its datatype to the new_added_columns_datatype field
        
        Args:
            transaction_id (str): ID of the transaction to update
            column_name (str): Name of the new column
            datatype (str): Datatype of the new column
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
                {
                    "$set": {
                        f"new_added_columns_datatype.{column_name}": datatype,
                        "updated_at": datetime.now()
                    }
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while adding new column datatype: {e}")
            return False

    def update_transaction(self, transaction_id, update_fields):
        """Update transaction's fields with provided data
        
        Args:
            transaction_id (str): ID of the transaction to update
            update_fields (dict): Dictionary containing fields to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Ensure '_id' and 'user_id' are not updated through this method
            update_fields.pop("_id", None)
            update_fields.pop("user_id", None)
            
            update_fields = add_timestamps(update_fields, is_update=True)
            
            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
                {"$set": update_fields}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating transaction: {e}")
            return False

    def delete_transaction(self, transaction_id):
        """Delete a transaction from the database
        
        Args:
            transaction_id (str): ID of the transaction to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.delete_one({"_id": ObjectId(transaction_id)})
            return result.deleted_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while deleting transaction: {e}")
            return False

    def get_transactions_by_user(self, user_id):
        """Fetch all transactions for a given user ID with base file location
        
        Args:
            user_id (str): ID of the user whose transactions are to be fetched
            
        Returns:
            list: List of transactions as dictionaries with base file location, or an empty list on error
        """
        try:
            transactions = self.collection.find({"user_id": ObjectId(user_id)})
            transaction_list = []
            for transaction in transactions:
                transaction["_id"] = str(transaction["_id"])
                transaction["user_id"] = str(transaction["user_id"])
                
                # Add base file location if base_file exists
                if transaction.get("base_file"):
                    base_version = self.transaction_version_model.get_version(transaction["base_file"])
                    if base_version:
                        transaction["base_file_location"] = base_version.get("files_path", "")
                else:
                    transaction["base_file_location"] = ""
                    
                transaction_list.append(transaction)
            return transaction_list
        except PyMongoError as e:
            logger.error(f"Database error while fetching transactions for user {user_id}: {e}")
            return []

    def set_base_file(self, transaction_id, version_id):
        """Set the base_file version_id for a transaction
        
        Args:
            transaction_id (str): ID of the transaction to update
            version_id (str): Version ID to set as base_file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            update_data = {
                "base_file": version_id,
                "updated_at": datetime.now()
            }

            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while setting base_file for transaction {transaction_id}: {e}")
            return False

    def set_preprocessed_file(self, transaction_id, version_id):
        """Set the preprocessed_file version_id for a transaction
        
        Args:
            transaction_id (str): ID of the transaction to update
            version_id (str): Version ID to set as preprocessed_file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            update_data = {
                "preprocessed_file": version_id,
                "updated_at": datetime.now()
            }

            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while setting preprocessed_file for transaction {transaction_id}: {e}")
            return False

    def change_transaction_name(self, transaction_id, new_name):
        """Change the name of a transaction
        
        Args:
            transaction_id (str): ID of the transaction to update
            new_name (str): New name for the transaction
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            update_data = {
                "name": new_name
            }
            update_data = add_timestamps(update_data, is_update=True)
            
            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while changing transaction name: {e}")
            return False

    # Add this method to transaction_model.py
    def update_cutoff_date(self, transaction_id, cutoff_date):
        """Update the cutoff date for a transaction
        
        Args:
            transaction_id (str): ID of the transaction to update
            cutoff_date (str): Cutoff date in dd/mm/yyyy format
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
                {
                    "$set": {
                        "cutoff_date": cutoff_date,
                        "updated_at": datetime.now()
                    }
                }
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Database error while updating cutoff date: {e}")
            return False



    def add_rule_application_root_version(self, transaction_id, version_id):
        """Add a new root version for rule application"""
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
                {"$push": {"rule_application_root_versions": version_id}}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Error adding root version: {e}")
            return False

    def remove_rule_application_root_version(self, transaction_id, version_id):
        """Remove a root version and all its sub-versions"""
        try:
            result = self.collection.update_one(
                {"_id": ObjectId(transaction_id)},
                {"$pull": {"rule_application_root_versions": version_id}}
            )
            return result.modified_count > 0
        except PyMongoError as e:
            logger.error(f"Error removing root version: {e}")
            return False