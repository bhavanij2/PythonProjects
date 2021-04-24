import psycopg2
from data.database_config import config_volume_db
from data.database_config import config_message_db


class VolumeDbConnection:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(**config_volume_db())
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            print("Volume database connection Successful!")
        except Exception as e:
            print("Cannot connect to Volume database:")
            print(e)

    def clear_test_data(self):
        try:

            # delete data from vc_message_ps before deleting the data from vc_vol_appr_ps
            message_connection.clear_message_data()

            # delete from volume_approval_request
            delete_volume_approval_request = "DELETE FROM volume_approval_request"
            self.cursor.execute(delete_volume_approval_request)

            # Delete data from volume_account_entry
            delete_account_entry = "DELETE FROM volume_account_entry"
            self.cursor.execute(delete_account_entry)

            # Delete data from volume_account
            delete_volume_account = "DELETE FROM volume_account"
            self.cursor.execute(delete_volume_account)

            # Delete data from volume
            delete_volume = "DELETE FROM volume"
            self.cursor.execute(delete_volume)

            # Delete data from volume_account_transaction
            delete_volume_account_transaction = "DELETE FROM volume_account_transaction"
            self.cursor.execute(delete_volume_account_transaction)
            self.connection.commit()
            print("Successfully deleted data from Volume database ")

        except Exception as e:
            print("Error occurred during deletion of data on Volume database:")
            print(e)
            print("Rolling back the deletion")
            self.connection.rollback()


class MessageDbConnection:

    def __init__(self):
        try:
            self.connection = psycopg2.connect(**config_message_db())
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            print("Message database connection successful!")
        except Exception as e:
            print("Cannot connect to Message database:")
            print(e)

    def get_message_ids(self):
        # Get message_id of all the records from content table
        get_message_id = """ SELECT message_id from content where content_type = 'VOLUME_APPROVAL_REQUEST' """
        self.cursor.execute(get_message_id)
        return parse_data(self.cursor.fetchall())

    def clear_message_data(self):
        message_ids = message_connection.get_message_ids()

        if message_ids:
            try:

                delete_recipient = """ DELETE FROM recipient WHERE message_id IN (%s)""" % message_ids
                self.cursor.execute(delete_recipient)

                delete_content = """ DELETE FROM content WHERE message_id IN (%s)""" % message_ids
                self.cursor.execute(delete_content)

                delete_message = """ DELETE FROM message WHERE id IN (%s) """ % message_ids
                self.cursor.execute(delete_message)
                self.connection.commit()

                print("Successfully deleted data from Message database")

            except Exception as e:
                print("Error occurred during deletion of data on Message database:")
                print(e)
                print("Rolling back the deletion.")
                self.connection.rollback()
        else:
            print("No records to delete from the Message database")


# Parse the data
def parse_data(data):
    if len(data) != 0:
        flat_items = "'" + data[0][0] + "'"
        for d in data[1: len(data)]:
            flat_items = flat_items + ',' + "'" + d[0] + "'"
        return flat_items


database_connection = VolumeDbConnection()
message_connection = MessageDbConnection()
#database_connection.clear_test_data()
