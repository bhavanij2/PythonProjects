import psycopg2

from data.databaseConfig import config


class DbConnection:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(**config(section='volume_dev'))
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            print("Volume database connection Successful!")
        except:
            print("Cannot connect to Volume database")

    def getMessageIds(self):

        # Get message_id of all the records from volume_approval_request table

        getMessageId = """ SELECT message_id from volume_approval_request """
        self.cursor.execute(getMessageId)
        return parse_data(self.cursor.fetchall())

    def clear_test_data(self):
        try:

            # delete data from vc_message_ps before deleting the data from vc_vol_appr_ps
            message_connection.clearMessageData()

            # delete from volume_approval_request
            deleteVolumeApprovalRequest = " DELETE FROM volume_approval_request "
            self.cursor.execute(deleteVolumeApprovalRequest)

            # Delete data from volume_account_entry
            deleteAccountEntry = "DELETE FROM volume_account_entry"
            self.cursor.execute(deleteAccountEntry)

            # Delete data from volume_account
            deleteVolumeAccount = "DELETE FROM volume_account"
            self.cursor.execute(deleteVolumeAccount)

            # Delete data from volume
            deleteVolume = "DELETE FROM volume"
            self.cursor.execute(deleteVolume)

            # Delete data from volume_account_transaction
            deleteVolumeAccountTransaction = "DELETE FROM volume_account_transaction"
            self.cursor.execute(deleteVolumeAccountTransaction)
            self.connection.commit()
            print("Successfully deleted data from Volume database ")

        except (RuntimeError, ConnectionError):
            print("Error occurred during deletion of data on volume database. Rolling back the deletion")
            self.connection.rollback()


class MessageDbConnection:

    def __init__(self):
        try:
            self.connection = psycopg2.connect(**config(section='message_dev'))
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            print("Message database connection successful!")
        except:
            print("Cannot connect to Message database")

    def clearMessageData(self):
        messageIds = database_connection.getMessageIds()

        if(messageIds):
            try:
                deleteContent = """ DELETE FROM content WHERE message_id IN (%s)""" % messageIds
                self.cursor.execute(deleteContent)

                deleteRecipient = """ DELETE FROM recipient WHERE message_id IN (%s)""" % messageIds
                self.cursor.execute(deleteRecipient)

                deleteMessage = """ DELETE FROM message WHERE id IN (%s) """ % messageIds
                self.cursor.execute(deleteMessage)
                self.connection.commit()

                print("Successfully deleted data from Message database")

            except (RuntimeError, ConnectionError):
                print("Error occurred during deletion of data on Message database. Rolling back the deletion")
                self.connection.rollback()
        else:
            print("No records to delete from the Message database")


# Parse the data
def parse_data(data):
    if len(data) != 0:
        flatItems = "'" + data[0][0] + "'"
        for d in data[1: len(data)]:
            flatItems = flatItems + ',' + "'" + d[0] + "'"
        return flatItems

database_connection = DbConnection()
message_connection = MessageDbConnection()
#database_connection.clear_test_data()
