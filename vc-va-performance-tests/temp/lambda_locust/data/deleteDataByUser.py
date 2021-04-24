import psycopg2

from data.databaseConfig import config


# Parse the data
def parse_data(data):
    if len(data) != 0:
        flatItems = "'" + data[0][0] + "'"
        for d in data[1: len(data)]:
            flatItems = flatItems + ',' + "'" + d[0] + "'"
        return flatItems

class DbConnection:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(**config(section='volume_dev'))
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            print("Volume database connection Successful!")
        except:
            print("Cannot connect to Volume database")

    def getMessageIds(self, loginUser):

        #Get message_ids of all the records from volume_approval_request table for the given login user
        getMessageId = """ SELECT message_id FROM volume_approval_request WHERE 
                                        pending_entry_id IN (SELECT DISTINCT vEntry.id FROM volume_account_entry vEntry 
                                        JOIN volume_account_transaction vTransaction ON vEntry.volume_account_transaction_id = vTransaction.id 
                                        WHERE vTransaction.login_user = %s ) OR
                                        closed_entry_id IN (SELECT DISTINCT vEntry.id FROM volume_account_entry vEntry 
                                        JOIN volume_account_transaction vTransaction ON vEntry.volume_account_transaction_id = vTransaction.id 
                                        WHERE vTransaction.login_user = %s )
                                        """ % (str(loginUser), str(loginUser))
        self.cursor.execute(getMessageId)
        result = self.cursor.fetchall()
        return parse_data(result)

    def delete_data(self, loginUser):
        try:

            # delete data from vc_message_ps before deleting the data from vc_vol_appr_ps
            message_connection.clearMessageData(str(loginUser))

            vat_transaction_id = """ SELECT id FROM volume_account_transaction WHERE login_user = %s """ % str(loginUser)

            vae_id = """ SELECT DISTINCT vEntry.id FROM volume_account_entry vEntry 
                                        JOIN volume_account_transaction vTransaction ON vEntry.volume_account_transaction_id = vTransaction.id 
                                        WHERE vTransaction.login_user = %s """ % str(loginUser)


            vae_volume_account_id = """ SELECT DISTINCT vEntry.volume_account_id FROM volume_account_entry vEntry 
                                        JOIN volume_account_transaction vTransaction ON vEntry.volume_account_transaction_id = vTransaction.id 
                                        WHERE vTransaction.login_user = %s """ % str(loginUser)

            va_volume_id = """ SELECT DISTINCT vAccount.volume_id FROM volume_account vAccount JOIN volume_account_entry vEntry  ON vAccount.id = vEntry.volume_account_id
                                    JOIN volume_account_transaction vTransaction ON vTransaction.id = vEntry.volume_account_transaction_id
                                    WHERE vTransaction.login_user = %s """ % str(loginUser)

            self.cursor.execute(vat_transaction_id)
            vat_transactionId = parse_data(self.cursor.fetchall())

            self.cursor.execute(vae_id)
            vaeId = parse_data(self.cursor.fetchall())

            self.cursor.execute(vae_volume_account_id)
            vae_volumeAccountId = parse_data(self.cursor.fetchall())

            self.cursor.execute(va_volume_id)
            va_volumeId = parse_data(self.cursor.fetchall())

            if vaeId:
                # delete from volume_approval_request
                deleteVolumeApprovalRequest = """DELETE FROM volume_approval_request WHERE pending_entry_id IN (%s) OR closed_entry_id IN (%s) """ % (vaeId, vaeId)
                self.cursor.execute(deleteVolumeApprovalRequest)

            if vat_transactionId:
                # Delete data from volume_account_entry
                deleteAccountEntry = """DELETE FROM volume_account_entry WHERE volume_account_transaction_id IN (%s) """ % vat_transactionId
                self.cursor.execute(deleteAccountEntry)

            # Delete data from volume_account_transaction
            deleteVolumeAccountTransaction = "DELETE FROM volume_account_transaction WHERE login_user = %s" % str(loginUser)
            self.cursor.execute(deleteVolumeAccountTransaction)

            if vae_volumeAccountId:
                # Delete data from volume_account
                deleteVolumeAccount = "DELETE FROM volume_account where id IN (%s)" % vae_volumeAccountId
                self.cursor.execute(deleteVolumeAccount)

            if va_volumeId:
                # Delete data from volume
                deleteVolume = "DELETE FROM volume where id IN (%s)" % va_volumeId
                self.cursor.execute(deleteVolume)
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

    def clearMessageData(self, loginUser):
        messageIds = database_connection.getMessageIds(loginUser)

        if(messageIds):
            try:
                deleteContent = """ DELETE FROM content WHERE message_id IN (%s)""" % messageIds
                self.cursor.execute(deleteContent)
                self.connection.commit()

                deleteRecipient = """ DELETE FROM recipient WHERE message_id IN (%s)""" % messageIds
                self.cursor.execute(deleteRecipient)
                self.connection.commit()

                deleteMessage = """ DELETE FROM message WHERE id IN (%s) """ % messageIds
                self.cursor.execute(deleteMessage)
                self.connection.commit()

                print("Successfully deleted data from Message database")
            except (RuntimeError, ConnectionError):
                print("Error occurred during deletion of data on Message database. Rolling back the deletion")
                self.connection.rollback()
        else:
            print("No records to delete from the Message database")


database_connection = DbConnection()
message_connection = MessageDbConnection()
#database_connection.delete_data("'EVNRU_PART'")
