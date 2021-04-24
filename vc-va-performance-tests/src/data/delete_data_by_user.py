import psycopg2

from data.database_config import config


# Parse the data
def parse_data(data):
    if len(data) != 0:
        flat_items = "'" + data[0][0] + "'"
        for d in data[1: len(data)]:
            flat_items = flat_items + ',' + "'" + d[0] + "'"
        return flat_items


class DbConnection:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(**config(section='volume_dev'))
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            print("Volume database connection Successful!")
        except:
            print("Cannot connect to Volume database")

    def get_message_ids(self, login_user):

        # Get message_ids of all the records from volume_approval_request table for the given login user
        get_message_id = """ SELECT message_id FROM volume_approval_request WHERE 
                                        pending_entry_id IN (SELECT DISTINCT vEntry.id FROM volume_account_entry vEntry 
                                        JOIN volume_account_transaction vTransaction 
                                        ON vEntry.volume_account_transaction_id = vTransaction.id 
                                        WHERE vTransaction.login_user = %s ) OR
                                        closed_entry_id IN (SELECT DISTINCT vEntry.id FROM volume_account_entry vEntry 
                                        JOIN volume_account_transaction vTransaction 
                                        ON vEntry.volume_account_transaction_id = vTransaction.id 
                                        WHERE vTransaction.login_user = %s )
                                        """ % (str(login_user), str(login_user))
        self.cursor.execute(get_message_id)
        result = self.cursor.fetchall()
        return parse_data(result)

    def delete_data(self, login_user):
        try:

            # delete data from vc_message_ps before deleting the data from vc_vol_appr_ps
            message_connection.clear_message_data(str(login_user))

            vat_transaction_id = """ SELECT id FROM volume_account_transaction WHERE login_user = %s """ % str(login_user)

            vae_id = """ SELECT DISTINCT vEntry.id FROM volume_account_entry vEntry 
                                        JOIN volume_account_transaction vTransaction 
                                        ON vEntry.volume_account_transaction_id = vTransaction.id 
                                        WHERE vTransaction.login_user = %s """ % str(login_user)

            vae_volume_account_id = """ SELECT DISTINCT vEntry.volume_account_id FROM volume_account_entry vEntry 
                                        JOIN volume_account_transaction vTransaction 
                                        ON vEntry.volume_account_transaction_id = vTransaction.id 
                                        WHERE vTransaction.login_user = %s """ % str(login_user)

            va_volume_id = """ SELECT DISTINCT vAccount.volume_id FROM volume_account vAccount 
                                        JOIN volume_account_entry vEntry  
                                        ON vAccount.id = vEntry.volume_account_id
                                        JOIN volume_account_transaction vTransaction 
                                        ON vTransaction.id = vEntry.volume_account_transaction_id
                                        WHERE vTransaction.login_user = %s """ % str(login_user)

            self.cursor.execute(vat_transaction_id)
            vat_transaction_id = parse_data(self.cursor.fetchall())

            self.cursor.execute(vae_id)
            vae_id = parse_data(self.cursor.fetchall())

            self.cursor.execute(vae_volume_account_id)
            vae_volume_account_id = parse_data(self.cursor.fetchall())

            self.cursor.execute(va_volume_id)
            va_volume_id = parse_data(self.cursor.fetchall())

            if vae_id:
                # delete from volume_approval_request
                delete_volume_approval_request = """DELETE FROM volume_approval_request 
                                                    WHERE pending_entry_id IN (%s) 
                                                    OR closed_entry_id IN (%s) """ % (vae_id, vae_id)
                self.cursor.execute(delete_volume_approval_request)

            if vat_transaction_id:
                # Delete data from volume_account_entry
                delete_account_entry = """DELETE FROM volume_account_entry 
                                          WHERE volume_account_transaction_id IN (%s) """ % vat_transaction_id
                self.cursor.execute(delete_account_entry)

            # Delete data from volume_account_transaction
            delete_volume_account_transaction = "DELETE FROM volume_account_transaction " \
                                                "WHERE login_user = %s" % str(login_user)
            self.cursor.execute(delete_volume_account_transaction)

            if vae_volume_account_id:
                # Delete data from volume_account
                delete_volume_account = "DELETE FROM volume_account where id IN (%s)" % vae_volume_account_id
                self.cursor.execute(delete_volume_account)

            if va_volume_id:
                # Delete data from volume
                delete_volume = "DELETE FROM volume where id IN (%s)" % va_volume_id
                self.cursor.execute(delete_volume)
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

    def clear_message_data(self, login_user):
        message_ids = database_connection.get_message_ids(login_user)

        if message_ids:
            try:
                delete_content = """ DELETE FROM content WHERE message_id IN (%s)""" % message_ids
                self.cursor.execute(delete_content)
                self.connection.commit()

                delete_recipient = """ DELETE FROM recipient WHERE message_id IN (%s)""" % message_ids
                self.cursor.execute(delete_recipient)
                self.connection.commit()

                delete_message = """ DELETE FROM message WHERE id IN (%s) """ % message_ids
                self.cursor.execute(delete_message)
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
