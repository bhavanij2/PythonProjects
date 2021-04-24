import os


def config_volume_db():
    db = {'host': os.environ['volume_host'], 'port': os.environ['volume_port'], 'database': os.environ['volume_database'],
          'user': os.environ['volume_user'], 'password': os.environ['volume_password']}
    return db


def config_message_db():
    db = {'host': os.environ['message_host'], 'port': os.environ['message_port'], 'database': os.environ['message_database'],
          'user': os.environ['message_user'], 'password': os.environ['message_password']}
    return db
