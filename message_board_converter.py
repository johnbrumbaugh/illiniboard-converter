import yaml, os
from pymongo import MongoClient


class ConfigNotFoundError(Exception): pass


def read_yaml(filename):
    """

    :param filename:
    :return:
    """
    if not os.path.isfile(filename):
        raise ConfigNotFoundError("Could not find the file with the name: %s" % filename)

    yaml_doc = {}

    with open(filename, 'r') as f:
        yaml_doc = yaml.load(f)

    return yaml_doc


print "---=== IlliniBoard.com Message Board Converter ===---"
config = read_yaml('db_config.yml')
mongo_db_config = config.get('mongodb')
db_uri = "mongodb://%s:%s@%s:%s/%s" % (mongo_db_config.get('user'), mongo_db_config.get('password'),
                                       mongo_db_config.get('host'), mongo_db_config.get('port'),
                                       mongo_db_config.get('database'))

print "DB Config:\n%s" % mongo_db_config
print "DB URI: \n%s" % db_uri

client = MongoClient(db_uri)
print client

db_illiniboard = client.illiniboard

for thread in db_illiniboard.thread.find():
    # Thread Object within MongoDB:
    #   - _id
    #   - author
    #   - body
    #   - dateTimePosted
    #   - ipAddress
    #   - lastResponseDate
    #   - status
    #   - title
    #   - topic
    #   - urlFriendlyTitle
    print "\t* %s: %s\n" % ( thread['_id'], thread['title'])

