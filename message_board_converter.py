import yaml, os, mysql.connector
from pymongo import MongoClient
from tqdm import tqdm


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
mysql_db_config = config.get('database').get('development')
mongo_db_config = config.get('mongodb')
db_uri = "mongodb://%s:%s@%s:%s/%s" % (mongo_db_config.get('user'), mongo_db_config.get('password'),
                                       mongo_db_config.get('host'), mongo_db_config.get('port'),
                                       mongo_db_config.get('database'))

print "DB Config:\n%s" % mongo_db_config
print "DB URI: \n%s" % db_uri

client = MongoClient(db_uri)
print client

db_illiniboard = client.illiniboard

for thread in tqdm(db_illiniboard.thread.find()):
    # print "\t* %s: %s\n" % (thread['_id'], thread['title'])
    # print "\t\t- Author: %s" % thread['author']
    # print "\t\t- Date Time Posted: %s" % thread['dateTimePosted']
    # print "\t\t- IP Address: %s" % thread['ipAddress']
    # print "\t\t- Last Response Date: %s" % thread['lastResponseDate']
    # print "\t\t- Title: %s" % thread['title']
    # print "\t\t- URL Friendly Title: %s" % thread['urlFriendlyTitle']
    # print "\t\t- Topic: %s" % thread['topic'] # OTHER, HOOPS, FOOTBALL

    child_posts = db_illiniboard.post.find({'containingThread': thread['_id']})

    try:
        db_conn = mysql.connector.connect(**mysql_db_config)
        cursor = db_conn.cursor()
        query = ("INSERT INTO threads ( author, board, date_time_posted, ip_address, last_response_date, title, topic, \
                  url_friendly_title, body, response_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

        if thread['topic'] == "OTHER":
            thread_topic = "O"
        elif thread['topic'] == "HOOPS":
            thread_topic = "B"
        elif thread['topic'] == "FOOTBALL":
            thread_topic = "F"
        else:
            thread_topic = "O"

        thread_data = (thread['author'], 4, thread['dateTimePosted'], thread['ipAddress'], thread['lastResponseDate'],
                       thread['title'], thread_topic, thread['urlFriendlyTitle'], thread['body'], child_posts.count())
        cursor.execute(query, thread_data)
        thread_id = cursor.lastrowid

        post_id_links = {}
        for post in child_posts:
            try:
                if post['parent']:
                    try:
                        parent_id = post_id_links[post['parent']]
                    except KeyError:
                        parent_id = None
                    post_query = ("INSERT INTO posts (author, body, containing_thread, date_time_posted, ip_address, \
                                   parent) VALUES (%s, %s, %s, %s, %s, %s)")
                    post_data = (post['author'], post['body'], thread_id, post['dateTimePosted'], post['ipAddress'],
                                 parent_id)
                else:
                    post_query = ("INSERT INTO posts (author, body, containing_thread, date_time_posted, ip_address) \
                                   VALUES (%s, %s, %s, %s, %s)")
                    post_data = (post['author'], post['body'], thread_id, post['dateTimePosted'], post['ipAddress'])
            except KeyError:
                post_query = ("INSERT INTO posts (author, body, containing_thread, date_time_posted, ip_address) \
                               VALUES (%s, %s, %s, %s, %s)")
                post_data = (post['author'], post['body'], thread_id, post['dateTimePosted'], post['ipAddress'])
            cursor.execute(post_query, post_data)
            post_id_links[post['_id']] = cursor.lastrowid
    except mysql.connector.Error as error:
        print "[save_thread_in_db] :: error number=%s" % error.errno
        print "[save_thread_in_db] :: error=%s" % error
    else:
        db_conn.commit()
        cursor.close()
        db_conn.close()

print "Done."
