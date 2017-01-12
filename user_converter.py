import yaml, os, mysql.connector, json
from tqdm import tqdm
from datetime import datetime
from pprint import pprint


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

print "---=== IlliniBoard.com User Converter ===---"
config = read_yaml('db_config.yml')
db_config = config.get('database').get('development')

user_data = json.loads(open("_User.json").read())

user_list = user_data['results']
print "User List Size: %d" % len(user_list)

for user in tqdm(user_list):
    try:
        db_conn = mysql.connector.connect(**db_config)
        cursor = db_conn.cursor()
        query = ("INSERT INTO user (username, password, created_on, email_address, registration_ip_address, \
                  avatar_url, is_validated) VALUES (%s, %s, %s, %s, %s, %s, %s)")
        try:
            avatar_url = user['avatar_url']
        except KeyError:
            avatar_url = None

        try:
            registration_ip_address = user['registration_ip_address']
        except KeyError:
            registration_ip_address = "from_import"

        try:
            email = user['email']
        except KeyError:
            email = None
        created_on_time = datetime.strptime(user['createdAt'], "%Y-%m-%dT%H:%M:%S.%fZ")
        user_data = (user['username'], user['bcryptPassword'], created_on_time.strftime("%Y-%m-%d %H:%M:%S"),
                     email, registration_ip_address, avatar_url, 1)
        cursor.execute(query, user_data)
    except mysql.connector.Error as error:
        print "[save_user_in_db] :: error number=%s" % error.errno
        print "[save_user_in_db] :: error=%s" % error
    else:
        db_conn.commit()
        cursor.close()
        db_conn.close()

print "Users Saved."
