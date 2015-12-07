import xml.etree.cElementTree as ET
import html2text, os, json, yaml, mysql.connector
from datetime import datetime


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

print " ------====== IlliniBoard WordPress Converter ======------"
default_namespace = {
    'excerpt': 'http://wordpress.org/export/1.2/excerpt/',
    'content': 'http://purl.org/rss/1.0/modules/content/',
    'wfw': 'http://wellformedweb.org/CommentAPI/',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'wp': 'http://wordpress.org/export/1.2/'
}

config = read_yaml('db_config.yml')
db_config = config.get('database').get('development')

tree = ET.parse('illiniboardcom.wordpress.2015-11-28.xml')
# tree = ET.parse('illiniboard-small-extract.xml')
root = tree.getroot()
channel = root.find('channel')

all_categories = set()

for post in channel.findall('item'):
    title = post.find('title').text
    story_id = post.find('wp:post_id', default_namespace).text
    posted_date_string = post.find('wp:post_date', default_namespace).text
    posted_date = datetime.strptime(posted_date_string, "%Y-%m-%d %H:%M:%S")
    slug = post.find('wp:post_name', default_namespace).text
    full_content = post.find('content:encoded', default_namespace).text
    full_content = full_content.replace('\n', '<br />')

    md_converter = html2text.HTML2Text()
    md_converter.body_width = 0
    md_content = md_converter.handle(full_content)
    directory_name = 'output/%s/%s/%s' % (posted_date.year, posted_date.month, posted_date.day)
    file_name = '%s.md' % slug

    print "*** Processing Story: %s ***" % title
    print "----> ID: %s" % story_id
    print "----> Posted Date: Year=%s, Month=%s, Day=%s" % (posted_date.year, posted_date.month, posted_date.day)
    print "----> URL Slug: %s" % slug
    print "----> Writing to directory: %s" % directory_name
    print "----> Writing file name: %s" % file_name

    if not os.path.exists(directory_name):
        os.makedirs(directory_name)
    full_file_path = "%s/%s" % (directory_name, file_name)
    markdown_file = open(full_file_path, 'w')
    page_title = "# %s\n" % title
    markdown_file.write(page_title.encode('utf8'))
    markdown_file.write(md_content.encode('utf8'))
    markdown_file.close()

    print "----> File write complete."

    # Generate Category Information
    category_title = post.find('category').text
    category_slug = post.find('category').get('nicename', "none")

    category = {'title': category_title, 'slug': category_slug}

    all_categories.add(json.dumps(category))

    print "----> Saving Category Link to Story in Database."
    # directory_name = 'output/%s/%s/%s' % (posted_date.year, posted_date.month, posted_date.day)
    # file_name = '%s.md' % slug

    try:
        story_link = '/story/%s/%s/%s/%s' % (posted_date.year, posted_date.month, posted_date.day, slug)
        db_conn = mysql.connector.connect(**db_config)
        cursor = db_conn.cursor()
        query = ("INSERT INTO category_story (category_slug, story_link) VALUES (%s, %s)")
        data_category_link = (category_slug, story_link)
        cursor.execute(query, data_category_link)
    except mysql.connector.Error as error:
        print "[save_category_link] :: error number=%s" % error.errno
        print "[save_category_link] :: error=%s" % error
    else:
        db_conn.commit()
        cursor.close()
        db_conn.close()

    print "----> Category Link Saved to Database."


print "-------------------------------"
print "Final Category List:"
for cat in all_categories:
    my_cat = json.loads(cat)
    print "%s: %s" % (my_cat.get('title'), my_cat.get('slug'))

    # Save the Category Information into the Database.
    try:
        db_conn = mysql.connector.connect(**db_config)
        cursor = db_conn.cursor()
        query = ("INSERT INTO category (slug, title) VALUES (%s, %s)")
        data_category = (my_cat.get('slug'), my_cat.get('title'))
        cursor.execute(query, data_category)
    except mysql.connector.Error as error:
        print "[save_category] :: error number=%s" % error.errno
        print "[save_category] :: error=%s" % error
    else:
        db_conn.commit()
        cursor.close()
        db_conn.close()

print "Categories Saved to %s" % db_config.get('host')
