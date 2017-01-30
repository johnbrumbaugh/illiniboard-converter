import xml.etree.cElementTree as ET
import html2text, os, json, yaml, mysql.connector
from datetime import datetime
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


def get_trim_distance( a, b, c, d ):
    """

    :param a:
    :param b:
    :param c:
    :param d:
    :return:
    """
    trim_distance = 0

    if abs(a) < abs(b):
        if abs(a) < abs(c):
            if abs(a) < abs(d):
                trim_distance = a
            else:
                trim_distance = d
        else:
            if abs(c) < abs(d):
                trim_distance = c
            else:
                trim_distance = d
    else:
        if abs(b) < abs(c):
            if abs(b) < abs(d):
                trim_distance = b
            else:
                trim_distance = d
        else:
            if abs(c) < abs(d):
                trim_distance = c
            else:
                trim_distance = d

    return trim_distance


def create_snippet(text):
    """

    :param text:
    :return:
    """
    if len(text) < 500:
        return text

    initial_cut = text[:550]

    last_index_of_space = initial_cut.rfind(' ')
    last_index_of_semicolon = initial_cut.rfind(';')
    last_index_of_comma = initial_cut.rfind(',')
    last_index_of_period = initial_cut.rfind('.')

    first_index_of_space = initial_cut.find(' ' )
    first_index_of_semicolon = initial_cut.find(';')
    first_index_of_comma = initial_cut.find(',')
    first_index_period = initial_cut.find('.')

    if 500 - last_index_of_space < first_index_of_space - 500:
        distance_of_space = 500 - last_index_of_space
    else:
        distance_of_space = first_index_of_space - 500

    if 500 - last_index_of_semicolon < first_index_of_semicolon - 500:
        distance_of_semicolon = 500 - last_index_of_semicolon
    else:
        distance_of_semicolon = first_index_of_semicolon - 500

    if 500 - last_index_of_comma < first_index_of_comma - 500:
        distance_of_comma = 500 - last_index_of_comma
    else:
        distance_of_comma = first_index_of_comma - 500

    if 500 - last_index_of_period < first_index_period - 500:
        distance_of_period = 500 - last_index_of_period
    else:
        distance_of_period = first_index_period - 500

    trim_distance = get_trim_distance(distance_of_comma, distance_of_period, distance_of_space, distance_of_semicolon)
    return text[:abs(trim_distance)]


def get_twitter_handle(author):
    """
    Quickly get the Twitter handle based on the author.
    :param author:
    :return:
    """
    if author == "Robert":
        return "@alioneye"
    elif author == "CraigG":
        return "@CraigG_IB"
    elif author == "Tyler":
        return "@TylerCott"
    else:
        return "@IlliniBoard"


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

# Parse out the image feed, and place all of the data into an in-memory table accessible by the Post ID as a key.
image_tree = ET.parse('illiniboardcom.media.wordpress.2017-01-29.xml')
image_root = image_tree.getroot()
image_channel = image_root.find('channel')

all_images = dict()
print "Parsing Out the Images from the Image Feed."
for item in tqdm(image_channel.findall('item')):
    image_link = item.find('wp:attachment_url', default_namespace).text
    image_post_id = item.find('wp:post_parent', default_namespace).text
    all_images[image_post_id] = image_link

print "Completed Parsing Images, all_images size is {%s}" % len(all_images)

# Parse out the article feeds.
print "Starting to Parse the Articles from the Article Feed"
article_tree = ET.parse('illiniboardcom.wordpress.2017-01-29.xml')
# article_tree = ET.parse('illiniboardcom.singleentry.xml')
article_root = article_tree.getroot()
article_channel = article_root.find('channel')

all_categories = set()
all_slugs = set()

for post in tqdm(article_channel.findall('item')):
    title = post.find('title').text
    story_id = post.find('wp:post_id', default_namespace).text
    author = post.find('dc:creator', default_namespace).text

    try:
        featured_image_link = all_images[story_id]
    except KeyError:
        featured_image_link = None

    posted_date_string = post.find('wp:post_date', default_namespace).text
    posted_date = datetime.strptime(posted_date_string, "%Y-%m-%d %H:%M:%S")
    slug = post.find('wp:post_name', default_namespace).text
    full_content = post.find('content:encoded', default_namespace).text
    if not full_content:
        full_content = ""
    full_content = full_content.replace('\n', '<br />')

    md_converter = html2text.HTML2Text()
    md_converter.body_width = 0
    md_content = md_converter.handle(full_content)
    directory_name = 'output/%s/%s/%s' % (posted_date.year, posted_date.month, posted_date.day)
    file_name = '%s.md' % slug
    full_slug_path = '/story/%s/%s/%s/%s' % (posted_date.year, posted_date.month, posted_date.day, slug)

    # Getting Flag Information.
    is_featured = "0"
    is_free = "1"
    categories = []
    for category in post.findall('category'):
        categories.append(category.get('nicename', 'none'))
        if category.get('nicename', "none") == "illini":
            is_free = "0"
        if category.get('nicename', "none") == "top-story":
            is_featured = "1"

    #print "*** Processing Story: %s ***" % title
    #print "----> ID: %s" % story_id
    #print "----> Posted Date: Year=%s, Month=%s, Day=%s" % (posted_date.year, posted_date.month, posted_date.day)
    #print "----> URL Slug: %s" % slug
    #print "----> Writing to directory: %s" % directory_name
    #print "----> Writing file name: %s" % file_name
    #print "----> Featured Story: %s" % is_featured
    #print "----> Free Story: %s" % is_free
    #print "----> Featured Image: %s" % featured_image_link

    if not os.path.exists(directory_name):
        os.makedirs(directory_name)
    full_file_path = "%s/%s" % (directory_name, file_name)
    markdown_file = open(full_file_path, 'w')
    # Start to write the header
    markdown_file.write("Start-Header\n".encode('utf8'))
    markdown_file.write(("Title: %s\n" % title).encode('utf8'))
    markdown_file.write(("Tags: %s\n" % categories).encode('utf8'))
    markdown_file.write(("Category: %s\n" % categories).encode('utf8'))
    markdown_file.write(("Featured-Image: %s\n" % featured_image_link).encode('utf8'))
    markdown_file.write(("Author: %s\n" % author).encode('utf8'))
    markdown_file.write(("Twitter-Handle: %s\n" % get_twitter_handle(author)).encode('utf8'))
    if is_free == "0":
        markdown_file.write("Free-Story: False\n".encode('utf8'))
    else:
        markdown_file.write("Free-Story: True\n".encode('utf8'))
    markdown_file.write("End-Header\n".encode('utf8'))

    markdown_file.write(md_content.encode('utf8'))
    markdown_file.close()

    # print "----> File write complete."

    # print "----> Saving Article into the Database."
    try:
        db_conn = mysql.connector.connect(**db_config)
        cursor = db_conn.cursor()
        query = ("INSERT INTO article (title, body, url_slug, date_created, date_published, featured_story, free_story, \
                featured_image_link, author) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
        # data_category_link = (category_slug, story_link)
        article_data = (title, md_content, slug, posted_date, posted_date, is_featured, is_free, featured_image_link, author)
        cursor.execute(query, article_data)
    except mysql.connector.Error as error:
        print "[save_article_in_db] :: error number=%s" % error.errno
        print "[save_article_in_db] :: error=%s" % error
    else:
        db_conn.commit()
        cursor.close()
        db_conn.close()

    # Generate and Store Comment Information
    snippet = create_snippet(md_content)
    thread_id = 0

    try:
        db_conn = mysql.connector.connect(**db_config)
        cursor = db_conn.cursor()
        query = ("INSERT INTO threads (author, board, date_time_posted, is_illiniboard_article, title, topic, body, \
                  article_url_slug, ip_address, last_response_date, url_friendly_title) VALUES (%s, %s, %s, %s, %s, \
                  %s, %s, %s, %s, %s, %s)")
        thread_data = (author, 1, posted_date, 1, title, 'C', snippet, full_slug_path, 'converter_generated',
                       posted_date, slug)
        cursor.execute(query, thread_data)
        thread_id = cursor.lastrowid
    except mysql.connector.Error as error:
        print "error_number=%s" % error.errno
        print "error=%s" % error
    else:
        db_conn.commit()
        cursor.close()
        db_conn.close()

    # print "thread_id=%s" % thread_id
    all_comments = post.findall('wp:comment', default_namespace)

    comment_id_links = {}

    for comment in all_comments:
        comment_id = comment.find('wp:comment_id', default_namespace).text
        comment_author = comment.find('wp:comment_author', default_namespace).text
        comment_posted_date_string = comment.find('wp:comment_date_gmt', default_namespace).text
        comment_parent_id = comment.find('wp:comment_parent', default_namespace).text
        comment_content = comment.find('wp:comment_content', default_namespace).text

        comment_posted_date = datetime.strptime(comment_posted_date_string, "%Y-%m-%d %H:%M:%S")
        if comment_parent_id == "0":
            comment_parent_id = None
        else:
            try:
                comment_parent_id = comment_id_links[comment_parent_id]
            except KeyError:
                comment_parent_id = None


        #print "----> Comment Information"
        #print " * Comment ID: %s" % comment_id
        #print " * Comment Parent Id: %s" % comment_parent_id
        #print " * Comment Author: %s" % comment_author
        #print " * Comment Date: %s" % comment_posted_date
        #print " * Comment Content: %s" % comment_content

        try:
            db_conn = mysql.connector.connect(**db_config)
            cursor = db_conn.cursor()
            query = ("INSERT INTO posts (author, body, containing_thread, date_time_posted, ip_address, parent) \
                      VALUES (%s, %s, %s, %s, %s, %s)")
            post_data = (comment_author, comment_content, thread_id, comment_posted_date, "comment:imported",
                         comment_parent_id)
            cursor.execute(query, post_data)
            post_id = cursor.lastrowid
            comment_id_links[comment_id] = post_id
        except mysql.connector.Error as error:
            print "error_number=%s" % error.errno
            print "error=%s" % error
        else:
            db_conn.commit()
            cursor.close()
            db_conn.close()

        # print "----> Comment Saved."

    # Generate Category Information
    if post.find('category'):
        category_title = post.find('category').text
        category_slug = post.find('category').get('nicename', "none")

        category = {'title': category_title, 'slug': category_slug}

        all_categories.add(json.dumps(category))

        #print "----> Saving Category Link to Story in Database."
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

    #print "----> Category Link Saved to Database."

    # Save the slug information to eventually write it to the file.
    all_slugs.add(slug)

print "Saving Categories to %s" % db_config.get('host')
for cat in tqdm(all_categories):
    my_cat = json.loads(cat)

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

print "Writing Final Imported Slug List"
slug_file = open('slug_list.txt', 'w')
for slug in tqdm(all_slugs):
    slug_file.write('%s\n' % slug)
slug_file.close()

