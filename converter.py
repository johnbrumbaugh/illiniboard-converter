import xml.etree.cElementTree as ET
import html2text, os
from datetime import datetime

print " ------====== IlliniBoard WordPress Converter ======------"
default_namespace = {
    'excerpt': 'http://wordpress.org/export/1.2/excerpt/',
    'content': 'http://purl.org/rss/1.0/modules/content/',
    'wfw': 'http://wellformedweb.org/CommentAPI/',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'wp': 'http://wordpress.org/export/1.2/'
}
tree = ET.parse('illiniboardcom.wordpress.2015-11-28.xml')
root = tree.getroot()
channel = root.find('channel')

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
