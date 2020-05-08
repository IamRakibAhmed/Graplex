from bs4 import BeautifulSoup
import sqlite3
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
import ssl

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('spiderWEB.sqlite')
curr = conn.cursor()

curr.executescript('''
CREATE TABLE IF NOT EXISTS Pages (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT, error INTEGER, 
old_rank REAL, new_rank REAL);
CREATE TABLE IF NOT EXISTS Links (from_id INTEGER, to_id INTEGER);
CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE);
''')

curr.execute('SELECT id, url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
row = curr.fetchone()

def starturl():
    inp = input('Enter the URL: ')
    return inp

if row is not None:
    print('Restarting the existing crawl...\nPlease remove the spiderWEB.sqlite for starting a new crawl.')
else:
    sURL = starturl()
    if len(sURL) < 1:
        starturl()
    if sURL.endswith('/'):
        sURL = sURL[:-1]

    site = sURL

    if sURL.endswith('.html') or sURL.endswith('.htm'):
        pos = sURL.rfind('/')
        site = sURL[:pos]
    if len(site) > 1:
        curr.execute('INSERT OR IGNORE INTO webs(url) VALUES(?)', (site, ))
        curr.execute('INSERT OR IGNORE INTO Pages(url, html, new_rank) VALUES(?, NULL, 1.0)', (sURL, ))
        conn.commit()

curr.execute('SELECT url FROM webs')

webs = list()
for row in curr:
    webs.append(row[0])
print(webs)

many = 0
while True:
    if many < 1:
        pnum = input('How many page(s) do you want to retrieve: ')
        if len(pnum) < 1:
            break
        many = int(pnum)
    many = many - 1

    curr.execute('SELECT id, url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')

    try:
        row = curr.fetchone()
        fromid = row[0]
        url = row[1]
    except:
        print('No HTML page found to retrieve.')
        many = 0
        break
    print(fromid, url, end=' ')

    curr.execute('DELETE FROM Links WHERE from_id = ?', (fromid, ))
    try:
        doc = urlopen(url, context=ctx)
        html = doc.read()
        soup = BeautifulSoup(html, 'html.parser')

        if doc.getcode() != 200:
            print('Error', doc.getcode())
            curr.execute('UPDATE Pages SET error = ? WHERE url = ?', (doc.getcode(), url))
        if 'text/html' != doc.info().get_content_type():
            print('Ignore non-text/html pages.')
            curr.execute('DELETE FROM Pages WHERE url = ?', (url, ))
            conn.commit()
            continue

        print('('+str(len(html))+')', end=' ')
    except KeyboardInterrupt:
        print('')
        print('Program is interrupted by User.')
        break
    except:
        print('Unable to retrieve or parse the page.')
        curr.execute('UPDATE Pages SET error = -1 WHERE url = ?', (url, ))
        conn.commit()
        continue

    curr.execute('INSERT OR IGNORE INTO Pages(url, html, new_rank) VALUES(?, NULL, 1.0)', (url, ))
    curr.execute('UPDATE Pages SET html = ? WHERE URL = ?', (memoryview(html), url))

    tags = soup('a')
    count = 0

    for tag in tags:
        href = tag.get('href', None)
        if href is None:
            continue
        up = urlparse(href)
        if len(up.scheme) < 1:
            href = urljoin(url, href)

        ipos = href.find('#')
        if ipos > 1:
            href = href[:ipos]
        if href.endswith('.png') or href.endswith('jpg') or href.endswith('gif'):
            continue
        if href.endswith('/'):
            href = href[:-1]
        if len(href) < 1:
            continue

        found = False
        for web in webs:
            if href.startswith(web):
                found = True
                break
        if not found:
            continue

        curr.execute('INSERT OR IGNORE INTO Pages(url, html, new_rank) VALUES(?, null, 1.0)', (href, ))
        count = count + 1
        conn.commit()

        curr.execute('SELECT id FROM Pages WHERE url = ?', (href, ))
        try:
            row = curr.fetchone()
            toid = row[0]
        except:
            print('Could not retrieve ID')
            continue
        curr.execute('INSERT OR IGNORE INTO Links(from_id, to_id) VALUES(?, ?)', (fromid, toid))

    print(count)

curr.close()
