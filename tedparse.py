import pandas as pd
import json
from lxml import html
import itertools as itt
from requests_futures.sessions import FuturesSession

'''

    program to parse ted talk transcripts and meta information
    results are stored as json file in working directory

'''


def gallery_scrape(index_range=xrange(1, 60)):
    '''

        scrape information about tedtalks listed on tedtalk gallery pages

        Arguments:
            index_range: (int iterator) gallery pages to scrape
            there were 60 total pages at the time of writing this method
            with urls of the following form:
                https://www.ted.com/talk?page=i
            where i is in index_range

        Results:
            returns list of tuples containing info for each page
            (speaker names, talk titles, talk urls, posted date, ratings)

    '''
    url_root = 'https://www.ted.com'

    def get_pages(response):
        '''scrape the url and meta info for ted-talk pages from ted-talk index pages
        '''
        try:
            tree = html.fromstring(response.content)
            speakers = tree.xpath(
                          '//h4[@class="h12 talk-link__speaker"]/text()')
            titles = tree.xpath('//h4[@class="h9 m5"]//a/text()')
            urls = tree.xpath('//h4[@class="h9 m5"]//a/@href')
            meta = tree.xpath(
                      '//div[@class="meta"]//span[@class="meta__val"]//text()')
            dates = [meta[i] for i in range(len(meta)) if i % 2 == 0]
            cats = [meta[i] for i in range(len(meta)) if i % 2 > 0]
        except Exception, e:
            print 'err: %s in get_pages' % str(e)
            speakers, titles, urls, dates, cats = [], [], [], [], []
        return zip(speakers, titles, urls, dates, cats)

    session = FuturesSession(max_workers=5)
    page_urls = ['%s/talks?page=%s' % (url_root, i) for i in range(1, 60)]
    futures = [session.get(url) for url in page_urls]
    pages = [get_pages(f.result()) for f in futures]

    return [page for page in itt.chain.from_iterable(pages)]


def talk_scrape(talk_urls):
    '''

        scrape meta information from main talk page

        Arguments:
            talk_urls: list of urls to tedtalk summary pages of the form
                /talks/<talk_title>?language=en

        Result:
            returns list of tuples containing info for each talk
            (view count, similar topics (as comma delimeted string))

    '''
    url_root = 'https://www.ted.com'

    def get_meta(response):
        try:
            tree = html.fromstring(response.content)
            view_count = tree.xpath(
                            '//span[@class="talk-sharing__value"]/text()')
            similar_topics = tree.xpath(
                            '//li[@class="talk-topics__item"]/a//text()')
            similar_topics = ','.join([t.strip('\n') for t in similar_topics])
        except Exception, e:
            print 'err: %s in get_meta' % str(e)
            view_count = 0
            similar_topics = ''
        return (view_count, similar_topics)

    session = FuturesSession(max_workers=5)
    urls = ['%s%s?language=en' % (url_root, talk_url)
            for talk_url in talk_urls]
    futures = [session.get(url) for url in urls]
    meta = [get_meta(f.result()) for f in futures]

    return meta


def transcript_scrape(talk_urls):
    '''

        scrape talk english transcript from talk summary page

        Arguments:
            talk_urls: list of urls to tedtalk summary pages of the form
                /talks/<talk_title>/transcript?language=en

        Result:
            returns list of transcripts

    '''
    url_root = 'https://www.ted.com'

    def get_content(response):
        try:
            tree = html.fromstring(response.content)
            transcript = tree.xpath(
                '//div[@class="talk-article__body talk-transcript__body"]\
                //span[@class="talk-transcript__para__text"]//text()'
            )
            transcript = ' '.join([t for t in transcript if t != '\n'])
            transcript = transcript.replace('\n', ' ')
        except Exception, e:
            print 'err: %s in get_content' % str(e)
            transcript = ''
        return transcript

    session = FuturesSession(max_workers=5)
    urls = ['%s%s/transcript?language=en' % (url_root, talk_url)
            for talk_url in talk_urls]
    futures = [session.get(url) for url in urls]
    transcripts = [get_content(f.result()) for f in futures]

    return transcripts


if __name__ == '__main__':

    print 'scraping https://www.ted.com ...'
    df = pd.DataFrame(
                      gallery_scrape(),
                      columns=['speaker', 'title', 'url', 'date', 'categories']
    )
    df['transcript'] = transcript_scrape(df.url)
    df['view_n'], df['topics'] = map(list, zip(*talk_scrape(df.url)))

    print '%d talks found. saving raw data ...' % len(df)

    df.view_n = df.view_n.apply(lambda x: x[0] if len(x) > 0 else '0')
    df = df.applymap(lambda x: x.strip('\n'))
    df.view_n = df.view_n.apply(lambda x: int(x.replace(',', '')))

    def row_to_dict(row):
        '''map of df columns to dictionary keys,values'''
        url_root = 'https://www.ted.com'
        return {
            'speaker': row.speaker,
            'title': row.title,
            'date': row.date,
            'url': url_root + row.url,
            'categories': [x for x in row.categories.split(',')],
            'transcript': row.transcript,
            'view_n': row.view_n,
            'topics': [x for x in row.topics.split(',')]
        }

    print 'serializing data to json ...'
    serial = df.apply(row_to_dict, axis=1).tolist()

    print 'saving as json ...'
    fname = 'tedtalk_full.json'
    with open(fname, 'w') as f:
        json.dump(serial, f)

    print 'completed.  data saved as "%s" in working directory' % fname
