import os
import datetime
import urllib
import heapq

import gevent
from gevent.event import AsyncResult
from gevent.queue import Queue, Empty
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy.exc
import yaml

try:
    import xml.etree.cElementTree as ElementTree
except ImportError:
    import xml.etree.ElementTree as ElementTree

from wikibot import cacheschema
from wikibot import monkey

monkey.patch()

class PageProxy(object):
    """A page in a wiki

    The page may not be loaded when this object is created; accessing its
    attributes may block.

    A page is true in a boolean context if it exists on the wiki.
    (Note that the usage in a bool context may also block.)
    """
    def __init__(self, cache, title):
        self.title = title
        self.cache = cache
        self._result = AsyncResult()

    def _set_result(self, contents):
        self._result.set(contents)

    @property
    def up_to_date(self):
        """Return True if the page is up to date"""
        self._result.get()
        return True

    @property
    def contents(self):
        """Return the contents of the page, or None if the page is missing"""
        return self._result.get()

    @property
    def exists(self):
        """Return true if the page exists on the wiki"""
        return self.contents is not None

    @property
    def text(self):
        """Return the contents of the page; raise ValueError if page missing"""
        if self.exists:
            return self.contents
        else:
            raise ValueError('Page does not exist')

    def __bool__(self):
        return self.exists
    __nonzero__ = __bool__


class WikiCache(object):
    """A cache of a MediaWiki

    :param url_base: Base URL of the MediaWiki API, including a '?',
        e.g. 'http://en.wikipedia.org/w/api.php?'
    :param db_path: Path to a SQLite file holding the cache, or SQLAlchemy
        database URL. If not given, a file next to the wikicache module will be
        used.
    :param limit: The cache will not make more than one request each `limit`
        seconds.

    Use the cache as a dictionary: ``cache[page_title]`` will give you a
    PageProxy object.
    """
    workset_limit = 1000
    queue_limit = 10

    def __init__(self, url_base, db_url=None, force_sync=False, limit=5):
        if db_url is None:
            db_url = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                'wikicache.sqlite')
        self.db_url = db_url

        if '://' not in db_url:
            db_url = os.path.abspath(db_url)
            db_url = 'sqlite:///' + db_url

        self._engine = create_engine(db_url)
        self._make_session = sessionmaker(bind=self._engine)

        self._url_base = url_base
        self.limit = limit
        self._needed_metadata = Queue(0)
        self._needed_pages = Queue(0)

        self.update(force_sync=force_sync)

        gevent.spawn(self._request_loop)

    def _get_wiki(self, session):
        """Get the wiki object, creating one if necessary"""
        query = session.query(cacheschema.Wiki).filter_by(
            url_base=self._url_base)
        try:
            return query.one()
        except (sqlalchemy.exc.OperationalError,
                sqlalchemy.orm.exc.NoResultFound):
            cacheschema.metadata.create_all(self._engine)
            wiki = cacheschema.Wiki()
            wiki.url_base = self._url_base
            wiki.sync_timestamp = None
            session.add(wiki)
            session.commit()
            return wiki

    def log(self, string):
        """Log a message"""
        # TODO: Something more fancy
        print string

    def _sleep_seconds(self):
        """Number of seconds to sleep until next request"""
        now = lambda: datetime.datetime.today()
        try:
            next_time = self._next_request_time
        except AttributeError:
            return 0
        else:
            sleep_seconds = (next_time - now()).total_seconds()
            if sleep_seconds > 0:
                self.log('Current sleep time: {}'.format(sleep_seconds))
                return sleep_seconds
            else:
                return 0

    def _sleep_before_request(self):
        """Sleep before another request can be made

        The request rate is controlled by the "limit" attribute
        """
        sleep_seconds = self._sleep_seconds()
        if sleep_seconds > 0:
            self.log('Sleeping %ss' % sleep_seconds)
            gevent.sleep(sleep_seconds)

    def _apirequest_raw(self, **params):
        """Raw MW API request; returns filelike object"""

        self._sleep_before_request()

        try:
            enc = lambda s: unicode(s).encode('utf-8')
            params = [(enc(k), enc(v)) for k, v in params.items()]
            url = self._url_base + urllib.urlencode(params)
            self.log('GET %s' % url)
            result = urllib.urlopen(url)
            return result
        finally:
            self._next_request_time = (datetime.datetime.today() +
                    datetime.timedelta(seconds=self.limit))

    def apirequest(self, **params):
        """MW API request; returns result dict"""
        params['format'] = 'yaml'
        return yaml.load(self._apirequest_raw(**params))

    def update(self, force_sync=True):
        """Fetch a batch of page changes from the server"""
        session = self._make_session()
        wiki = self._get_wiki(session)
        if wiki.last_update and not force_sync:
            thresh = datetime.datetime.today() - datetime.timedelta(minutes=5)
            if wiki.last_update > thresh:
                self.log('Skipping update (last update was {})'.format(
                    wiki.last_update))
                return
        if wiki.sync_timestamp is None:
            self.log('Initial cache setup')
            feed = self.apirequest(action='query', list='recentchanges',
                    rcprop='timestamp', rclimit=1)
            last_change = feed['query']['recentchanges'][0]
            wiki.sync_timestamp = last_change['timestamp']
            wiki.synced = True
            self.invalidate_cache(session, wiki)
            session.commit()
        else:
            self.log('Updating cache')
            feed = self.apirequest(action='query', list='recentchanges',
                    rcprop='title|user|timestamp', rclimit=100,
                    rcend=wiki.sync_timestamp
                )
            sync_timestamp = feed['query']['recentchanges'][0]['timestamp']
            while feed:
                invalidated = set()
                changes = feed['query']['recentchanges']
                for change in changes:
                    title = change['title']
                    if title not in invalidated:
                        self.log(u'Change to {0} by {1}'.format(title,
                                change['user']))
                        obj = self._page_object(session, wiki, title)
                        obj.last_revision = None
                        invalidated.add(title)
                session.commit()
                try:
                    feed = self.apirequest(action='query', list='recentchanges',
                            rcprop='title|user|timestamp', rclimit=100,
                            rcend=wiki.sync_timestamp,
                            **feed['query-continue']['recentchanges']
                        )
                except KeyError:
                    feed = None
                    wiki.sync_timestamp = sync_timestamp
                    wiki.synced = True
                else:
                    wiki.synced = False
                session.commit()
        wiki.last_update = datetime.datetime.today()
        session.commit()

    def _page_query(self, session, wiki):
        """Return a SQLA query for pages on this wiki"""
        return session.query(cacheschema.Page).filter_by(wiki=wiki)

    def _page_object(self, session, wiki, title):
        """Get an object for the page 'title', *w/o* adding it to the session
        """
        title = self.normalize_title(title)
        obj = session.query(cacheschema.Page).get((self._url_base, title))
        if obj:
            return obj
        else:
            obj = cacheschema.Page()
            obj.wiki = wiki
            obj.title = title
            obj.revision = None
            obj.last_revision = None
            return obj

    def _request_loop(self):
        """The greenlet that requests needed metadata/pages
        """
        session = self._make_session()
        wiki = self._get_wiki(session)
        needed_metadata = {}
        needed_pages = {}
        page_query = self._page_query(session, wiki).filter(
            cacheschema.Page.revision != cacheschema.Page.last_revision,
            cacheschema.Page.revision != None)
        md_query = self._page_query(session, wiki).filter(
            cacheschema.Page.last_revision == None)
        while True:
            self.log('Request loop active')
            self._fill_set_from_queue(needed_metadata, self._needed_metadata)
            self._fill_set_from_queue(needed_pages, self._needed_pages)

            done, finish_pages = self._try_fetch(session, wiki,
                self._fetch_pages, needed_pages, page_query,
                chunk_limit=20)
            if done:
                continue

            done, finish_md = self._try_fetch(session, wiki,
                self._fetch_metadata, needed_metadata, md_query,
                chunk_limit=50)
            if done:
                continue

            if finish_md:
                finish_md()
                continue

            if finish_pages:
                finish_pages()
                continue

            self.update(force_sync=False)
            gevent.sleep(1)

    def _try_fetch(self, session, wiki, fetch_func, work_set, extra_query,
            chunk_limit):
        """Generic function for fetching some info from the server

        :param session: The session to use
        :param wiki: The wiki object to use
        :param fetch_func:
            Function that does the work, see _fetch_metadata for signature
        :param work_set:
            Set of titles that were requested to be processed by fetch_func
        :param extra_query:
            Query that yields Page objects that should be processed by
            fetch_func, but were not explicitly requested.
            This is used to "fill up" API requests, so that we fetch as many
            possibly useful pages as we can.
        :param chunk_limit:
            Limit for how many pages can be processed in a single API request.
            Note that there is also an URL size limit.

        :return (done, next):
            done: If a query was made, return True
            next: If not enough pages were accumulated yet for a "full"
                request, a function is returned here. Call it to process the
                remaining pages.
        """
        def _process(chunk):
            for t in fetch_func(session, wiki, chunk):
                for result in work_set.pop(t):
                    result.set()

        session.rollback()
        current_chunk, full = self._get_chunk(work_set, limit=chunk_limit)
        if full:
            _process()
            return True, None
        if current_chunk:
            def fetch_remaining():
                session.rollback()
                finish_chunk, full = self._get_chunk(
                    [t.title for t in extra_query.limit(50)],
                    initial=current_chunk, limit=chunk_limit)
                _process(finish_chunk)
            return False, fetch_remaining
        else:
            return False, None

    def _fill_set_from_queue(self, the_set, queue):
        """Fill a working set from a queue

        We keep at most ``workset_limit`` items in the set; if the set
        gets filled up, we block until some requests are processed.
        """
        sleep_seconds = self._sleep_seconds()
        while len(the_set) < self.workset_limit:
            try:
                title, event = queue.get(timeout=sleep_seconds)
                the_set.setdefault(title, []).append(event)
            except Empty:
                break

    def _get_chunk(self, source, initial=(), limit=20, title_limit=700):
        """Get some pages from a set

        Limit by number of pages (limit) and combined length of titles
        (title_limit).

        Return (titles, full) where titles is the list of titles in the chunk
        and full is true iff the chunk is already full.
        """
        chunk = set(initial)
        length = 0
        source = set(source)
        while source:
            title = source.pop()
            if len(chunk) >= limit or length + len(title) > title_limit:
                return chunk, True
            else:
                chunk.add(title)
                length += len(title)
        return chunk, False

    def _fetch_metadata(self, session, wiki, titles):
        """Fetch page metadata for the given pages.
        """
        result = self.apirequest(action='query', info='lastrevid',
                prop='revisions',  # should not be necessary on modern MW
                titles='|'.join(titles))
        assert 'normalized' not in result['query'], (
                result['query']['normalized'])  # XXX: normalization
        fetched_titles = []
        for page_info in result['query'].get('pages', []):
            page = self._page_object(session, wiki, page_info['title'])
            session.add(page)
            if 'missing' in page_info:
                page.last_revision = 0
                page.revision = 0
                page.contents = None
            else:
                revid = page_info['revisions'][0]['revid']
                # revid = page_info['lastrevid']  # for the modern MW
                page.last_revision = revid
            fetched_titles.append(page.title)
        session.commit()
        return fetched_titles

    def _fetch_pages(self, session, wiki, titles):
        """Fetch content of the given pages from the server.

        Missing pages should not be given to this method! They will make the
        whole request fail.
        """
        dump = self._apirequest_raw(action='query',
                export='1', exportnowrap='1',
                titles='|'.join(titles))
        tree = ElementTree.parse(dump)
        fetched_titles = []
        for elem in tree.getroot():
            tag = elem.tag
            if tag.endswith('}siteinfo'):
                continue
            elif tag.endswith('}page'):
                revision, = (e for e in elem if e.tag.endswith('}revision'))
                pagename, = (e for e in elem if e.tag.endswith('}title'))
                text, = (e for e in revision if e.tag.endswith('}text'))
                revid, = (e for e in revision if e.tag.endswith('}id'))
                page = self._page_object(session, wiki, pagename.text)
                page.last_revision = int(revid.text)
                page.revision = int(revid.text)
                page.contents = text.text
                session.add(page)
                fetched_titles.append(pagename.text)
            else:
                raise ValueError(tag)
        session.commit()
        return fetched_titles

    def invalidate_cache(self, session, wiki):
        """Invalidate the entire cache

        This marks all articles for re-downloading when requested.
        Note that articles with a current revision ID will not be re-downloaded
        entirely, only their metadata will be queried.
        (To clear the cache entirely, truncate the articles table.)
        """
        self._page_query(session, wiki).update({'last_revision': None})
        session.commit()


    def normalize_title(self, title):
        # TODO: http://www.mediawiki.org/wiki/API:Query#Title_normalization
        title = title.replace('_', ' ')
        title = title.replace('\n', '')
        return title[0].upper() + title[1:]


    def get(self, title, follow_redirect=False):
        """Return a page from this cache

        :param follow_redirect: If True, a Mediawiki redirect will be followed
            once.
        """
        title = self.normalize_title(title)

        if follow_redirect:
            try:
                return self[self.redirect_target(title)]
            except KeyError:
                pass

        if not title:
            return default

        result = PageProxy(self, title)

        gevent.spawn(self._read, result)
        return result

    def _read(self, result):
        """Greenlet to fill a PageProxy object

        Submits work to the queues until a page is fully fetched from the
        server, then sets the PageProxy result to unblock the consumer
        """
        session = self._make_session()
        wiki = self._get_wiki(session)
        title = result.title
        obj = self._page_object(session, wiki, title)
        session.add(obj)
        session.commit()
        # Make sure we know the page's last revision
        # This is a loop with rollbacks in it, since the DB can change under us
        while True:
            # Fetch metadata to see if the page has changed (or is empty!)
            if obj.last_revision is None or (not obj.up_to_date and
                    obj.contents is None):
                self.log('Requesting metadata for {}'.format(title))
                md = AsyncResult()
                self._needed_metadata.put((title, md))
                md.get()
            # Now, if metadata says we're out of date, actually fetch the page
            session.refresh(obj)
            if not obj.up_to_date:
                self.log('Requesting page {}'.format(title))
                rd = AsyncResult()
                self._needed_pages.put((title, rd))
                rd.get()
            # If everything was successful, notify the caller!
            session.refresh(obj)
            if obj.up_to_date:
                result._set_result(obj.contents)
                session.rollback()
                return

    def __getitem__(self, title):
        """Return the content of a page, if it exists, or raise KeyError
        """
        return self.get(title)
