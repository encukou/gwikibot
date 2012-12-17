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

from wikibot import cacheschema
from wikibot import monkey

monkey.patch()

class PageProxy(object):
    def __init__(self, cache, title):
        self.title = title
        self.cache = cache
        self._result = AsyncResult()

    def _set_result(self, contents):
        self._result.set(contents)

    @property
    def up_to_date(self):
        self._result.get()
        return True

    @property
    def contents(self):
        return self._result.get()

    @property
    def exists(self):
        return self.contents is not None

    @property
    def text(self):
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
    """
    workset_limit = 100
    queue_limit = 2

    def __init__(self, url_base, db_url=None, force_sync=False, limit=5):
        if db_url is None:
            db_url = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                'wikicache.sqlite')
        self.db_url = db_url

        if '://' not in db_url:
            db_url = os.path.abspath(db_url)
            db_url = 'sqlite:///' + db_url

        self._engine = create_engine(db_url, echo=True)
        self._make_session = sessionmaker(bind=self._engine)

        self._url_base = url_base
        self.limit = limit
        self._needed_metadata = Queue(0)
        self._needed_pages = Queue(0)

        self.update(force_sync=force_sync)

        gevent.spawn(self._request_loop)

    def _get_wiki(self, session):
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
            return wiki

    def log(self, string):
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
                return sleep_seconds
            else:
                return 0

    def _sleep_before_request(self):
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
        needed_metadata = {}
        needed_pages = {}
        while True:
            self.log('Request loop active')
            self._fill_set_from_queue(needed_metadata, self._needed_metadata)
            self._fill_set_from_queue(needed_pages, self._needed_pages)
            gevent.sleep(1)

    def _fill_set_from_queue(self, the_set, queue):
        while len(the_set) < self.workset_limit:
            try:
                title, event = queue.get(timeout=self._sleep_seconds())
                the_set.setdefault(title, []).append(event)
            except Empty:
                break
            print the_set, queue

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
        """Fill the result
        """
        session = self._make_session()
        wiki = self._get_wiki(session)
        obj = self._page_object(session, wiki, result.title)
        # Make sure we know the page's last revision
        # This is a loop since the DB can actually change from under us
        while obj.last_revision is None:
            md = AsyncResult()
            self._needed_metadata.put((result.title, md))
            md.get()
            # Make sure the page's last revision matches our data
            session.rollback()
            if not obj.up_to_date:
                rd = AsyncResult()
                self._needed_pages.put((result.title, rd))
                rd.get()
            session.rollback()
            if obj.up_to_date:
                result._set_result(obj.contents)
                return

    def __getitem__(self, title):
        """Return the content of a page, if it exists, or raise KeyError
        """
        return self.get(title)
