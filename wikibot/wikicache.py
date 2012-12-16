import os
import datetime
import urllib

import gevent
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy.exc
import yaml

from wikibot import cacheschema
from wikibot import monkey

monkey.patch()

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
    def __init__(self, url_base, db_url=None, force_sync=False, limit=5):
        if db_url is None:
            db_url = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                'wikicache.sqlite')
        self.db_url = db_url

        if '://' not in db_url:
            db_url = os.path.abspath(db_url)
            db_url = 'sqlite:///' + db_url

        self._engine = create_engine(db_url, echo=True)
        self.make_session = sessionmaker(bind=self._engine)

        self.url_base = url_base
        self.limit = limit
        self._needed_metadata = set()
        self._needed_pages = set()

        self.update(force_sync=force_sync)

        gevent.spawn(self._update_loop)

    def _update_loop(self):
        while True:
            gevent.sleep(60)
            self.log('Updating!')
            self.update(force_sync=True)

    def get_wiki(self, session):
        query = session.query(cacheschema.Wiki).filter_by(
            url_base=self.url_base)
        try:
            return query.one()
        except (sqlalchemy.exc.OperationalError,
                sqlalchemy.orm.exc.NoResultFound):
            cacheschema.metadata.create_all(self._engine)
            wiki = cacheschema.Wiki()
            wiki.url_base = self.url_base
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

    def _apirequest_raw(self, **params):
        """Raw MW API request; returns filelike object"""

        sleep_seconds = self._sleep_seconds()
        if sleep_seconds > 0:
            self.log('Sleeping %ss' % sleep_seconds)
            gevent.sleep(sleep_seconds)

        try:
            enc = lambda s: unicode(s).encode('utf-8')
            params = [(enc(k), enc(v)) for k, v in params.items()]
            url = self.url_base + urllib.urlencode(params)
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
        session = self.make_session()
        wiki = self.get_wiki(session)
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
            self.invalidate_cache(session)
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
                        obj.up_to_date = False
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

    def _page_query(self, session):
        return session.query(cacheschema.Page)

    def _page_object(self, session, wiki, title):
        """Get an object for the page 'title', *w/o* adding it to the session
        """
        title = self.normalize_title(title)
        obj = self._page_query(session).get((self.url_base, title))
        if obj:
            return obj
        else:
            obj = cacheschema.Page()
            obj.wiki = wiki
            obj.title = title
            obj.revision = 0
            obj.up_to_date = False
            return obj

    def normalize_title(self, title):
        # TODO: http://www.mediawiki.org/wiki/API:Query#Title_normalization
        title = title.replace('_', ' ')
        return title[0].upper() + title[1:]

    def invalidate_cache(self, session):
        """Invalidate the entire cache

        This marks all articles for re-downloading when requested.
        Note that articles with a current revision ID will not be re-downloaded
        entirely, only their metadata will be queried.
        (To clear the cache entirely, truncate the articles table.)
        """
        self._page_query(session).update({'up_to_date': False})
        session.commit()
