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
    def __init__(self, url_base, db_url=None, limit=5):
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

        self.update()

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

    def update(self):
        """Fetch a batch of page changes from the server"""
        session = self.make_session()
        wiki = self.get_wiki(session)
        if wiki.sync_timestamp is None:
            self.log('Initial update')
            feed = self.apirequest(action='query', list='recentchanges',
                    rcprop='timestamp', rclimit=1)
            last_change = feed['query']['recentchanges'][0]
            wiki.sync_timestamp = last_change['timestamp']
            wiki.synced = True
            self.invalidate_cache(session)
            session.commit()

    def _page_query(self, session):
        return session.query(cacheschema.Page)

    def invalidate_cache(self, session):
        """Invalidate the entire cache

        This marks all articles for re-downloading when requested.
        Note that articles with a current revision ID will not be re-downloaded
        entirely, only their metadata will be queried.
        (To clear the cache entirely, truncate the articles table.)
        """
        self._page_query(session).update({'up_to_date': False})
        session.commit()
