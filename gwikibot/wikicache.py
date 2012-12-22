import os
import datetime
import heapq
import itertools
import collections

import gevent
import requests
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

from gwikibot import cacheschema
from gwikibot import monkey

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

    :param url_base: Base URL of the MediaWiki API,
        e.g. 'http://en.wikipedia.org/w/api.php'
    :param db_path: Path to a SQLite file holding the cache, or SQLAlchemy
        database URL. If not given, a file next to the wikicache module will be
        used.
    :param limit: The cache will not make more than one request each `limit`
        seconds.

    Use the cache as a dictionary: ``cache[page_title]`` will give you a
    PageProxy object.
    """
    def __init__(
            self, url_base, db_url=None, force_sync=False, limit=5,
            verbose=False):

        self.verbose = verbose

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
        self.request_queue = Queue(0)

        self._updated = AsyncResult()

        gevent.spawn(self._request_loop, force_sync)
        gevent.sleep(0)

    def get_wiki(self):
        """Get the wiki object, creating one if necessary"""
        session = self._make_session()
        query = session.query(cacheschema.Wiki).filter_by(
            url_base=self._url_base)
        try:
            wiki = query.one()
        except (sqlalchemy.exc.OperationalError,
                sqlalchemy.orm.exc.NoResultFound):
            cacheschema.metadata.create_all(self._engine)
            wiki = cacheschema.Wiki()
            wiki.url_base = self._url_base
            wiki.sync_timestamp = None
            session.add(wiki)
            session.commit()
        wiki.session = session
        return wiki

    def log(self, string):
        """Log a message"""
        # TODO: Something more fancy
        if self.verbose:
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
        """Sleep before another request can be made

        The request rate is controlled by the "limit" attribute
        """
        sleep_seconds = self._sleep_seconds()
        if sleep_seconds > 0:
            self.log('Sleeping %ss' % sleep_seconds)
            gevent.sleep(sleep_seconds)

    def _apirequest_raw(self, **params):
        """Raw MW API request; returns Requests response"""

        self._sleep_before_request()

        try:
            result = requests.post(self._url_base, data=params, stream=True)
            self.log('POST {} {}'.format(result.url, params))
            result.raise_for_status()
            return result
        finally:
            self._next_request_time = (datetime.datetime.today() +
                    datetime.timedelta(seconds=self.limit))

    def apirequest(self, **params):
        """MW API request; returns result dict"""
        params['format'] = 'yaml'
        return yaml.safe_load(self._apirequest_raw(**params).text)

    def update(self, force_sync=True):
        """Fetch a batch of page changes from the server"""
        wiki = self.get_wiki()
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
            self.invalidate_cache(wiki)
            wiki.session.commit()
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
                        obj = self._page_object(wiki, title)
                        obj.last_revision = None
                        invalidated.add(title)
                wiki.session.commit()
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
                wiki.session.commit()
        wiki.last_update = datetime.datetime.today()
        wiki.session.commit()

    def _page_query(self, wiki):
        """Return a SQLA query for pages on this wiki"""
        return wiki.session.query(cacheschema.Page).filter_by(wiki=wiki)

    def _page_object(self, wiki, title):
        """Get an object for the page 'title', *w/o* adding it to the session
        """
        title = self.normalize_title(title)
        obj = wiki.session.query(cacheschema.Page).get((self._url_base, title))
        if obj:
            return obj
        else:
            obj = cacheschema.Page()
            obj.wiki = wiki
            obj.title = title
            obj.revision = None
            obj.last_revision = None
            return obj

    def _request_loop(self, force_sync):
        """The greenlet that requests needed metadata/pages
        """
        self.update(force_sync=force_sync)
        self._updated.set()

        requests = {}
        while True:
            self.log('Request loop active')

            while True:
                while not self.request_queue.empty():
                    self.request_queue.get().insert_into(requests)
                try:
                    request = self.request_queue.get(
                        timeout=self._sleep_seconds())
                except Empty:
                    break
                else:
                    request.insert_into(requests)

            request_list = [(k, v) for k, v in requests.items() if v]
            request_list.sort(key=lambda k_v: -len(k_v[1]))
            requests = dict(request_list)
            if request_list:
                for k, v in request_list[0][1].items():
                    v.run(requests)
                    break
            else:
                self.update(force_sync=False)
                gevent.sleep(self._sleep_seconds())


    def invalidate_cache(self, wiki):
        """Invalidate the entire cache

        This marks all articles for re-downloading when requested.
        Note that articles with a current revision ID will not be re-downloaded
        entirely, only their metadata will be queried.
        (To clear the cache entirely, truncate the articles table.)
        """
        self._page_query(wiki).update({'last_revision': None})
        wiki.session.commit()


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
        self._updated.get()
        wiki = self.get_wiki()
        title = result.title
        obj = self._page_object(wiki, title)
        wiki.session.add(obj)
        wiki.session.commit()
        # Make sure we know the page's last revision
        # This is a loop with rollbacks in it, since the DB can change under us
        while True:
            # Fetch metadata to see if the page has changed (or is empty!)
            if obj.last_revision is None or (not obj.up_to_date and
                    obj.contents is None):
                self.log('Requesting metadata for {}'.format(title))
                info = MetadataRequest(self, title, []).go()
            # Now, if metadata says we're out of date, actually fetch the page
            wiki.session.refresh(obj)
            if not obj.up_to_date:
                self.log('Requesting page {}'.format(title))
                PageRequest(self, title).go()
            # If everything was successful, notify the caller!
            wiki.session.refresh(obj)
            if obj.up_to_date:
                result._set_result(obj.contents)
                wiki.session.rollback()
                return

    def __getitem__(self, title):
        """Return the content of a page, if it exists, or raise KeyError
        """
        return self.get(title)


class Request(object):
    """A request to the remote server

    Requests can be grouped together by a "group key". All requests with
    the same group key can be gotten with the same API request.

    The cache's request-loop will take requests, and as soon as there's enough
    of them for an an API request, it does that request.
    If there's not enough requests for a while, it fires an "incomplete"
    request.
    """
    limit = 50

    def __init__(self, cache):
        self.cache = cache
        self.result = AsyncResult()
        self._subordinates = []

    def go(self):
        """Schedule the request and block until it's done"""
        self.cache.request_queue.put(self)
        return self.result.get()

    def insert_into(self, all_requests):
        """Insert this request into the given dict

        Run this from the request-loop greenlet; it can do the actual
        API request if enough requests have accumulated.
        How many are needed is specified in the "limit" variable.
        """
        peers = all_requests.setdefault(self.group_key, {})
        try:
            master = peers[self.key]
        except KeyError:
            peers[self.key] = self
        else:
            master._subordinates.append(self)
        if len(peers) >= self.limit:
            self.run(all_requests)

    @property
    def group_key(self):
        return (self, )

    @property
    def key(self):
        return self

    def run(self, all_requests):
        pass

    def _all_finished_requests(self, all_requests, key):
        master = all_requests.get(self.group_key, {}).pop(key, None)
        if master:
            yield master
            for s in master._subordinates:
                yield s


def powerset(iterable):
    "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
    s = list(iterable)
    return itertools.chain.from_iterable(
        itertools.combinations(s, r) for r in range(len(s)+1))


class MetadataRequest(Request):
    limit = 100

    def __init__(self, cache, title, token_requests):
        super(MetadataRequest, self).__init__(cache)
        self.title = title
        self.token_requests = tuple(sorted(token_requests))
        self.result = AsyncResult()

    @property
    def group_key(self):
        return MetadataRequest, self.token_requests

    @property
    def key(self):
        return self.title

    def _all_finished_requests(self, all_requests, key):
        # A bit more complicated since we can mark all requests with a subset
        # of our tokens as done
        mdr, token_requests = self.group_key
        for subset in powerset(token_requests):
            peers = all_requests.get((MetadataRequest, subset), {})
            master = peers.pop(key, None)
            if master:
                yield master
                for s in master._subordinates:
                    yield s

    def run(self, all_requests):
        wiki = self.cache.get_wiki()
        titles = all_requests[self.group_key].keys()
        # TODO: Fill up request if we can fetch more
        result = self.cache.apirequest(action='query', info='lastrevid',
                prop='revisions',  # should not be necessary on modern MW
                titles='|'.join(titles))
        assert 'normalized' not in result['query'], (
                result['query']['normalized'])  # XXX: normalization
        fetched_titles = []
        for page_info in result['query'].get('pages', []):
            title = page_info['title']
            page = self.cache._page_object(wiki, title)
            wiki.session.add(page)
            if 'missing' in page_info:
                page.last_revision = 0
                page.revision = 0
                page.contents = None
            else:
                revid = page_info['revisions'][0]['revid']
                # revid = page_info['lastrevid']  # for the modern MW
                page.last_revision = revid
            for p in self._all_finished_requests(all_requests, title):
                p.result.set(page_info)
        wiki.session.commit()


class PageRequest(Request):
    def __init__(self, cache, title):
        super(PageRequest, self).__init__(cache)
        self.title = title

    @property
    def group_key(self):
        return (PageRequest,)

    @property
    def key(self):
        return self.title

    def run(self, all_requests):
        wiki = self.cache.get_wiki()
        titles = all_requests[self.group_key].keys()

        dump = self.cache._apirequest_raw(action='query',
            export='1', exportnowrap='1',
            titles='|'.join(titles)).raw
        tree = ElementTree.parse(dump)
        for elem in tree.getroot():
            tag = elem.tag
            if tag.endswith('}siteinfo'):
                continue
            elif tag.endswith('}page'):
                revision, = (e for e in elem if e.tag.endswith('}revision'))
                pagename, = (e for e in elem if e.tag.endswith('}title'))
                text, = (e for e in revision if e.tag.endswith('}text'))
                revid, = (e for e in revision if e.tag.endswith('}id'))
                title = pagename.text
                page = self.cache._page_object(wiki, title)
                page.last_revision = int(revid.text)
                page.revision = int(revid.text)
                page.contents = text.text
                wiki.session.add(page)
                for p in self._all_finished_requests(all_requests, title):
                    p.result.set()
            else:
                raise ValueError(tag)
        wiki.session.commit()
