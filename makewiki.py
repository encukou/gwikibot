
from __future__ import print_function, unicode_literals

import os

from gwikibot import WikiCache
from pokedex.db import connect
from ptcgdex import tcg_tables
from mako.lookup import TemplateLookup
import gevent
import gevent.pool
from sqlalchemy.orm import (
    joinedload, joinedload_all, subqueryload, subqueryload_all)

url = 'http://localhost/api.php'


class WikiMaker(object):
    def __init__(self, cache, prefix='', session=None, spawn=None):
        self.cache = cache
        self.prefix = prefix
        self.session = session or connect()
        if spawn:
            self._spawner = spawn
            self.join_group = gevent.pool.Group()
        else:
            self._spawner = gevent.pool.Pool(size=1000)
            self.join_group = self._spawner

        basedir = os.path.dirname(os.path.abspath(__file__))
        templatedir = os.path.join(basedir, 'templates')
        self.template_lookup = TemplateLookup(
            directories=[templatedir],
            input_encoding='utf-8',
            output_encoding='utf-8')

    def spawn(self, func, *args, **kwargs):
        gevent.sleep(0)
        greenlet = self._spawner.spawn(func, *args, **kwargs)
        if self.join_group is not self._spawner:
            self.join_group.add(greenlet)
        return greenlet

    def render(self, filename, **args):
        args.setdefault('wm', self)
        template = self.template_lookup.get_template(filename)
        return template.render(**args)

    def card_page_title(self, card):
        sets = (p.set for p in card.prints)
        last_set = max(sets, key=lambda s: s.id)
        return '{}TCG card:{}, {}'.format(
            self.prefix, card.name, last_set.name)

    def set_page_title(self, tcg_set):
        return '{}TCG set:{}'.format(self.prefix, tcg_set.name)

    def handle_set(self, tcg_set):
        title = self.set_page_title(tcg_set)
        page = self.cache.get_editable(title)
        section = self.render('set.mako', tcg_set=tcg_set)
        if not page.exists:
            print('Create new page', title)
            page.edit(section)
        else:
            page.edit(section)

    def handle_card(self, card):
        title = self.card_page_title(card)
        page = self.cache.get_editable(title)
        section = self.render('card.mako', card=card)
        if not page.exists:
            print('Create new page', title)
            page.edit(section)
        else:
            page.edit(section)

    def run(self):
        Query = self.session.query

        query = Query(tcg_tables.Card).order_by(tcg_tables.Card.id)
        query = query.options(subqueryload_all('prints'))
        query = query.options(subqueryload_all('family.names'))
        for card in query:
            self.spawn(self.handle_card, card)

        query = Query(tcg_tables.Set).order_by(tcg_tables.Set.id)
        query = query.options(subqueryload_all('prints.card.family.names'))

        for tcg_set in query:
            self.spawn(self.handle_set, tcg_set)
        gevent.sleep(0)

        self.join_group.join()


WikiMaker(
    cache=WikiCache(url, verbose=True, limit=1),
    prefix='test1/',
    #session=connect(engine_args={'echo': True}),
).run()
