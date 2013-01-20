
from __future__ import print_function, unicode_literals

import os

from gwikibot import WikiCache
from pokedex.db import connect
from ptcgdex import tcg_tables
from mako.lookup import TemplateLookup
import gevent
import gevent.pool
from sqlalchemy.orm import (
    joinedload, joinedload_all, subqueryload, subqueryload_all, lazyload)

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
        moduledir = os.path.join(basedir, 'template_cache')
        self.template_lookup = TemplateLookup(
            directories=[templatedir],
            input_encoding='utf-8',
            output_encoding='utf-8',
            module_directory=moduledir,
        )

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
        print_ = min(card.prints, key=lambda p: p.set_id)
        return '{}TCG:{} ({} {})'.format(
            self.prefix, card.name, print_.set.name, print_.set_number)

    def set_page_title(self, tcg_set):
        return '{}TCG:{}'.format(self.prefix, tcg_set.name)

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
        prints = card.prints
        section = self.render('card.mako',
            card=card,
            first_print=min(prints, key=lambda p: p.set_id),
            last_print=max(prints, key=lambda p: p.set_id),
            prints=prints,
        )
        if not page.exists:
            print('Create new page', title)
            page.edit(section)
        else:
            page.edit(section)

        page = self.cache.get_editable(title + ' Release Info')
        if not page.exists:
            page.edit('.')

    def run(self):
        Query = self.session.query

        query = Query(tcg_tables.Card).order_by(tcg_tables.Card.id)
        query = query.join(tcg_tables.Card.family)
        query = query.options(joinedload_all('prints.set'))
        query = query.options(subqueryload('card_types'))
        query = query.options(joinedload_all('family.names_local'))
        query = query.options(joinedload_all('card_subclasses.subclass.names_local'))
        query = query.options(joinedload_all('prints.pokemon_flavor.flavor_local'))
        query = query.options(joinedload_all('prints.pokemon_flavor.species.names_local'))
        query = query.options(joinedload_all('card_mechanics.mechanic.effects_local'))
        query = query.options(joinedload_all('card_mechanics.mechanic.class_.names_local'))
        query = query.options(joinedload_all('card_types.type.names_local'))
        query = query.options(joinedload_all('stage.names_local'))
        query = query.options(joinedload_all('prints.illustrator'))
        query = query.options(joinedload_all('evolutions.family.names_local'))
        query = query.options(joinedload_all('family.evolutions.card.family.names_local'))
        query = query.options(joinedload_all('family.evolutions.card.prints.set.names_local'))
        query = query.options(joinedload_all('family.evolutions.card.card_mechanics.mechanic.names_local'))
        query = query.options(joinedload_all('card_mechanics.mechanic.costs.type.names_local'))
        query = query.options(joinedload_all('damage_modifiers.type.names_local'))
        query = query.options(subqueryload('card_mechanics.mechanic.costs'))
        query = query.options(subqueryload('family.evolutions'))
        query = query.options(subqueryload('family.evolutions.card.prints'))
        query = query.options(subqueryload('family.evolutions.card.card_mechanics'))
        query = query.options(subqueryload('family.cards.prints'))
        query = query.options(subqueryload('prints.scans'))
        query = query.options(lazyload('prints.pokemon_flavor.species.default_pokemon'))
        query = query.options(lazyload('prints.pokemon_flavor.species.evolutions'))
        query = query.options(lazyload('prints'))
        query = query.options(lazyload('card_mechanics'))
        query = query.options(lazyload('damage_modifiers'))
        query = query.options(lazyload('card_subclasses'))

        query = query.filter(tcg_tables.CardFamily.identifier.in_(
            'zapdos'.split()
        ))

        for card in query:
            self.spawn(self.handle_card, card)

        query = Query(tcg_tables.Set).order_by(tcg_tables.Set.id)
        query = query.options(subqueryload_all('prints.card.family.names'))

        query = query.filter(tcg_tables.Set.id == None)

        for tcg_set in query:
            self.spawn(self.handle_set, tcg_set)
        gevent.sleep(0)

        self.join_group.join()


WikiMaker(
    cache=WikiCache(url, verbose=True, limit=0),
    prefix='',
    #session=connect(engine_args={'echo': True}),
).run()
