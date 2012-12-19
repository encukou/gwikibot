from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy import (Column, ForeignKey, MetaData, PrimaryKeyConstraint,
    Table, UniqueConstraint)
from sqlalchemy.types import Unicode, Integer, Boolean, DateTime, PickleType
from sqlalchemy.orm import relationship


metadata = MetaData()
TableBase = declarative_base(metadata=metadata)

class Wiki(TableBase):
    __tablename__ = 'wikis'
    url_base = Column(Unicode, nullable=False, primary_key=True, info=dict(
        doc="MediaWiki API URL base, including the '?', e.g. 'http://en.wikipedia.org/w/api.php?'"))
    synced = Column(Boolean, nullable=True, info=dict(
        doc="If True, the cache is synced to the server."))
    sync_timestamp = Column(PickleType, nullable=False, info=dict(
        doc="timestamp for the next sync. (If None, cache will be invalidated.)"))
    last_update = Column(DateTime, nullable=True, info=dict(
        doc="Time of the last update."))

class Page(TableBase):
    __tablename__ = 'articles'
    wiki_id = Column(Unicode, ForeignKey('wikis.url_base'), primary_key=True, nullable=False, info=dict(
        doc="ID of the Wiki this article is part of"))
    title = Column(Unicode, primary_key=True, nullable=False, info=dict(
        doc="Title of the article"))
    contents = Column(Unicode, nullable=True, info=dict(
        doc="Textual contents of the article. NULL if there's no such article."))
    revision = Column(Integer, nullable=True, info=dict(
        doc="RevID of the article that `contents` reflect."))
    last_revision = Column(Integer, nullable=True, info=dict(
        doc="Last RevID of this article as of wiki.sync_timestamp, or NULL if unknown."))

    @property
    def up_to_date(self):
        lastrev = self.last_revision
        return lastrev is not None and lastrev == self.revision

    def __repr__(self):
        return '<Page {}: rev {}, last {}, content {}>'.format(
            self.title, self.revision, self.last_revision,
            self.contents is not None)

Page.wiki = relationship(Wiki)
