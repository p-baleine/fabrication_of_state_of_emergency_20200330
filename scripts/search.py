#!/usr/bin/env python3

"""
キーワード「ロックダウン OR 緊急事態宣言 OR 4月1日」で検索される tweet を
Twitter API から検索して、data/search.db に格納します。
使い方:

    $ python scripts/search.py 2>&1 | tee  logs/search_$(date +'%s').log
"""

import click
import logging
from operator import attrgetter
import os
from pypika import Query, Table
import sqlite3
import time
import twitter
import urllib

logger = logging.getLogger(__name__)

# 検索のキーワード
DEFAULT_QUERY = ' OR '.join([
    'ロックダウン',
    '都市封鎖',
    '緊急事態宣言',
    '4月1日'])

# 環境変数に TWITTER_API_KEY とかが設定してある想定
api = twitter.Api(
    consumer_key=os.environ['TWITTER_API_KEY'],
    consumer_secret=os.environ['TWITTER_API_SECRET'],
    access_token_key=os.environ['TWITTER_ACCESS_TOKEN'],
    access_token_secret=os.environ['TWITTER_ACCESS_TOKEN_SECRET'])


def bulk_search(term, max_id, since, count, lang,
                sleep_seconds=15 * 60 // 180):
    """`count` 個の tweet のリストを毎回 yield します"""
    oldest_id = '1244640592367251456'  # Mon Mar 30 15:00:16 +0000 2020 の tweet
    # 1244636860384563200 ここから
    oldest_created_at = '2020-12-31'  # FIXME: 直値やめて

    while oldest_created_at > since:
        params = {'term': term, 'max_id': oldest_id,
                  'count': count, 'lang': lang}

        logger.debug('Call GET /search, current tweet\'s created_at: {}, '
                     'params: {}'.format(oldest_created_at, params))

        raw_result = api.GetSearch(**params)

        if len(raw_result) == 0:
            # FIXME: これだと結果が0の場合に無限ループしかしない
            continue

        result = list(sorted(raw_result, key=attrgetter('created_at')))
        # なんか知らないけど、古い方が落ちやすいので
        # 頭から20番目に古いところから次をトライする
        oldest_status = result[20 if len(result) > 20 else 0]
        oldest_id = oldest_status.id
        oldest_created_at = oldest_status.created_at

        yield result

        time.sleep(sleep_seconds)


def search(conn, term, since, count=100, lang='ja'):
    """`term` のキーワードで Twitter GET /search に問い合せた結果を
    conn の DB に保存します。`since` で指定された日付まで遡ろうとします。"""
    for result in bulk_search(term, None, since, count, lang):
        # conn をコンテキストマネージャーとして利用するとトランザクションに
        # 包んでくれると理解しているんだけど自信ない
        # https://docs.python.org/ja/3/library/sqlite3.html#using-the-connection-as-a-context-manager
        with conn:
            users = [s.user for s in result]
            conn.execute(insert_users_sql(users))
            conn.execute(insert_tweets_sql(result))


def replace_nul_str(text):
    # See: https://gist.github.com/jeremyBanks/1083518
    if type(text) is not str:
        return text
    encodable = text.encode('utf-8', 'ignore').decode('utf-8')
    if encodable.find('\x00') >= 0:
        logger.error('{} contains nul character.'.format(encodable))
        encodable = encodable.replace('\x00', '')
    return encodable


def insert_users_sql(users):
    field = ['id', 'created_at', 'description', 'followers_count',
             'friends_count', 'statuses_count', 'screen_name']
    getter = attrgetter(*field)
    return str(Query.into(Table('users'))
               .columns(field)
               # 面倒だから replace
               .replace(*[list(map(replace_nul_str, getter(u)))
                          for u in users]))


def insert_tweets_sql(tweets):
    field = ['id', 'created_at', 'retweet_count', 'favorite_count',
             'lang', 'text']
    getter = attrgetter(*field)
    return str(
        Query.into(Table('tweets'))
        .columns(field + ['tweeted_by', 'raw_json', 'retweeted_status'])
        .replace(*[list(map(replace_nul_str, getter(t)
                            + (t.user.id, str(t), str(t.retweeted_status))))
                   for t in tweets]))


def prepare_database(conn):
    conn.execute('''
create table users (
  id text not null primary key,
  created_at text not null,
  description text,
  followers_count integer,
  friends_count integer,
  statuses_count integer,
  screen_name text not null)
''')
    conn.execute('''
create table tweets (
  id text not null primary key,
  created_at text not null,
  retweet_count integer,
  favorite_count integer,
  lang text,
  text text,
  retweeted_status text,
  raw_json text,
  tweeted_by integer not null,
  foreign key(tweeted_by) references users(id))
''')
    conn.commit()


@click.command()
@click.option('--db', default='data/search.db')
def main(db):
    since = '2020-03-30 09:00'
    query = os.environ.get('QUERY', DEFAULT_QUERY)

    if not os.path.exists(db):
        with sqlite3.connect(db) as conn:
            prepare_database(conn)

    with sqlite3.connect(db) as conn:
        search(conn, urllib.parse.quote(query), since)


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s')
    logger.setLevel(level=logging.DEBUG)
    # logging.getLogger('twitter.api').setLevel(level=logging.DEBUG)
    main()
