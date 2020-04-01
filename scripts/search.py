#!/usr/bin/env python3

"""
キーワード「ロックダウン OR 緊急事態宣言 OR 4月1日」で検索される tweet を
Twitter API から検索して、データベース に格納します。
使い方:

    $ python scripts/search.py 2>&1 | tee log/search_$(date +'%s').log
"""

import click
import logging
import mysql.connector
from operator import attrgetter
import os
from pypika import MySQLQuery as Query, Table
from pypika.terms import Values
import time
import twitter
import unicodedata
import urllib

logger = logging.getLogger(__name__)

# 検索のキーワード
DEFAULT_QUERY = ' OR '.join([
    'ロックダウン',
    '都市封鎖',
    '緊急事態宣言',
    # '4月1日' # これ、「殿様」の 180,000 リツイートを含んでしまうため、とりあえず除く
])

# 環境変数に TWITTER_API_KEY とかが設定してある想定
api = twitter.Api(
    consumer_key=os.environ['TWITTER_API_KEY'],
    consumer_secret=os.environ['TWITTER_API_SECRET'],
    access_token_key=os.environ['TWITTER_ACCESS_TOKEN'],
    access_token_secret=os.environ['TWITTER_ACCESS_TOKEN_SECRET'])

database_config = {
    'user': os.environ['FABRICATION_DB_USER'],
    'password': os.environ['FABRICATION_DB_PASSWORD'],
    'host': os.environ['FABRICATION_DB_HOST'],
    'port': os.environ['FABRICATION_DB_PORT'],
    'database': os.environ['FABRICATION_DB_NAME'],
    'charset': 'utf8mb4',
}


def bulk_search(term, max_id, since, count, lang,
                sleep_seconds=15 * 60 // 180):
    """`count` 個の tweet のリストを毎回 yield します"""
    oldest_id = '1244640592367251456'  # Mon Mar 30 15:00:16 +0000 2020 の tweet
    oldest_created_at = '2020-12-31'  # FIXME: ここらへん、直値やめて

    while oldest_created_at > since:
        params = {'term': term, 'max_id': oldest_id,
                  'count': count, 'lang': lang}

        start_time = time.time()
        logger.debug('Call GET /search, current tweet\'s created_at: {}, '
                     'params: {}'.format(oldest_created_at, params))

        raw_result = api.GetSearch(**params)
        elapsed_time = time.time() - start_time
        logger.debug(
            'Fetched {} tweets, elapsed time: {} sec({} tweets/sec)'.format(
                len(raw_result), elapsed_time,
                len(raw_result) / elapsed_time))

        if len(raw_result) == 0:
            # FIXME: これだと処理がおわっちゃう…
            raise Exception('Empty result.')

        result = list(sorted(raw_result, key=attrgetter('created_at')))
        # なんか知らないけど、古い方が落ちやすいので
        # 頭から20番目に古いところから次をトライする
        oldest_status = result[20 if len(result) > 20 else 0]
        oldest_id = oldest_status.id
        oldest_created_at = oldest_status.created_at

        yield result

        time.sleep(sleep_seconds)


def search(cnx, term, since, count=100, lang='ja'):
    """`term` のキーワードで Twitter GET /search に問い合せた結果を
    cnx の DB に保存します。`since` で指定された日付まで遡ろうとします。"""
    logger.info('Search tweets...')

    for result in bulk_search(term, None, since, count, lang):
        users = [s.user for s in result]
        sqls = [
            insert_users_sql(users),
            insert_tweets_sql(result),
        ]
        cursor = cnx.cursor()

        try:
            for sql in sqls:
                logger.debug(format_sql_for_logging(sql))
                cursor.execute(sql)
            cnx.commit()
        except Exception as e:
            cnx.rollback()
            raise e
        finally:
            cursor.close()


def replace_nul_str(text):
    # See: https://gist.github.com/jeremyBanks/1083518
    encodable = text.encode('utf-8', 'ignore').decode('utf-8')
    if encodable.find('\x00') >= 0:
        logger.error('{} contains nul character.'.format(encodable))
        encodable = encodable.replace('\x00', '')
    return encodable


def normalize(text):
    if type(text) is not str:
        return text
    return unicodedata.normalize('NFD', replace_nul_str(text))


def insert_users_sql(users):
    field = ['id', 'description', 'followers_count',
             'friends_count', 'statuses_count', 'screen_name']
    getter = attrgetter(*field)
    table = Table('users')

    def params_of(user):
        created_at = ctime_to_mysql_datetime(user.created_at)
        values = getter(user) + (created_at,)
        return tuple(map(normalize, values))

    return str(
        Query.into(table)
        .columns(field + ['created_at'])
        .insert(*[params_of(u) for u in users])
        .on_duplicate_key_update(
            table.screen_name, Values(table.screen_name)))


def insert_tweets_sql(tweets):
    field = ['id', 'retweet_count', 'favorite_count',
             'lang', 'text']
    extra_field = ['tweeted_by', 'raw_json',
                   'retweeted_status', 'created_at']
    getter = attrgetter(*field)
    table = Table('tweets')

    def params_of(tweet):
        retweeted_status = str(tweet.retweeted_status) \
            if tweet.retweeted_status else None
        raw_json = str(tweet)
        created_at = ctime_to_mysql_datetime(tweet.created_at)
        values = getter(tweet) + \
            (tweet.user.id, raw_json, retweeted_status, created_at)
        return list(map(normalize, values))

    return str(
        Query.into(table)
        .columns(field + extra_field)
        .insert(*[params_of(t) for t in tweets])
        .on_duplicate_key_update(
            table.lang, Values(table.lang)))


def ctime_to_mysql_datetime(text):
    return time.strftime(
        '%Y-%m-%d %H:%M:%S',
        time.strptime(text, '%a %b %d %H:%M:%S +0000 %Y'))


def format_sql_for_logging(sql, truncate_at=200):
    flatten_sql = sql.replace('\n', ' ')
    return 'SQL: {}'.format(
        (flatten_sql[:truncate_at] + '..')
        if len(sql) > truncate_at else sql)


def prepare_tables(cnx):
    cursor = cnx.cursor()
    cursor.execute('''
create table if not exists users (
  id bigint auto_increment not null primary key,
  created_at datetime not null,
    description varchar(1200),
  followers_count integer,
  friends_count integer,
  statuses_count integer,
  screen_name varchar(100) not null)
''')
    cursor.execute('''
alter table users add index idx_users_screen_name(screen_name)
    ''')
    cursor.execute('''
create table if not exists tweets (
  id bigint auto_increment not null primary key,
  created_at datetime not null,
  retweet_count integer,
  favorite_count integer,
  lang varchar(10),
  text varchar(5000),
  retweeted_status text,
  raw_json text,
  tweeted_by bigint not null,
  foreign key(tweeted_by) references users(id))
''')
    cursor.execute('''
alter table tweets add index idx_tweets_created_at(created_at)
    ''')


@click.command()
@click.option('--reset-db', default=False)
def main(reset_db):
    since = '2020-03-29 15:00'
    query = os.environ.get('QUERY', DEFAULT_QUERY)

    cnx = mysql.connector.connect(**database_config)

    if reset_db:
        logger.info('Dropping tables...')
        cursor = cnx.cursor()
        try:
            cursor.execute('drop table if exists tweets')
            cursor.execute('drop table if exists users')
        except Exception as e:
            cursor.close()
            cnx.close()
            raise e

        logger.info('Preparing tables...')
        try:
            prepare_tables(cnx)
        except Exception as e:
            cnx.close()
            raise e

    try:
        search(cnx, urllib.parse.quote(query), since)
    finally:
        cnx.close()

    logger.info('Successfully finished!!')


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s')
    logger.setLevel(level=logging.DEBUG)
    # logging.getLogger('twitter.api').setLevel(level=logging.DEBUG)
    main()
