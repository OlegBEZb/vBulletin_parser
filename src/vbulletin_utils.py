from bs4 import BeautifulSoup
import os
import itertools
import pandas as pd
import re
from dateutil.parser import parse
import time
from datetime import datetime, timedelta

import logging
logger = logging.getLogger(__name__)
print("vbulletin_parser.py logger name: {}".format(__name__))


def flatten_list(ls):
    return list(itertools.chain.from_iterable(ls))


def driver2soup(driver):
    return BeautifulSoup(markup=driver.page_source, features="lxml")


class VbulletinExtractor:
    def __init__(self, forum):
        self.forum = forum

    @staticmethod
    def _get_posts_from_page(soup):
        posts_ol = soup.find('ol', {'id': 'posts'})
        assert posts_ol is not None, 'Posts not found on page. Soup:\n{}'.format(soup)
        return posts_ol.find_all('li', {'class': 'postbitlegacy postbitim postcontainer old'})

    @staticmethod
    def _get_post_text(post):
        return post.find('div', 'postdetails').find('div', 'content').find('blockquote', 'postcontent restore').text

    # def get_post_texts_from_page(self, soup):
    #     posts = self._get_posts_from_page(soup)
    #     return [self._get_post_text(post) for post in posts]

    @staticmethod
    def _get_user_extra_info(post):
        extra_info = post.find('dl', {'class': 'userinfo_extra'})
        # print('extra_info', extra_info)
        if extra_info is None:
            return {'guest': True}
        else:
            info_dict = dict(
                zip(
                    list(map(lambda x: x.text,
                             extra_info.find_all("dt"))),  # Extra field names
                    list(map(lambda x: x.text.replace('\xa0', ''),
                             extra_info.find_all("dd")))  # Extra field names
                ))
            info_dict.update({'guest': False})
            return info_dict

    def _get_user_info_from_post(self, post):
        user_info = dict()
        user_info['user_title'] = post.find('span', {'class': 'usertitle'}).text
        try:
            user_info['user_name'] = post.find('div',
                                               {'class': 'username_container'}).find("strong").text
            user_info['user_link'] = self.forum + post.find('div', {'class': 'username_container'}).find("a").attrs[
                'href']
        except:
            user_info['user_name'] = post.find('span', {
                'class': 'username guest'
            }).text
            # print('!'*100, post)
        user_info.update(self._get_user_extra_info(post))
        return user_info

    @staticmethod
    def _parse_date(date_string, file_path=None):
        try:
            return parse(date_string, dayfirst=True)
        except:
            #         print('failed to parse date using dateutil.parser') # debug
            try:
                if file_path is not None:
                    file_created_date = time.ctime(os.path.getctime(file_path))
                    file_created_date = parse(file_created_date).date()
                else:
                    file_created_date = parse(time.ctime()).date()
                post_time = datetime.strptime(
                    date_string.split('\xa0')[-1], '%H:%M').time()
                post_datetime = datetime.combine(file_created_date, post_time)
                if re.findall('вчера', date_string):
                    post_datetime -= timedelta(days=1)
                elif re.findall('сегодня', date_string):
                    pass
                else:
                    raise ValueError('Unexpected characters in date_string:',
                                     date_string)

                #             print('file was created', file_created_date,
                #                   '\ndate_string', date_string,
                #                   '\npost_time', post_time,
                #                   '\npost_datetime', post_datetime, f"D{post_datetime.day},M{post_datetime.month},Y{post_datetime.year}")
                return post_datetime
            except:
                raise ValueError('Unreadable format of date:', date_string)

    def _get_info_from_post_header(self, post, file_path=None):
        header_info = dict()
        header_info['post_datetime'] = self._parse_date(post.find(
            'span', {'class': 'date'}).text, file_path=file_path)

        div = post.find('div', {'class': 'posthead'}).find("div")
        # print(div, '\n')
        if div is not None:
            try:
                header_info['answer_to_user'] = re.findall(
                    'ответ для ([\S\s]*) , на сообщение « ', div.text)[0]

                header_info['answer_to_post_head'] = re.findall(
                    ' , на сообщение « ([\S\s]*) »', div.text)[0]

                links = div.find_all('a', href=True)
                if len(links) == 2:
                    header_info['answer_to_user_link'], header_info['answer_to_post_link'] \
                        = [self.forum + link.attrs['href'] for link in links]
                else:
                    header_info[
                        'answer_to_post_link'] = self.forum + links[0].attrs['href']
            except:
                print('!' * 20, '\n', div, '\n', file_path, '\n', '!' * 20)  # debug

        return header_info

    def get_all_info_from_page(self, page_soup, file_path=None):
        posts = self._get_posts_from_page(page_soup)

        page_texts = [self._get_post_text(post) for post in posts]
        texts_df = pd.DataFrame({'post_text': page_texts})

        users_info = [self._get_user_info_from_post(post) for post in posts]
        users_info_df = pd.DataFrame(users_info)

        header_info = [self._get_info_from_post_header(post=post, file_path=file_path) for post in posts]
        header_info_df = pd.DataFrame(header_info)

        return pd.concat([texts_df, users_info_df, header_info_df], axis=1)  # .astype({'Сообщений': int}) NaNs occurs

    # Add functionality for continuing stopped thread

    def get_all_info_from_file(self, file_path):
        with open(file_path, 'r') as f:
            contents = f.read()
            soup = BeautifulSoup(contents, features="lxml")
            all_info_from_page = self.get_all_info_from_page(soup, file_path)
            return all_info_from_page

    @staticmethod
    def _get_save_freq(n_pages):
        if n_pages < 1000:
            return 500
        elif n_pages < 10000:
            return 5000
        else:
            return 10000

    @staticmethod
    def _get_pages_num(thread_soup):
        # it seems that from the moment of first development the engine changed a bit
        #     last_page_tag = thread_soup.find(
        #         "a", title=lambda title: title and title.startswith("Последняя"))
        #     top_n_pages = int(re.findall('из ([\d\s]*)',
        #                                  last_page_tag.attrs['title'])[0].replace(u'\xa0', ''))
        # current version
        try:
            pages_regex = 'Страница [\d]* из ([\d]*)'
            pages_tag = thread_soup.find('td', text=re.compile(pages_regex))
            top_n_pages = int(re.findall(pages_regex, pages_tag.text)[0])
            logger.info('found pages', top_n_pages)
        except:
            top_n_pages = 1
            logger.info('Pagination wasn\'t foudn on the page. Load only one')

        return top_n_pages

    # def _get_user_info_from_page(self, soup):
    #     posts = self._get_posts_from_page(soup)
    #     return [self._get_user_info_from_post(post) for post in posts]
