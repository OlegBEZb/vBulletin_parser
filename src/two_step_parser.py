import wget
from tqdm import tqdm
import re, os
import pandas as pd

from .selenium_utils import *
from .vbulletin_utils import VbulletinExtractor

from .vbulletin_utils import driver2soup

import logging
logger = logging.getLogger(__name__)
print("two_step_parser.py logger name: {}".format(__name__))


class TwoStepExtractor(VbulletinExtractor):
    def __init__(self, raw_data_path, target_file_path=None, **kwargs):
        super().__init__(**kwargs)
        self.raw_data_path = raw_data_path
        self.target_file_path = target_file_path

    @staticmethod
    def _get_topic_pages_num_from_subsection(topic):
        last_page_tag = topic.find("a", title=lambda title: title and title.startswith("Перейти"))
        logger.debug(f"last_page_tag: {str(last_page_tag)}")
        if last_page_tag is not None:  # this may occur if there is no "go to specific page in preview"
            logger.debug('enough pages')
            return int(re.findall('стр. ([\d]*)', last_page_tag.attrs['title'])[0])

        pagination = topic.find('dl', {'class': 'pagination'})
        if pagination is not None:
            logger.debug('use pagination \(few pages\)')
            return int(pagination.find('dd').find_all('span')[-1].text)

        logger.debug('only one page')
        return 1

    # TODO: rename to more specific
    # TODO: check if may be replaced
    # TODO: mb move to super
    def get_link(self, thread_num, page):
        return self.forum + 'showthread.php?t=' + str(thread_num) + '&page=' + str(page)

    def download_subsection_threads(self, subsection_url, subsection_pages=2, **kwargs):
        soup, driver = read_html_with_webdriver(url_to_read=subsection_url, headless=True)

        subsection_pages = min(subsection_pages, self._get_pages_num(thread_soup=soup))
        for subsection_page in tqdm(range(1, subsection_pages + 1), total=subsection_pages):
            logger.debug(f"{subsection_page} page in subsection")
            topics = soup.find('ol', {'id': 'threads'}).find_all('li', {'class': re.compile('threadbit hot *')})
            for topic in topics:
                # print('\n', '~' * 80)

                pages = self._get_topic_pages_num_from_subsection(topic)

                topic_name = topic.find('a', 'title').text
                href = topic.find('a', href=True).attrs['href']
                # print('href', href)
                topic_num = re.findall('\d{7}', href)[0]
                logger.info(f"topic_num: {str(topic_num)}, topic name: {topic_name}, pages: {str(pages)}")

                topic_local_dir = os.path.join(self.raw_data_path, str(topic_num))
                # extract a function
                if not os.path.exists(topic_local_dir):
                    os.makedirs(topic_local_dir)
                else:
                    logger.debug('already exists')
                    continue

                for i in range(1, pages + 1):
                    wget.download(self.get_link(thread_num=topic_num, page=i),
                                  out=os.path.join(topic_local_dir, str(i)),
                                  bar=None)
                logger.info('downloaded')

            # where is no page after the last one
            if subsection_page != subsection_pages:
                # why not to try a direct link like above?
                move_to_next_page(driver)
                soup = driver2soup(driver)
        driver.close()

    def process_thread(self,
                       thread_num,
                       return_df=False):
        result_df = pd.DataFrame()
        logger.info('thread_num', thread_num)

        if any('Thread_' + thread_num in s for s in os.listdir(self.target_file_path)):
            logger.debug('already processed')
            return None

        pages = os.listdir(os.path.join(self.raw_data_path, thread_num))
        for file in pages:
            all_info_from_page = self.get_all_info_from_file(os.path.join(self.raw_data_path, thread_num, file))
            result_df = pd.concat([result_df, all_info_from_page], ignore_index=True)

        if self.target_file_path is not None:
            result_df.to_csv(os.path.join(self.target_file_path, 'Thread_' + thread_num + \
                                          '_full_{}_pages.csv'.format(len(pages))),
                             index=False)
        if return_df:
            return result_df
        else:
            return 1
