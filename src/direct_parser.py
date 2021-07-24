import logging
import os
import re

import pandas as pd
from tqdm import tqdm

logger = logging.getLogger(__name__)
print("direct_parser.py logger name: {}".format(__name__))

from .vbulletin_utils import VbulletinExtractor
from .selenium_utils import *


class DirectExtractor(VbulletinExtractor):
    def __init__(self, target_file_path=None, **kwargs):
        super().__init__(**kwargs)
        self.target_file_path = target_file_path

    def get_thread_msgs(self, thread_url, top_n_pages=5, save=True,
                        close_page=True, **webdriver_kwargs):
        thread_soup, driver = read_html_with_webdriver(thread_url, **webdriver_kwargs)

        thread_num = re.findall('\d{7}', thread_url)[0]
        logger.info(f"Thread num: {thread_num}")

        matching_files = [f for f in os.listdir(self.target_file_path) if 'Thread_' + thread_num in f]
        if len(matching_files) > 0:
            logger.info(f"The thread is already processed")
            logger.debug(str(matching_files))
            driver.close()
            logger.debug(f"Return the first one, {matching_files[0]}")
            return pd.read_csv(os.path.join(self.target_file_path, matching_files[0]))

        if top_n_pages == -1:
            logger.debug('top_n_pages = -1, so will scrap all pages')
            top_n_pages = self._get_pages_num(thread_soup)

        if save:
            save_frequency = self._get_save_freq(top_n_pages)

        result_df = pd.DataFrame()
        for i in tqdm(range(1, top_n_pages + 1), total=top_n_pages, mininterval=10):
            try:
                all_info_from_page = self.get_all_info_from_page(page_soup=thread_soup)
            except:
                pass
            #     log_in(driver)
            #     all_info_from_page = self.get_all_info_from_page(page_soup=thread_soup)

            result_df = pd.concat([result_df, all_info_from_page], ignore_index=True)

            if save:
                if i % save_frequency == 0:
                    logger.debug("Saving after {}-th epoch".format(i))
                    result_df.to_csv(self._get_filename(thread_num, i), index=False)

            # where is no page after the last one
            if i != top_n_pages:
                move_to_next_page(driver)
                thread_soup = driver2soup(driver)

        if close_page:
            driver.close()

        if save:
            result_df.to_csv(self._get_filename(thread_num, top_n_pages, is_full=True), index=False)
            temp_files = [
                file for file in os.listdir(self.target_file_path)
                if thread_num in file and '_full' not in file
            ]
            [os.remove(os.path.join(self.target_file_path, file)) for file in temp_files]

        return result_df

    def _get_filename(self, thread_num, pages, is_full=False):
        filename = 'Thread_' + thread_num
        if is_full:
            filename += '_full'
        filename += '_{}_pages.csv'.format(pages)
        return os.path.join(self.target_file_path, filename)

    # TODO: a folder for the whole subsection may be created
    def get_subsection_msgs(self, subsection_url, **webdriver_kwargs):
        soup, driver = read_html_with_webdriver(subsection_url)
        topics = soup.find('ol', {'id': 'threads'}).find_all('li', {'class': re.compile('threadbit hot *')})
        for topic in topics:
            logger.info("parsing topic: ", topic.find('a', 'title').text)
            href = topic.find('a', href=True).attrs['href']
            self.get_thread_msgs(self.forum + href, **webdriver_kwargs);
        driver.close()
