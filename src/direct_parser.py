from tqdm import tqdm

from .vbulletin_utils import *
from .selenium_utils import *

def get_thread_msgs(thread_url, top_n_pages=5, save_path=None,
                    close_page=True, **kwargs):
    thread_soup, domain, driver = read_html_with_webdriver(thread_url, **kwargs)

    thread_num = re.findall('\d{7}', thread_url)[0]
    print('Thread num:', thread_num)

    if any('Thread_' + thread_num in filename
           for filename in os.listdir(save_path)):
        matching_files = [
            f for f in os.listdir(save_path) if 'Thread_' + thread_num in f
        ]
        print('The thread is already processed. Files:\n', matching_files)
        driver.close()
        print('Return the first one', matching_files[0])
        return pd.read_csv(os.path.join(save_path, matching_files[0]))

    result_df = pd.DataFrame()

    if top_n_pages == -1:
        print('top_n_pages = -1, so will scrap all pages')
        top_n_pages = get_pages_num(thread_soup)

    #         if top_n_pages < 1000:
    #             print('less than 1000 pages')
    #             driver.close()
    #             return None # mb return full file is exists

    if save_path is not None:
        save_frequency = get_save_freq(top_n_pages)

    for i in tqdm(range(1, top_n_pages + 1), total=top_n_pages,
                  mininterval=10):
        all_info_from_page = get_all_info_from_page(page_soup=thread_soup, forum=domain)
        result_df = pd.concat([result_df, all_info_from_page],
                              ignore_index=True)

        if save_path is not None:
            if i % save_frequency == 0:
                print("Saving after {}-th epoch".format(i))
                result_df.to_csv(os.path.join(
                    save_path, 'Thread_' + thread_num + '_{}_pages.csv'.format(i)),
                    index=False)

        # where is no page after the last one
        if i != top_n_pages:
            move_to_next_page(driver)
            thread_soup = driver2soup(driver)

    if close_page:
        driver.close()

    if save_path is not None:
        result_df.to_csv(os.path.join(
            save_path,
            'Thread_' + thread_num + '_full_{}_pages.csv'.format(top_n_pages)),
            index=False)
        temp_files = [
            file for file in os.listdir(save_path)
            if thread_num in file and 'full' not in file
        ]
        [os.remove(os.path.join(save_path, file)) for file in temp_files]

    return result_df


# TODO: a folder for the whole subsection may be created
def get_subsection_msgs(subsection_url, **kwargs):
    soup, domain, driver = read_html_with_webdriver(subsection_url)
    topics = soup.find('ol', {'id': 'threads'}).find_all('li', {'class': re.compile('threadbit hot *')})
    for topic in topics:
        print("parsing topic: ", topic.find('a', 'title').text)
        href = topic.find('a', href=True).attrs['href']
        get_thread_msgs('https://forum.littleone.ru/' + href, **kwargs);
    driver.close()
