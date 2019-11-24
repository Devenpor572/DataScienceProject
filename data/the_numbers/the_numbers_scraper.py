import bs4
import requests
import csv
import os
import time
# There are 5860 movies at the time this program was written
PAGE_COUNT = 59


class SimpleLogger(object):
    def __init__(self, filename):
        self.file = open(filename, 'w')

    def __del__(self):
        self.file.close()

    def log(self, msg):
        print(msg)
        print(msg, file=self.file)


def parse_dollars(dollar_str):
    return int(dollar_str.replace('$', '').replace(',', ''))


def clean_input(input_str):
    return input_str.encode("ascii", errors="ignore").decode()


def scrape_movie(url_ext):
    page_str = 'https://www.the-numbers.com' + url_ext
    movie_rsp = requests.get(page_str)
    try:
        movie_rsp.raise_for_status()
    except requests.HTTPError:
        logger.log('Request for ' + page_str + ' failed!')
        return None
    parsed_movie_rsp = bs4.BeautifulSoup(clean_input(movie_rsp.text), features='html.parser')
    table = parsed_movie_rsp.find(text='Movie Details').parent.find_next('table')
    mpaa_rating = None
    running_time = None
    source = None
    genre = None
    production_method = None
    creative_type = None
    temp = table.find(text='MPAA Rating:')
    if temp:
        mpaa_rating = temp.parent.parent.parent.select('a')[0].getText()
    temp = table.find(text='Running Time:')
    if temp:
        temp = temp.parent.parent.find_next('td').getText()
        running_time = int(temp.split(' ')[0])
    temp = table.find(text='Source:')
    if temp:
        source = temp.parent.parent.parent.select('a')[0].getText()
    temp = table.find(text='Genre:')
    if temp:
        genre = temp.parent.parent.parent.select('a')[0].getText()
    temp = table.find(text='Production Method:')
    if temp:
        production_method = temp.parent.parent.parent.select('a')[0].getText()
    temp = table.find(text='Creative Type:')
    if temp:
        creative_type = temp.parent.parent.parent.select('a')[0].getText()
    return mpaa_rating, running_time, source, genre, production_method, creative_type


def scrape_the_numbers_generator():
    url_ext_lst = [''] + ['/' + str(x) + '01' for x in range(1, PAGE_COUNT)]
    for url_ext in url_ext_lst:
        count = 0
        page_str = 'https://www.the-numbers.com/movie/budgets/all' + url_ext
        movie_lst_rsp = requests.get(page_str)
        # Halt if there was an issue with the request
        try:
            movie_lst_rsp.raise_for_status()
        except requests.HTTPError:
            logger.log('Request for ' + page_str + ' failed!')
            continue
        else:
            logger.log('Request for ' + page_str + ' succeeded')
        parsed_movie_lst_rsp = bs4.BeautifulSoup(clean_input(movie_lst_rsp.text),
                                                 features="html.parser")
        table_cells = parsed_movie_lst_rsp.select('td')
        # There is an error in the source HTML, every <tr> tag ends in another <tr> tag instead of a </tr> tag.
        # This code is a workaround
        i = 0
        while i < len(table_cells):
            release_date = table_cells[i + 1].getText()
            title = table_cells[i + 2].getText()
            # Scrape the movie page
            movie_details = scrape_movie(table_cells[i + 2].select('a')[0]['href'])
            mpaa_rating = None
            running_time = None
            source = None
            genre = None
            production_method = None
            creative_type = None
            if movie_details and len(movie_details) == 6:
                mpaa_rating, running_time, source, genre, production_method, creative_type = movie_details
            production_budget = parse_dollars(table_cells[i + 3].getText())
            domestic_gross = parse_dollars(table_cells[i + 4].getText())
            worldwide_gross = parse_dollars(table_cells[i + 5].getText())
            yield (release_date, title, production_budget, domestic_gross, worldwide_gross, mpaa_rating,
                   running_time, source, genre, production_method, creative_type)
            count += 1
            i += 6
        logger.log('Read ' + str(count) + ' entries from page ' + page_str)


def store_the_numbers(filename):
    start_total = time.time()
    i = 1
    # We really don't want to overwrite any existing good DB file, it takes a long time to scrape all of the data
    with open(filename, 'w', newline='') as file:
        csv_file = csv.writer(file)
        csv_file.writerow(['Index', 'Release Date', 'Title', 'Production Budget', 'Domestic Gross', 'Worldwide Gross',
                           'MPAA Rating', 'Running Time', 'Source', 'Genre', 'Production Method', 'Creative Type'])
        start_row = time.time()
        for entry in scrape_the_numbers_generator():
            entry = (i,) + entry
            csv_file.writerow(entry)
            exec_time_row = time.time() - start_row
            logger.log('In {:.3f} seconds - '.format(exec_time_row) + str(entry))
            start_row = time.time()
            i += 1
    exec_time_total = time.time() - start_total
    logger.log('{} total entries in {:.3f} seconds'.format(i - 1, exec_time_total))


if __name__ == '__main__':
    filename = 'the_numbers.csv'
    if os.path.isfile(filename):
        exit()
    logger = SimpleLogger('log.txt')
    store_the_numbers(filename)
