from multiprocessing import Pool, cpu_count
from datetime import datetime, timedelta
import time
from functools import partial
import requests
from bs4 import BeautifulSoup


def get_main_date():
    """
    Retrieve the main date displayed on the New York Times Best Sellers main page.

    Returns:
        datetime: A datetime object representing the date obtained from the website's main page.

    Raises:
        Exception: If there is an error in the HTTP request or parsing the date.
    """
    url = "https://www.nytimes.com/books/best-sellers"
    response = requests.get(url, timeout=5)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        # get date from the main page
        time_elements = soup.find('time', class_='css-6068ga').text
        time_elements = datetime.strptime(time_elements, '%B %d, %Y')
        return time_elements

    raise Exception(f"Error response: {response.status_code}, {response.text}")


def scrape_best_seller(week, date):
    """
    Scrape best-selling book data for a specific week preceding the provided date.

    Args:
        week (int): The number of weeks before the provided date.
        date (datetime): A datetime object representing the reference date.

    Returns:
        dict: A dictionary containing the following data:
            - 'week': The week number.
            - 'data': A list of dictionaries, where each dictionary contains information
                about a book category and its best-selling books, including title, description, and image URL.

    Raises:
        Exception: If there is an error in the HTTP request or parsing the book data.
    """
    best_seller = {
        'week': week,
        'data': []
    }

    weeks_back = date - timedelta(days=7 * week)
    formatted_date = weeks_back.strftime("%Y/%m/%d")

    url = f"https://www.nytimes.com/books/best-sellers/{formatted_date}/"
    response = requests.get(url, timeout=5)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        div_elements = soup.find_all('div', attrs={'itemscope': True})[0]
        category_elements = div_elements.find_all('div', class_='css-v2kl5d')

        for category in category_elements:
            books_data = []

            category_name = category.find("h2").text
            book_elements = category.find_all('li', class_='css-1mr03gh')

            for position, book_element in enumerate(book_elements, start=1):
                title = book_element.find('h3', class_='css-i1z3c1').text.strip()
                description = book_element.find('p', class_='css-5yxv3r').text.strip()
                image_url = book_element.find('img')['src']

                data = {
                    'position': position,
                    'title': title,
                    'description': description,
                    'image_url': image_url
                }

                books_data.append(data)

            best_seller['data'].append({
                'category': category_name,
                'books': books_data
            })

        return best_seller
    raise Exception(f"Error response: {response.status_code}, {response.text}")


if __name__ == "__main__":
    num_processes = cpu_count()
    current_date = get_main_date()
    start = time.time()

    with Pool(num_processes) as pool:
        weeks = list(range(105))
        results = pool.map(partial(scrape_best_seller, date=current_date), weeks)
        end = time.time()
        print(results)
        print(end - start)
