""" File is intended for reading information from the website https://www.goodreads.com/
    and store the information inside the database using the ORM from orm.py """


import asyncio
import aiohttp
from bs4 import BeautifulSoup

from orm import *


class Library(BaseModel):
    id = Field('INTEGER PRIMARY KEY AUTOINCREMENT')
    book_title = Field('TEXT')
    link = Field("TEXT")
    author = Field("TEXT")
    rating = Field("TEXT")


class Parser():
    """Asynchronusly Parses Information from given website using AioHttp""" 
 
    def get_urls(self, url: str, url_count: int):
        """Get's the urls for the number of pages wanted"""

        try:
            list_urls = []
            for num in range(1, url_count+1):
                list_urls.append(f'{url}list/show/10198.Books_With_a_Goodreads_Average_Rating_of_4_5_and_above_and_With_At_Least_100_Ratings?page={num}')
            return list_urls
        except Exception:
            print("Page out of range")

    async def async_parse(self,list_urls):
        """Sends a request to the website"""

        session = aiohttp.ClientSession()
        for url in list_urls:
                async with session.get(url) as req:
                    data = await req.text()
                    self.async_parse_data(data)
        await session.close()

    def async_parse_data(self,single_html_page):
        """Reads information from html"""

        soup = BeautifulSoup(single_html_page, "lxml")
        books = soup.find_all("tr")
        library = Library()
        for book in books:
            book_name = book.find("a", {"class":"bookTitle"}).span.text
            book_link = f'https://www.goodreads.com/{book.find("a")["href"]}'
            book_author = book.find("a", {"class": "authorName"}).span.text
            book_rating = book.find("span", {"class":"minirating"}).text
            library.storage.insert(book_title=book_name, link=book_link, author=book_author, rating=book_rating)

    async def main(self,list_urls):
        await asyncio.gather(self.async_parse(list_urls))

    
if __name__ == "__main__":
    parse=Parser()
    list_urls = parse.get_urls("https://www.goodreads.com/", 1)
    asyncio.run(parse.main(list_urls))
    obj = Library()
    obj.storage.read()
        
