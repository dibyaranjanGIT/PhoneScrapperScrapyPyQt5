# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html


# myproject/items.py
import scrapy

class PhoneScrapperItem(scrapy.Item):
    url = scrapy.Field()
    phone_number_1 = scrapy.Field()
    country_1 = scrapy.Field()
    phone_number_2 = scrapy.Field()
    country_2 = scrapy.Field()
    phone_number_3 = scrapy.Field()
    country_3 = scrapy.Field()

