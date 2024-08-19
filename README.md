# Welcome to Agora Events API!

This project contains the scrapers for ticketmaster, dice, feverup and eventbrite websites.
The scraped events will be added to the database.



## Installation
> pip3 install -r requirements.txt

All pip modules will be installed with this command.

## How to run the scraper

To run the scraper individually

> python3 ticketmaster.py
> python3 feverup.py
> scrapy crawl eventbrite
> scrapy crawl dice

To schedule the scraper run,
> python3 scheduler.py

This schedule will run the scrapers at 5 am EST everyday.


