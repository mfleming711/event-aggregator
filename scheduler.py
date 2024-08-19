import pytz
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import subprocess

scheduler = BlockingScheduler()

def feverup_job():
    try:
        result = subprocess.run(['python3', 'feverup.py'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(result.stdout.decode())
    except subprocess.CalledProcessError as e:
        print(f"Command failed with error: {e.stderr.decode()}")

def ticketmaster_job():
    try:
        result = subprocess.run(['python3', 'ticketmaster.py'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(result.stdout.decode())
    except subprocess.CalledProcessError as e:
        print(f"Command failed with error: {e.stderr.decode()}")

def eventbrite_job():
    try:
        result = subprocess.run(['scrapy crawl', 'eventbrite'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(result.stdout.decode())
    except subprocess.CalledProcessError as e:
        print(f"Command failed with error: {e.stderr.decode()}")

def dice_job():
    try:
        result = subprocess.run(['scrapy', 'crawl', 'dice'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(result.stdout.decode())
    except subprocess.CalledProcessError as e:
        print(f"Command failed with error: {e.stderr.decode()}")

est = pytz.timezone('America/New_York')
feverup_trigger = CronTrigger(hour=5, minute=0, timezone=est)
ticketmaster_trigger = CronTrigger(hour=5, minute=10, timezone=est)
eventbrite_trigger = CronTrigger(hour=5, minute=20, timezone=est)
dice_trigger = CronTrigger(hour=5, minute=40, timezone=est)

scheduler.add_job(feverup_job, feverup_trigger)
scheduler.add_job(ticketmaster_job, ticketmaster_trigger)
scheduler.add_job(eventbrite_job, eventbrite_trigger)
scheduler.add_job(dice_job, dice_trigger)

scheduler.start()
