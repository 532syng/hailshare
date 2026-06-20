This project implements a Discord bot (Python + SQLite) for one specific server, using UTC+7 time. Hailshare requests are grouped with any same route requests within meetup time +/- buffer minutes. A private channel is created for the trio for communication until 90 minutes after request meetup time.

## What it does
Provides slash commands:
-  **/request**: submit a hailshare request with date, time, from/to location, and buffer (15/30/60/90 mins). 
-  **/my_request**: show your current request and optionally cancel it.
-  **/leave_trio**: give up matched group and request.

Stores data in SQLite:
-  user requests (current, matched, cancelled)
-  trio channels and members
-  channel create/update event history
-  lightweight DB locks for scheduled tasks

## Request behavior
-  If user has no current request → inserts new one. 
-  If user has a future current request → asks whether to replace it.
-  If user’s existing current request is already past → auto-cancels old one and inserts new.

## Background jobs 
1.  cleanup_task (every 3 minutes)
    -  Cancels past current requests.
    -  Deletes expired hailshare channels.
2.  fill_existing_channels_task (every minute)
    -  Scans trio-... channels with <3 users.
    -  Parses meetup time from channel name.
    -  Finds compatible current requests using per-user buffer check.
    -  Adds matching user to channel permissions.
    -  Marks that request matched.
    -  Updates channel/member/event records in DB.
3.  create_channels_task (every minute)
    -  Reads all current requests.
    -  Groups by route (from_location, to_location).
    -  Builds trios where each user’s requested meetup is within their own buffer around the trio median meetup time.
    -  Creates private channel trio-YYYYMMDD_HHMM-<timestamp>.
    -  Sends greeting message with date/time/route.
    -  Marks trio requests matched.
    -  Stores channel + members + create event in DB.

## Constraints enforced
-  Location choices are fixed to a list of AABW locations and Central Saigon attractions for simplicity.
-  Buffer choices are fixed to 15/30/60/90.
-  Commands are restricted to the configured guild (GUILD_ID).
-  **At most one active hailshare channel per user** at any time is checked using:
    -  DB active-channel lookup
    -  Discord channel membership scan
