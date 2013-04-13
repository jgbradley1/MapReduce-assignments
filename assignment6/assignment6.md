Assignment 6 - Josh Bradley
====================
<ul>
<li>
I was able to complete the assignment.
```printf()```
function.

<br><br>Pig Script - Analysis #1
<pre>
-- Load data
A = load '/user/shared/tweets2011/tweets2011.txt' as (id:chararray, timestamp:chararray, user:chararray, message:chararray);

-- Parse out the timestamp
B = foreach A generate SUBSTRING(timestamp, 4, 7) as month, SUBSTRING(timestamp, 8, 10) as day, SUBSTRING(timestamp, 11, 13) as hour;

-- Filter out unneccessary data
CLEAN_DATA = filter B by month matches 'Jan|Feb';
B = CLEAN_DATA;

C = foreach B generate REPLACE(month, 'Jan', '1') as month, day, hour;
D = foreach C generate REPLACE(month, 'Feb', '2') as month, day, hour;

-- Produce counts of tweets of each hour by each day
E = group D by (month, day, hour);
F = foreach E generate group, COUNT(D) as count;

-- Format output to be pretty
G = foreach F generate CONCAT( CONCAT(group.month, '/'), group.day) as date, group.hour, count;
store G into 'hourly-counts-all.txt';
</pre>

<br>Pig Script - Analysis #2
<pre>
-- Load data
DATA = load '/user/shared/tweets2011/tweets2011.txt' as (id:chararray, timestamp:chararray, user:chararray, message:chararray);

-- filter out tweets that mention "egypt" or "cairo"
A = filter DATA by message matches '.*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).*';

-- Parse out the timestamp
B = foreach A generate SUBSTRING(timestamp, 4, 7) as month, SUBSTRING(timestamp, 8, 10) as day, SUBSTRING(timestamp, 11, 13) as hour;

-- Filter out unneccessary data
CLEAN_DATA = filter B by month matches 'Jan|Feb';
B = CLEAN_DATA;

C = foreach B generate REPLACE(month, 'Jan', '1') as month, day, hour;
D = foreach C generate REPLACE(month, 'Feb', '2') as month, day, hour;

-- Produce counts of tweets of each hour by each day
E = group D by (month, day, hour);
F = foreach E generate group, COUNT(D) as count;

-- Format output to be pretty
G = foreach F generate CONCAT( CONCAT(group.month, '/'), group.day) as date, group.hour, count;
store G into 'hourly-counts-egypt-cairo.txt';
</pre>
</li>
</ul>

