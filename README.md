# `themuserb`: RB's Submission to the Recruitment Execise by "The Muse"


## Prerequisites

* a postgresql server must be accessible, together with a shared directory
  that can be seen by both the server and the machine you're running
  `themuserb` from.

* a python environment is required (version `3.5.2` was used for
  development/testing, other versions may well work).  the following
  packages must be importable from your python environment:

    - [psycopg2](http://initd.org/psycopg/)    
    - [requests](http://docs.python-requests.org/en/master/)


## How to Run the Code

```
python3 themuserb.zip 01_create_schema localhost mydfltusr mydfltpwd mydfltusr
python3 themuserb.zip 02_download_data /tmp 1
python3 themuserb.zip 03_load_data localhost mydfltusr mydfltpwd mydfltusr /tmp
python3 themuserb.zip 04_query localhost mydfltusr mydfltpwd mydfltusr
```

Some comments:

* Replace `localhost` by the tcp/ip hostname of the host that runs
  the postgresql database.
* Replace `mydfltusr`/`mydfltpwd` with the user/password that should
  be used for connecting to postgresql.
* Replace the second occurence of `mydfltusr` with the name of the
  database you'd like to connect to (by default the same as the user name).
* For step 2 (`02_download_data`), replace `/tmp` with a writable directory.
  the script will place some CSV files there for later import by the server.
* For step 2 (`02_download_data`), the parameter `1` in the above example
  means that at most one page will be downloaded.  Omit this parameter to
  download as many pages as the API offers.
* For step 3 (`03_load_data`), replace `/tmp` with a directory that's
  readable by the postgresql server and contains the CSV files created
  in step 2.


## How to Inspect the Code

Just unpack `themuserb.zip`.


## How to Modify the Code

If you need to change the code, then unpack `themuserb.zip` and modify
the individual source files as required.  For a faster development cycle,
it would be more convenient to run things through a command line along
the lines of `PYTHONPATH="." python3 ./__main__.py 02_download_data ...`
instead of `python3 themuserb.zip 02_download_data`.  When you're finished
and you'd like to make a deliverable just call `pack.sh`.


## Notes on Architecture/Style

This repository follows an
[assertive](https://en.wikipedia.org/wiki/Assertion_(software_development))
style of programming.

When creating an interface between a weakly typed
philosophy of data modelling as encapsulated through the *The Muse API*
and a strongly typed
philosophy of data modelling as is required for relational databases,
then the relational data modelling is based on a whole bunch of assumptions.

When I do something like this, I'd like to make sure that when those
assumptions are violated, I force my program to exit with a failure of
some kind (usually a violated assertion), rather than tolerating
incorrect assumption to propagate into the system through silent failure.

This assertive programming style is also the method whereby I actually
explore the data: I will look at it, formulate an assumption, make the
assumption explicit through assertions, and when it turns out that those
assertions are violated, then I need to reevaluate my assumptions, until
I arrive at a set of assumptions that are justified based on the data.

This assertive programming business is to some extent a pain in the ass:
A big part of the reason for why people like weakly typed data modelling,
data with self-describing schemas and so on, is because they anticipate
that the schema will be subject to change over time.  Whenever such
change occurs, this is likely to break my code and require changes to it.
But I'd rather be alerted to changes in the data model, giving me the
opportunity to think about all of its implications rather than suppress
errors and have my data potentially turn into garbage.

So, I'd like to say in my defense:  When you're running my code, and it
breaks, and you can see an opportunity that the code could have been more
"robust" to that type of failure, then think twice about whether you're
looking at a bug or at a feature.


## Notes on Limitations

My code doesn't take into account the content fields in the data, as they
are logistically a pain and aren't all that useful for showing off my
data engineering skill in the context of this exercise.  If the exercise
had been to build a search engine index to make the content fields
searchable using some smart natural language processing etc. then the
situation would have been different, of course.

Also: My code assumes that data gets read from the API and writte on the DB
in a "one shot" fashion.  In real-life scenarios, it would be more likely
that one would want the ability to incrementally add new data to a database
that already contains a bunch of data, raising problems of deduplication etc.
Again, I didn't think it necessary to go into the complexity of this, in
order to show off my data engineering skills.  We can discuss in the
interview what would be involved in implementing such a feature.
