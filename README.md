eurostat
========

To run the scraper, copy ``config-template.py`` to ``config.py`` and edit
the database URI according to your local environment.

Make sure the dependencies are installed (see requirements.txt).

Then run

    python run.py

When running without further arguments, the script will fetch a list
of all tables from Eurostat and import every table that has new updates.

You can add a table id as argument to download only one table.

   python run.py teiis500
