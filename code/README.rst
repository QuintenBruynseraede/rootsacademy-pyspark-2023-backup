.. image:: https://img.shields.io/badge/maintained%20by-dataroots-%2300b189
    :target: https://dataroots.io
    :alt: maintained by dataroots

.. image:: https://img.shields.io/badge/python-3.8-blue
    :target: https://img.shields.io/badge/python-3.8-blue
    :alt: Python version

.. image:: https://codecov.io/gh/datarootsio/skeleton-pyspark/branch/main/graph/badge.svg?token=UMPN2VL0AM
    :target: https://codecov.io/gh/datarootsio/skeleton-pyspark


PYSPARK ETL PIPELINE
====================

Objective
^^^^^^^^^

Implement an ETL pipeline using pyspark. 
We will combine multiple datasets containing football data to build a feature set, ready for model training.

Data
^^^^

A database(https://www.kaggle.com/hugomathien/soccer) of matches, together with:

    * The players that played
    * Info about those players (stats)
    * Info about the teams

26k matches, 11k players, 299 teams

Scope
^^^^^

* We need to create a feature set for this that can be used by a MLE (training a model is not in scope today)
* Target is a discrete variable with three values (classification): Tie, win, loss
* You can normalize data: each game yields two data points (e.g. a 'win' from the viewpoint of the home team, and a 'loss' from the viewpoint of the away team).
  For example: team A plays team B and the result is a 3-0 scoreline.
  We can use this one match to generate two datapoints: 

  +-------+-----------+-------+--------+--+--+--+--+--+--+
  | Home? | Win ratio | (...) | Target |  |  |  |  |  |  |
  +=======+===========+=======+========+==+==+==+==+==+==+
  | true  | 0.23      | (...) | win    |  |  |  |  |  |  |
  +-------+-----------+-------+--------+--+--+--+--+--+--+
  | false | 0.78      | (...) | loss   |  |  |  |  |  |  |
  +-------+-----------+-------+--------+--+--+--+--+--+--+

Exploratory Features:
^^^^^^^^^^^^^^^^^^^^^
* ``is_playing_home_game``:  whether this team is playing a home game (True | False)
* ``average_potential``: average potential of the players at the day the game is played
* ``target``: result of the game (WIN | LOSE | TIE)

Basic Features:
^^^^^^^^^^^^^^^
Add these features to the dataset after completing the previous section

* ``win_ratio``: win ratio of the team (number of wins / number of total games)
* ``has_high_potential_player``: does the team have a high potential player (>87) at the day the game is player

Advanced Features:
^^^^^^^^^^^^^^^^^^
* ``hist_win_ratio``: win ratio of the team until the game's date (number of wins / number of total games)
* ``recent_win_ratio``: win ratio of the team in the last 4 weeks before the game is played


Technical requirements
^^^^^^^^^^^^^^^^^^^^^^

* Store the final dataset as parquet to disk
* Use pytest and this repo as a starting point
* Follow software engineering best practices

  * Prevent hardcoded values ("magic constants")
  * All non-trivial functions should be covered by a unit test
  * The in- and output of your pipelines is covered by integration tests
* Push your code regularly to your personal branch

Usage
^^^^^

To start a pyspark notebook session::

    $ docker build -f notebooks/Dockerfile .
    $ docker run -p 8888:8888 -p 4040:4040 <image-id>

This makes a Jupyter server available at `localhost:8888 <localhost:8888>`_

To run the pyspark pipeline::

    $ docker build .
    $ docker run <image-id>
