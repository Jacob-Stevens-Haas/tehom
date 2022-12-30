.. tehom documentation master file, created by
   sphinx-quickstart on Sat Jun 19 12:58:11 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root ``toctree`` directive.

Tehom - Machine Learning on Underwater Acoustics
=================================================

This package facilitates the creation of machine learning training data on
underwater acoustics.  While the raw data is available from Marine Cadastre and
Ocean Networks Canada (via the ``onc`` package), ``tehom`` tracks downloads that it conducts
and exposes more useful queries for data exploration and sampling.  e.g.:

  "What hydrophones outside Vancouver have data during April-June, 2017?"

  "Where have I downloaded time/geo overlapping data?"

  "Give me acoustic snippets as numpy arrays, labeled with whether a container
  ship was close to the hydrophone that recorded the snippet."

See example.ipynb in the GitHub repo for an intro to the basic commands

Requirements
----------------
- ``sqlite3`` available as a command line program
- Create `Ocean Networks Canada account <https://data.oceannetworks.ca/Login?service=https://data.oceannetworks.ca/LandingPage>`_
  and get your API token `here <https://data.oceannetworks.ca/Profile#api_tab>`_


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   modules


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
