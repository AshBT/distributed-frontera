.. distributed-frontera documentation master file, created by
   sphinx-quickstart on Wed Jul 15 20:20:07 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Distributed Frontera: Large-scale web crawling framework
========================================================

`Frontera`_ is a crawl frontier implementation of a `web crawler`_. It's managing when and what to crawl next, checking
for crawling goal accomplishment. `Distributed Frontera`_ is extension to Frontera providing replication, sharding and
isolation of all parts of Frontera-based crawler to scale and distribute it. Both these packages contain components to
allow creation of fully-operational web crawler with Scrapy.

Contents
========
.. toctree::
    :maxdepth: 2

    topics/overview
    topics/quickstart
    topics/production


Customizing
-----------
.. toctree::
    :maxdepth: 2

    topics/customization/own_crawling_strategy
    topics/customization/scrapy_spider

..      topics/customization/communication
        topics/customization/extending_sw

..  Maintenance
    -----------
    .. toctree::
        :maxdepth: 2

..      topics/maintenance/settings
        topics/maintenance/cluster-configuration
        topics/maintenance/rebuilding_queue

Miscellaneous
-------------
.. toctree::
    topics/maintenance/settings
    topics/glossary

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _`Frontera`: http://github.com/scrapinghub/frontera
.. _`web crawler`: https://en.wikipedia.org/wiki/Web_crawler
.. _`Distributed Frontera`: https://github.com/scrapinghub/distributed-frontera
