.. _install:

*************
Installation
*************

Major Dependencies
------------------

* ``Python`` 3.7+
* The **amazing** `trio <https://trio.readthedocs.io/en/stable/>`_ library for all concurrency and event loops
  and its socket interface (both client and server side for multiprocessing).
* `tree_config <https://matham.github.io/tree-config/index.html>`_ for remote configuration of the objects.
* `asks <https://asks.readthedocs.io/en/latest/>`_ for (internal) optional client REST requests.
* `trio-websocket <https://trio-websocket.readthedocs.io/en/stable/>`_ for (internal) optional client WebSocket requests.
* `Quart <https://pgjones.gitlab.io/quart/>`_ for optional WebSocket/REST server.

Installing PyMoa-Remote
-----------------------
pymoa_remote and its dependencies can be installed simply with::

    pip install pymoa_remote[network]

This installs pymoa_remote with support for the websocket/REST client and server. Install simply with::

    pip install pymoa_remote

to only install the threading and multiprocessing support without the websocket/REST support.
