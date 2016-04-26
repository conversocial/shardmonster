Developing Shardmonster
=======================

Running tests locally
---------------------

.. code-block:: bash

    > cp sample_test_settings.py test_settings.py

Update test_settings.py to reflect your local environment. ::

    > python setup.py nosetests

Running tests inside containers
-------------------------------

Make sure you have ``docker`` and ``docker-compose`` installed_. From the
shardmonster directory run:

.. code-block:: bash

    > cp sample_test_settings.py test_settings.py
    > docker-compose build
    > docker-compose up -d
    > docker exec -i -t shardmonster_test_1 /bin/bash
    root@container> mongo --host controller --eval 'rs.initiate()'
    root@container> mongo --host replica --eval 'rs.initiate()'
    root@container> exit
    > docker exec shardmonster_test_1 tox
    > docker-compose stop
    > docker-compose rm -f


Building Docs
-------------

.. code-block:: bash

    pip install sphinx sphinx_rtd_theme
    python setup.py build_sphinx

.. _installed: https://docs.docker.com/compose/install/
