Developing Shardmonster
=======================

Running tests
-------------

::

    > cp sample_test_settings.py test_settings.py

Update test_settings.py to reflect your local environment. ::

    > python setup.py nosetests


Building Docs
-------------

.. code-block:: bash

    pip install sphinx sphinx_rtd_theme
    python setup.py build_sphinx

