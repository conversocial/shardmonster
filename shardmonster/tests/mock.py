from __future__ import absolute_import

try:
    from unittest.mock import call, Mock, patch
except ImportError:
    from mock import call, Mock, patch

__all__ = ['call', 'Mock', 'patch']
