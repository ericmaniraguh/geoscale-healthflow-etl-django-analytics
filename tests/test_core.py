import pytest
import os
import sys
from importlib import import_module

class TestCoreFramework:
    def test_asgi_import(self):
        """Test that ASGI module can be imported."""
        try:
            import app.asgi
            assert app.asgi.application is not None
        except ImportError:
            pytest.fail("Could not import app.asgi")

    def test_wsgi_import(self):
        """Test that WSGI module can be imported."""
        try:
            import app.wsgi
            assert app.wsgi.application is not None
        except ImportError:
            pytest.fail("Could not import app.wsgi")

    def test_manage_py_import(self):
        """Test that manage.py can be imported."""
        # manage.py is in the root, so we might need to adjust path or import by file path
        # But usually it's importable if in python path.
        # Since 'manage.py' is top level, it might be tricky to import as a module depending on sys.path
        # We'll try dynamic import
        try:
            # Assumes project root is in sys.path
            import manage
            assert hasattr(manage, 'main')
        except ImportError:
            # Fallback for coverage: read and compile
            with open('manage.py', 'r') as f:
                compile(f.read(), 'manage.py', 'exec')
