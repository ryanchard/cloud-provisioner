from nose.tools import istest
from tests.helpers import MockedIO, ensure_except

from ggprovisioner import Singleton


class TestRunner(MockedIO):
    """
    A class wrapper for tests to automate setUp and tearDown which masks all
    writing to stderr and stdout.
    Ensures that if we have more elaborate setup and teardown to do in the
    future, we don't have to reindent the whole set of tests.
    """

    @istest
    def singleton_instantiation_works(self):
        """
        Unit: Singleton Doesn't Break Class Instatiation
        """
        # define a class with Singleton as its metaclass
        class A(object):
            __metaclass__ = Singleton

            # give instances some attributes
            def __init__(self):
                self.x = True

        # instantiate the class (and don't expect errors)
        instance = A()
        # check that the attribute was set to make sure we haven't broken
        # initialization either
        assert instance.x

    @istest
    def multiple_singleton_instantiations_yield_same(self):
        """
        Unit: Multiple Singleton Instantiations Yield One Instance
        """
        # define a class with Singleton as its metaclass
        class A(object):
            __metaclass__ = Singleton

            # give instances some attributes
            def __init__(self):
                self.x = True

        instance1 = A()
        instance2 = A()

        assert instance1 is instance2

    @istest
    def singleton_ignore_args(self):
        """
        Unit: Singleton Ignores Arguments (After First Instatiation)
        """
        # define a class with Singleton as its metaclass
        class A(object):
            __metaclass__ = Singleton

            # give instances an attribute passed via constructor arguments
            def __init__(self, x):
                self.x = x

        # create two instances, make sure that the second one's arguments are
        # ignored
        instance1 = A(1)
        instance2 = A(2)

        assert instance1.x is 1
        assert instance2.x is 1

    @istest
    def singleton_reset_by_del(self):
        """
        Unit: Singleton Can Be Reset By DelAttr
        """
        # define a class with Singleton as its metaclass
        class A(object):
            __metaclass__ = Singleton

            # give instances an attribute passed via constructor arguments
            def __init__(self, x):
                self.x = x

        instance1 = A(1)
        del A._instance
        instance2 = A(2)

        # note that the old instance is still hanging around because we have a
        # ref to it! The singleton's singularity is broken
        assert instance1.x is 1
        assert instance2.x is 2

    @istest
    def uninstantiated_singleton_lacks_instanceattr(self):
        """
        Unit: Singleton Lacks "_instance" Attribute Before Instantiation
        """
        # define a class with Singleton as its metaclass
        class A(object):
            __metaclass__ = Singleton

            # give instances an attribute
            def __init__(self):
                self.x = True

        # wrapping this in a function makes it easier to use exception
        # examination tooling
        def fetch_instance(classref):
            return classref._instance

        # get the AttributeError out of A when we try to pull its instance too
        # soon
        err = ensure_except(AttributeError, fetch_instance, A)

        # also make sure that this stuff works the other way after
        # instantiation

        instance = A()

        x = fetch_instance(A)
        assert x is instance
