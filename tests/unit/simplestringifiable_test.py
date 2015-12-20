from nose.tools import istest
from tests.helpers import MockedIO

from ggprovisioner import SimpleStringifiable


class Testclass(SimpleStringifiable):
    """
    A generic class for use in the tests below
    """

    # takes an attribute as a constructor arg
    def __init__(self, x):
        self.x = x


class TestRunner(MockedIO):
    @istest
    def simplestringifiable_repr_intattrs(self):
        """
        Unit: SimpleStringifiable repr() With Integer Attributes
        """
        # create an instance
        alpha = Testclass(1)
        # validate its repr (but order is nondeterministic
        assert repr(alpha) == "Testclass(x=1)", repr(alpha)

    @istest
    def simplestringifiable_repr_strattrs(self):
        """
        Unit: SimpleStringifiable repr() With String Attributes
        """
        # create an instance
        alpha = Testclass('foo')
        # validate its repr
        assert repr(alpha) == "Testclass(x='foo')", repr(alpha)

        # create another, with nastier strings
        beta = Testclass('foo\'')
        # str repr is smart enough to use the right type of quotes if we have a
        # single-tick quote in the string (and no double quotes)
        assert repr(beta) == "Testclass(x=\"foo'\")", repr(beta)

        # okay, now lets make the string REALLY gross
        gamma = Testclass('foo\'"\'"""')
        # str repr goes back to single quotes when we do crazy crap like this
        assert (repr(gamma) ==
                "Testclass(x='foo\\'\"\\'\"\"\"')"), repr(gamma)

    @istest
    def simplestringifiable_repr_nested_instances(self):
        """
        Unit: SimpleStringifiable repr() With Nested Classes
        """
        # create an instance
        alpha = Testclass(1)
        beta = Testclass(alpha)
        # validate its repr
        assert repr(beta) == "Testclass(x=Testclass(x=1))", repr(beta)

    @istest
    def simplestringifiable_repr_multiple_attrs(self):
        """
        Unit: SimpleStringifiable repr() With Multiple Attributes
        """
        # create an instance
        alpha = Testclass(1)
        alpha.y = 2
        # validate its repr
        assert repr(alpha) == "Testclass(x=1,y=2)", repr(alpha)
