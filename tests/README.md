Unit Tests
==========

The Unit tests isolate, as much as possible, each component of the system.
When a component has dependencies *every dependency* without exception MUST be
mocked. Failure to do so makes a test an integration test, possibly by mistake.
These tests aren't "bad" per-se, but they are misplaced and their results are
harder to understand.

There are gray areas with respect to mocking and unit vs. integration tests.
A function which handles instances of a class should be tested with a mocked
version of that class, and there should be a separate (nearly identical)
integration test that does the same things without mocking the class.
However, this can make testsuite maintenance very burdensome, and arguably a
function like this one

```
def job_is_fulfilled(job_instance):
    return job_instance.fulfilled
```

shouldn't ever be tested in isolation because such tests would be meaningless.
In such a case, not mocking the object's class is probably the correct call,
but the test should be named appropriately to indicate that it is testing the
function's understanding of the class interface.

Integration Tests
=================

Rather than testing components in isolation, Integration Tests are designed to
ensure that multiple components of the system work together correctly.
