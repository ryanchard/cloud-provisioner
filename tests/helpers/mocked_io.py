import io
import mock


class MockedIO(object):
    def __init__(self):
        # create io patches
        self.stderr = io.StringIO()
        self.stdout = io.StringIO()
        self.stderr_patch = mock.patch('sys.stderr', self.stderr)
        self.stdout_patch = mock.patch('sys.stdout', self.stdout)

    def setUp(self):
        self.stderr_patch.start()
        self.stdout_patch.start()

    def tearDown(self):
        self.stderr_patch.stop()
        self.stdout_patch.stop()
