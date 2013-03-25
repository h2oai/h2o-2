import unittest, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts

class TestPoll(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_redirect_poll_loop(self):
        n = h2o.nodes[0]
        for i in range(3):
            redir = n.test_redirect()['response']
            self.assertEqual(redir['status'], 'redirect')
            self.assertEqual(redir['redirect_request'], 'TestPoll')
            args = redir['redirect_request_args']
            status = 'poll'
            i = 0
            while status == 'poll':
                status = n.test_poll(args)['response']['status']
                i += 1
                if i > 100: self.fail('polling took too many iterations')
            self.assertEqual(status, 'done')
            


if __name__ == '__main__':
    h2o.unit_main()
