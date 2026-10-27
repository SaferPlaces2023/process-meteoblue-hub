import unittest

from process_meteoblue_hub import parse_event, run_meteoblue_ingestor, run_meteoblue_retriever

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambda'))
import lambda_function  # noqa: E402


class TestLambdaWiring(unittest.TestCase):
    """Offline tests validating the lambda handlers are wired correctly."""

    def test_handlers_exist(self):
        self.assertTrue(callable(lambda_function.ingestor_handler))
        self.assertTrue(callable(lambda_function.retriever_handler))

    def test_parse_event_type_coercion(self):
        event = {
            "debug": "true",
            "verbose": "false",
        }
        kwargs = parse_event(event, run_meteoblue_retriever)
        self.assertIs(kwargs["debug"], True)
        self.assertIs(kwargs["verbose"], False)

    def test_parse_event_filters_unknown_keys(self):
        event = {"not_a_real_argument": "x", "debug": "false"}
        kwargs = parse_event(event, run_meteoblue_ingestor)
        self.assertNotIn("not_a_real_argument", kwargs)
        self.assertIs(kwargs["debug"], False)


if __name__ == '__main__':
    unittest.main()
