import unittest

from sagemaker_studio._openapi.models import (
    GetExecutionRequest,
    ListExecutionsRequest,
    StartExecutionRequest,
)


class TestListExecutionsRequestSchema(unittest.TestCase):
    def test_it_should_validate_max_results(self):
        with self.assertRaises(Exception) as context:
            ListExecutionsRequest({"max_results": 0})
            self.assertTrue(
                "Invalid value for `max_results`, must be a value greater than or equal to `1`"
                in str(context.exception)
            )

        with self.assertRaises(Exception) as context:
            ListExecutionsRequest({"max_results": 101})
            self.assertTrue(
                "Invalid value for `max_results`, must be a value less than or equal to `100`"
                in str(context.exception)
            )
        with self.assertRaises(Exception) as context:
            ListExecutionsRequest({"max_results": "test"})
            self.assertTrue(
                "Invalid type for variable 'max_results'. Required value type is int and passed type was str at ['max_results']"
                in str(context.exception)
            )

    def test_it_should_validate_next_token(self):
        with self.assertRaises(Exception) as context:
            ListExecutionsRequest({"next_token": 0})
            self.assertTrue(
                "Invalid type for variable 'next_token'. Required value type is str and passed type was int at ['next_token']"
                in str(context.exception)
            )

    def test_it_should_validate_sort_by(self):
        with self.assertRaises(Exception) as context:
            ListExecutionsRequest({"sort_by": "test"})
            self.assertTrue(
                "Invalid value for `sort_by` (test), must be one of ['NAME', 'STATUS', 'START_TIME', 'END_TIME']"
                in str(context.exception)
            )

    def test_it_should_validate_sort_order(self):
        with self.assertRaises(Exception) as context:
            ListExecutionsRequest({"sort_order": "test"})
            self.assertTrue(
                "Invalid value for `sort_order` (test), must be one of ['ASCENDING', 'DESCENDING']"
                in str(context.exception)
            )

    def test_it_can_be_optional(self):
        resp = ListExecutionsRequest({})
        self.assertTrue(resp)

    def test_it_can_pass_tag_filters(self):
        resp = ListExecutionsRequest({"filter_by_tags": {"key": "value"}})
        self.assertTrue(resp)


class TestStartExecutionRequestSchema(unittest.TestCase):
    def setUp(self):
        self.minimal_valid_request = {
            "execution_name": "testing",
            "input_config": {"notebook_config": {"input_path": "path/to/file"}},
        }

    def test_execution_name_is_specified(self):
        test_request = {"input_config": {"notebook_config": {"input_path": "path/to/file"}}}
        with self.assertRaises(Exception) as context:
            StartExecutionRequest(**test_request)
            self.assertTrue(
                "__init__() missing 1 required positional argument: 'execution_name'"
                in str(context.exception)
            )

    def test_execution_name_is_not_empty(self):
        test_request = {
            **self.minimal_valid_request,
            "execution_name": "",
        }
        with self.assertRaises(Exception) as context:
            StartExecutionRequest(**test_request)
            self.assertTrue(
                "Invalid value for `execution_name`, length must be greater than or equal to `1`"
                in str(context.exception)
            )

    def test_execution_name_is_not_too_long(self):
        test_request = {
            **self.minimal_valid_request,
            "execution_name": "n" * 27,
        }
        with self.assertRaises(Exception) as context:
            StartExecutionRequest(**test_request)
            self.assertTrue(
                "Invalid value for `execution_name`, length must be less than or equal to `26`"
                in str(context.exception)
            )

    def test_execution_name_can_be_max_length(self):
        test_request = {
            **self.minimal_valid_request,
            "execution_name": "n" * 26,
        }
        resp = StartExecutionRequest(**test_request)
        self.assertTrue(resp)

    def test_execution_type_is_not_invalid_choice(self):
        test_request = {
            **self.minimal_valid_request,
            "execution_type": "INVALID",
        }
        with self.assertRaises(Exception) as context:
            StartExecutionRequest(**test_request)
            self.assertTrue(
                "Invalid value for `execution_type` (INVALID), must be one of ['NOTEBOOK']"
                in str(context.exception)
            )

    def test_execution_type_can_be_valid(self):
        test_request = {
            **self.minimal_valid_request,
            "execution_type": "NOTEBOOK",
        }
        resp = StartExecutionRequest(**test_request)
        self.assertTrue(resp)

    def test_execution_type_can_be_optional(self):
        test_request = {
            **self.minimal_valid_request,
        }
        resp = StartExecutionRequest(**test_request)
        self.assertTrue(resp)

    def test_input_config_is_specified(self):
        test_request = {
            "execution_name": "testing",
        }
        with self.assertRaises(Exception) as context:
            StartExecutionRequest(**test_request)
            self.assertTrue(
                "__init__() missing 1 required positional argument: 'input_config'"
                in str(context.exception)
            )

    def test_input_config_is_not_empty(self):
        test_request = {**self.minimal_valid_request, "input_config": {}}
        with self.assertRaises(Exception) as context:
            StartExecutionRequest(**test_request)
            self.assertTrue(
                "_from_openapi_data() missing 1 required positional argument: 'notebook_config'"
                in str(context.exception)
            )

    def test_input_config_can_be_valid(self):
        test_request = {
            **self.minimal_valid_request,
            "input_config": {"notebook_config": {"input_path": "path/to/file"}},
        }
        resp = StartExecutionRequest(**test_request)
        self.assertTrue(resp)

    def test_input_config_can_have_input_parameters(self):
        test_request = {
            **self.minimal_valid_request,
            "input_config": {
                "notebook_config": {
                    "input_path": "path/to/file",
                    "input_parameters": {
                        "a": "1",
                        "b": "2",
                    },
                }
            },
        }
        resp = StartExecutionRequest(**test_request)
        self.assertTrue(resp)

    def test_input_config_can_have_idempotency_token(self):
        test_request = {**self.minimal_valid_request, "client_token": "asdf"}
        resp = StartExecutionRequest(**test_request)
        self.assertTrue(resp)

    def test_output_config_can_be_specified(self):
        test_request = {
            **self.minimal_valid_request,
            "output_config": {"notebook_config": {"output_formats": ["NOTEBOOK", "HTML"]}},
        }
        resp = StartExecutionRequest(**test_request)
        self.assertTrue(resp)

    def test_output_config_must_be_valid(self):
        test_request = {
            **self.minimal_valid_request,
            "output_config": {"notebook_config": {"output_formats": ["INVALID"]}},
        }
        with self.assertRaises(Exception) as context:
            StartExecutionRequest(**test_request)
            self.assertTrue(
                "Invalid values for `output_formats` [('INVALID',)], must be a subset of [NOTEBOOK, HTML]"
                in str(context.exception)
            )

    def test_notebook_config_can_be_specified(self):
        test_request = {
            **self.minimal_valid_request,
            "input_config": {
                "notebook_config": {
                    "input_path": "path/to/file",
                }
            },
        }
        resp = StartExecutionRequest(**test_request)
        self.assertTrue(resp)

    def test_notebook_config_has_input_path(self):
        test_request = {**self.minimal_valid_request, "input_config": {"notebook_config": {}}}
        with self.assertRaises(Exception) as context:
            StartExecutionRequest(**test_request)
            self.assertTrue(
                "_from_openapi_data() missing 1 required positional argument: 'input_path'"
                in str(context.exception)
            )


class TestGetExecutionRequestSchema(unittest.TestCase):
    def test_it_should_validate_execution_id(self):
        with self.assertRaises(Exception) as context:
            GetExecutionRequest()
            self.assertTrue(
                "__init__() missing 1 required positional argument: 'execution_id'"
                in str(context.exception)
            )

    def test_it_should_validate_domain_id(self):
        with self.assertRaises(Exception) as context:
            GetExecutionRequest("test", {"domain_identifier": "test"})
            self.assertTrue(
                "Invalid value for `domain_identifier`, must match regular expression `^dzd[-_][a-zA-Z0-9_-]{1,36}$`"
                in str(context.exception)
            )
