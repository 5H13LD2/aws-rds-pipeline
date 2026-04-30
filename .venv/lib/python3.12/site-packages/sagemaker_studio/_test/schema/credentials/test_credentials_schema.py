import unittest

from sagemaker_studio._openapi.models import GetDomainExecutionRoleCredentialsRequest


class TestGetDomainExecutionRoleCredsSchema(unittest.TestCase):
    def test_it_should_have_valid_domain_identifier(self):
        domain_identifier = "test"
        with self.assertRaises(Exception) as context:
            GetDomainExecutionRoleCredentialsRequest(domain_identifier)
            self.assertTrue(
                "Invalid value for `domain_identifier`, must match regular expression `^dzd[-_][a-zA-Z0-9_-]{1,36}$`"
                in str(context.exception)
            )

    def test_it_require_domain_identifier(self):
        with self.assertRaises(Exception) as context:
            GetDomainExecutionRoleCredentialsRequest()
            self.assertTrue(
                "__init__() missing 1 required positional argument: 'domain_identifier'"
                in str(context.exception)
            )
