from unittest import TestCase
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

from sagemaker_studio import Domain
from sagemaker_studio.models.execution import ExecutionConfig

GET_DOMAIN_RESPONSE = {
    "id": "dzd_1234",
    "rootDomainUnitId": "bogus_root_domain_id",
    "name": "domain-10-07-24-114102",
    "singleSignOn": {
        "type": "IAM_IDC",
        "userAssignment": "AUTOMATIC",
        "idcInstanceArn": "arn:aws:sso:::instance/ssoins-bogus-id",
    },
    "domainExecutionRole": "arn:aws:iam::123456123456:role/DomainExecutionRole",
    "arn": "arn:aws:datazone:us-west-2:123456123456:domain/bogus_id",
    "status": "AVAILABLE",
    "portalUrl": "https://bogus_id.datazone.us-west-2.on.aws",
    "createdAt": "2024-10-07T11:41:11.338000-04:00",
    "lastUpdatedAt": "2024-10-07T11:41:11.417000-04:00",
    "tags": {},
}


def mock_build_execution_config(*args, **kwargs):
    return ExecutionConfig()


@patch(
    "sagemaker_studio.sagemaker_studio_api.SageMakerStudioAPI._build_execution_config",
    mock_build_execution_config,
)
class TestDomain(TestCase):

    def setUp(self):
        self.mock_datazone_api = Mock()
        self.mock_datazone_api.get_domain = Mock()
        self.mock_datazone_api.get_domain.return_value = GET_DOMAIN_RESPONSE

    @patch("sagemaker_studio.sagemaker_studio_api.SageMakerStudioAPI._get_aws_client")
    @patch("sagemaker_studio.utils._internal.InternalUtils._get_domain_id")
    def test_initialize_domain_without_domain_id(
        self, get_domain_id_mock: Mock, get_aws_client_mock: Mock
    ):
        get_domain_id_mock.return_value = "dzd_1234"
        get_aws_client_mock.side_effect = lambda x, y: self.mock_datazone_api
        domain = Domain()

        self.assertEqual(domain.root_domain_unit_id, GET_DOMAIN_RESPONSE["rootDomainUnitId"])
        self.assertEqual(domain.name, GET_DOMAIN_RESPONSE["name"])
        self.assertEqual(domain.domain_execution_role, GET_DOMAIN_RESPONSE["domainExecutionRole"])
        self.assertEqual(domain.status, GET_DOMAIN_RESPONSE["status"])
        self.assertEqual(domain.portal_url, GET_DOMAIN_RESPONSE["portalUrl"])

    @patch("sagemaker_studio.sagemaker_studio_api.SageMakerStudioAPI._get_aws_client")
    @patch("sagemaker_studio.utils._internal.InternalUtils._get_domain_id")
    def test_initialize_domain_with_domain_id(
        self, get_domain_id_mock: Mock, get_aws_client_mock: Mock
    ):
        get_domain_id_mock.return_value = "dzd_1234"
        get_aws_client_mock.side_effect = lambda x, y: self.mock_datazone_api
        domain = Domain(id="dzd_4321")

        self.assertEqual(domain.root_domain_unit_id, GET_DOMAIN_RESPONSE["rootDomainUnitId"])
        self.assertEqual(domain.name, GET_DOMAIN_RESPONSE["name"])
        self.assertEqual(domain.domain_execution_role, GET_DOMAIN_RESPONSE["domainExecutionRole"])
        self.assertEqual(domain.status, GET_DOMAIN_RESPONSE["status"])
        self.assertEqual(domain.portal_url, GET_DOMAIN_RESPONSE["portalUrl"])

    @patch("sagemaker_studio.sagemaker_studio_api.SageMakerStudioAPI._get_aws_client")
    def test_initialize_domain_get_domain_id_fails(self, get_domain_id_mock: Mock):
        get_domain_id_mock.side_effect = Exception("Something Went Wrong")

        with self.assertRaises(Exception) as context:
            Domain(id="dzd_4321")
            self.assertTrue("Something went wrong" in str(context.exception))

    @patch("sagemaker_studio.sagemaker_studio_api.SageMakerStudioAPI._get_aws_client")
    def test_initialize_domain_get_domain_fails(self, get_aws_client_mock: Mock):
        get_aws_client_mock.side_effect = lambda x, y: self.mock_datazone_api
        error_response = {
            "Error": {"Code": "ServiceQuotaExceededException", "Message": "Service Quota Exceeded"}
        }
        self.mock_datazone_api.get_domain.side_effect = ClientError(error_response, "GetDomain")  # type: ignore

        with self.assertRaises(ClientError) as context:
            Domain(id="dzd_4321")
            self.assertTrue("ServiceQuotaExceededException" in str(context.exception))

    def test_initialize_domain_cannot_find_domain_id_anywhere(self):
        with self.assertRaises(ValueError) as context:
            Domain()
            self.assertTrue(
                "Domain ID not found in environment. Please specify a domain ID."
                in str(context.exception)
            )

    @patch("sagemaker_studio.sagemaker_studio_api.SageMakerStudioAPI._get_aws_client")
    @patch("sagemaker_studio.utils._internal.InternalUtils._get_domain_id")
    @patch(
        "sagemaker_studio.utils.SAGEMAKER_METADATA_JSON_PATH",
        "test_sagemaker_space_metadata.json",
    )
    def test_get_user_id_from_sagemaker_json(
        self, get_domain_id_mock: Mock, get_aws_client_mock: Mock
    ):
        get_domain_id_mock.return_value = "dzd_1234"
        get_aws_client_mock.side_effect = lambda x, y: self.mock_datazone_api
        domain = Domain()
        self.assertEqual(domain.user_id, "bogus_user_id_sm_space")

    @patch("sagemaker_studio.sagemaker_studio_api.SageMakerStudioAPI._get_aws_client")
    @patch("sagemaker_studio.utils._internal.InternalUtils._get_domain_id")
    @patch(
        "sagemaker_studio.utils.SAGEMAKER_METADATA_JSON_PATH",
        "/tmp/this-will-not-exist-ever-anywhere.json",
    )
    def test_get_user_id_from_sagemaker_json_file_not_exists(
        self, get_domain_id_mock: Mock, get_aws_client_mock: Mock
    ):
        get_domain_id_mock.return_value = "dzd_1234"
        get_aws_client_mock.side_effect = lambda x, y: self.mock_datazone_api
        domain = Domain()
        with self.assertRaises(RuntimeError) as context:
            domain.user_id
            self.assertTrue(
                "Encountered an error getting the current user ID" in str(context.exception)
            )
