import unittest

from sagemaker_studio.execution.utils import RemoteExecutionUtils


class TestRemoteExecutionUtils(unittest.TestCase):

    def test_pack_s3_path_for_input_file(self):
        # Test case 1: project_s3_path ends without slash, local_file_path with leading 'src/'
        project_s3_path = "s3://bucket/domain/project"
        local_file_path = "src/getting_started.ipynb"
        is_git_project = True
        result = RemoteExecutionUtils.pack_s3_path_for_input_file(
            project_s3_path, local_file_path, is_git_project
        )
        expected = "s3://bucket/domain/project/workflows/project-files/getting_started.ipynb"
        self.assertEqual(result, expected)

        # Test case 2: local_file_path without leading 'src/'
        local_file_path = "getting_started.ipynb"
        result = RemoteExecutionUtils.pack_s3_path_for_input_file(
            project_s3_path, local_file_path, is_git_project
        )
        self.assertEqual(result, expected)

        # Test case 3: project_s3_path with a trailing slash
        project_s3_path = "s3://bucket/domain/project/"
        result = RemoteExecutionUtils.pack_s3_path_for_input_file(
            project_s3_path, local_file_path, is_git_project
        )
        self.assertEqual(result, expected)

        # Test case 4: local_file_path with extra sub-directory
        local_file_path = "src/folder/getting_started.ipynb"
        expected = "s3://bucket/domain/project/workflows/project-files/folder/getting_started.ipynb"
        result = RemoteExecutionUtils.pack_s3_path_for_input_file(
            project_s3_path, local_file_path, is_git_project
        )
        self.assertEqual(result, expected)

        # Test case 5: local_file_path with the leading slash
        local_file_path = "/src/folder/getting_started.ipynb"
        result = RemoteExecutionUtils.pack_s3_path_for_input_file(
            project_s3_path, local_file_path, is_git_project
        )
        self.assertEqual(result, expected)

        # Test case 6: provisioned_resource is non-git (S3) - unified storage
        project_s3_path = "s3://bucket/domain/project/dev/"
        local_file_path = "shared/folder/getting_started.ipynb"
        is_git_project = False
        expected = "s3://bucket/domain/project/shared/folder/getting_started.ipynb"
        result = RemoteExecutionUtils.pack_s3_path_for_input_file(
            project_s3_path, local_file_path, is_git_project
        )
        self.assertEqual(result, expected)

        # Test case 7: Lite project - unified storage
        project_s3_path = "s3://bucket/shared/"
        local_file_path = "shared/folder/getting_started.ipynb"
        is_git_project = False
        expected = "s3://bucket/shared/folder/getting_started.ipynb"
        result = RemoteExecutionUtils.pack_s3_path_for_input_file(
            project_s3_path, local_file_path, is_git_project
        )
        self.assertEqual(result, expected)

    def test_pack_full_path_for_input_file(self):
        local_file_path = "src/getting_started.ipynb"
        result = RemoteExecutionUtils.pack_full_path_for_input_file(local_file_path)
        expected = "/home/sagemaker-user/src/getting_started.ipynb"
        self.assertEqual(result, expected)

        local_file_path = "/src/folder/getting_started.ipynb"
        result = RemoteExecutionUtils.pack_full_path_for_input_file(local_file_path)
        expected = "/home/sagemaker-user/src/folder/getting_started.ipynb"
        self.assertEqual(result, expected)

    def test_pack_s3_path_for_output_file(self):
        # Test case 1: s3 output location should reuse input path with leading "_" added to file name
        project_s3_path = "s3://bucket/domain/project"
        local_input_file_path = "/src/input.ipynb"
        is_git_project = True
        expected_default = "s3://bucket/domain/project/workflows/output/_input.ipynb"
        result = RemoteExecutionUtils.pack_s3_path_for_output_file(
            project_s3_path, local_input_file_path, is_git_project
        )
        self.assertEqual(result, expected_default)

        # Test case 2: input file path has a leading slash
        local_input_file_path = "/src/input.ipynb"
        expected_default = "s3://bucket/domain/project/workflows/output/_input.ipynb"
        result = RemoteExecutionUtils.pack_s3_path_for_output_file(
            project_s3_path, local_input_file_path, is_git_project
        )
        self.assertEqual(result, expected_default)

        # Test case 3: input file path has sub-directory, should keep this sub-directory in output path
        local_input_file_path = "/src/folder/input.ipynb"
        expected_default = "s3://bucket/domain/project/workflows/output/folder/_input.ipynb"
        result = RemoteExecutionUtils.pack_s3_path_for_output_file(
            project_s3_path, local_input_file_path, is_git_project
        )
        self.assertEqual(result, expected_default)

        # Test case 4: provisioned_resource is non-git (S3) - unified storage
        project_s3_path = "s3://bucket/domain/project/dev/"
        local_file_path = "shared/folder/getting_started.ipynb"
        is_git_project = False
        expected = "s3://bucket/domain/project/dev/workflows/output/folder/_getting_started.ipynb"
        result = RemoteExecutionUtils.pack_s3_path_for_output_file(
            project_s3_path, local_file_path, is_git_project
        )
        self.assertEqual(result, expected)

    def test_is_git_project(self):
        # Test case 1: provisioned_resource is git
        provisioned_resources = [{"name": "codeRepositoryName", "value": "my-code-repository"}]
        expected = True
        result = RemoteExecutionUtils.is_git_project(provisioned_resources)
        self.assertEqual(result, expected)

        # Test case 2: provisioned_resource is non-git
        provisioned_resources = [{"name": "S3 FS", "value": "my-s3-file"}]
        expected = False
        result = RemoteExecutionUtils.is_git_project(provisioned_resources)
        self.assertEqual(result, expected)
