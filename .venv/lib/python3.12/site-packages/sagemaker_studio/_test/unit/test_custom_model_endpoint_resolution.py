import os
import pathlib
from unittest import TestCase

from boto3 import session


class TestEndpointResolution(TestCase):
    def setUp(self):
        models_directory = pathlib.Path(__file__).parent.parent.parent.joinpath("boto3_models")
        os.environ["AWS_DATA_PATH"] = str(models_directory)

    def tearDown(self):
        os.environ["AWS_DATA_PATH"] = ""

    def test_glue_model_url(self):
        client = session.Session(region_name="us-east-1").client("glue")
        self.assertTrue("https://glue.us-east-1.amazonaws.com" == client.meta.endpoint_url)

    def test_datazone_model_url(self):
        client = session.Session(region_name="us-east-1").client("datazone")
        self.assertTrue("https://datazone.us-east-1.api.aws" == client.meta.endpoint_url)
