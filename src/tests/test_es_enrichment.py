from src import es_enrichment
import os
import unittest.mock as mock


class TestEnrichmentAlgorithm:

    def _get_test_data_fh(self, filename):
        """
        Open a file under the fixtures directory.

        Opens the file in read mode and returns the file object.
        :param name:
        :return: file object
        """
        filename = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures',
                filename
            )
        )
        return open(filename, 'r')

    @mock.patch("src.es_enrichment.client")
    def test_successful_enrichment(self, mock_client):
        test_fh = [
            self._get_test_data_fh("testdata_enrichv3.csv"),
            self._get_test_data_fh("location_lookup.csv"),
            self._get_test_data_fh("responder_county_lookup.csv"),
            self._get_test_data_fh("county_lookup.csv")
        ]
        mock_client.file.return_value.getFile.side_effect = test_fh
        result = es_enrichment.apply(
            {"s3Pointer": "ons-bucket/datafile.csv"}
        )
        assert "success" in result
        assert result["success"] is True
