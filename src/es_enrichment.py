import Algorithmia
import pandas as pd
import traceback

client = Algorithmia.client()


def _get_fh(data_url):
    """
    Opens algorithmia data urls and returns a file object.

    :param filepath: data url to open
    :return: file object.
    :raises: AlgorithmException
    """
    try:
        fh = client.file(data_url).getFile()
    except Algorithmia.errors.DataApiError as exc:
        raise Algorithmia.errors.AlgorithmException(
            "Unable to get datafile {}: {}".format(
                data_url, ''.join(exc.args)
            )
        )
    return fh


def _get_traceback(exception):
    """
    Given an exception, returns the traceback as a string.

    :param exception: Exception object
    :return: string
    """
    return ''.join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    )


def apply(input):
    try:
        data_df = pd.read_csv(
            _get_fh("s3+input://".format(input["s3Pointer"]))
        )
        loc_lkup_df = pd.read_csv(
            _get_fh("data://ons/enrichment/location_lookup.csv")
        )
        responder_lkup_df = pd.read_csv(
            _get_fh("data://ons/enrichment/responder_lookup.csv")
        )
        responder_lkup_df.rename(columns={'ref': 'responder_id'}, inplace=True)
        county_lkup_df = pd.read_csv(
            _get_fh("data://thomashensonons/testcollection/_countyLookup.csv")
        )
        enriched_df = data_enrichment(
            data_df, responder_lkup_df, county_lkup_df, loc_lkup_df
        )
    except Algorithmia.errors.AlgorithmException as exc:
        return {
            "success": False,
            "error": _get_traceback(exc)
        }
    except Exception as exc:
        return {
            "success": False,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    return {
        "success": True,
        "data": enriched_df.to_json(
            orient="records"
        )[1:-1].replace("}, {", "},{")
    }


def _calculate_strata(row):
    row["strata"] = ""

    if row["strata"] == "":
        if row["land_or_marine"] == "m":
            row["strata"] = "M"
        if row["land_or_marine"] == "l" and row["Q608_total"] < 30000:
            row["strata"] = "E"
        if row["land_or_marine"] == "l" and row["Q608_total"] > 29999:
            row["strata"] = "D"
        if row["land_or_marine"] == "l" and row["Q608_total"] > 79999:
            row["strata"] = "C"
        if (
            row["land_or_marine"] == "l"
            and row["Q608_total"] > 129999
            and row["region"] > 9
        ):
            row["strata"] = "B2"
        if (
            row["land_or_marine"] == "l"
            and row["Q608_total"] > 129999
            and row["region"] < 10
        ):
            row["strata"] = "B1"
        if row["land_or_marine"] == "l" and row["Q608_total"] > 200000:
            row["strata"] = "A"
    return row


def _timeseries(row):
    """
    Add timeseriesperiod to a dataframe 'row'.

    :param row: Pandas series for row of dataframe
    :return: returns amended row
    """
    period_data = str(row["period"])
    period_data = period_data[0:4] + "-" + period_data[4:6]
    row["timeseriesperiod"] = period_data
    return row


def data_enrichment(data_df, responder_lkup_df, county_lkup_df, location_lkup_df):
    """


    :param data_df: Pandas dataframe of data to be enriched
    :param responder_lkup_df: Responder lookup dataframe (map responder code -> county code)
    :param county_lkup_df: County lookup datafram (map county code -> county name)
    :param location_lkup_df: Location lookup (map gor code -> location data)
    :return: pandas dataframe of enriched data
    """
    # merge county data
    responder_lkup_df.rename(columns={"ref": "responder_id", "county": "county"}, inplace=True)
    county_lkup_df.rename(columns={"cty_code": "county"}, inplace=True)
    data_df.rename(columns={"idbr": "responder_id"}, inplace=True)
    enriched_data = pd.merge(data_df, responder_lkup_df, on="responder_id")

    # merge location data
    enriched_data = pd.merge(enriched_data, location_lkup_df, on="gor_code")
    enriched_data = pd.merge(enriched_data, county_lkup_df, on=["county"])

    # rename columns
    enriched_data.rename(
        columns={
            "PERIOD": "period",
            "resp": "response_type",
            "sandcoat": "Q601_asphalting_sand",
            "sandbuil": "Q602_building_soft_sand",
            "sandconc": "Q603_concreting_sand",
            "gravcoat": "Q604_bituminous_gravel",
            "gravagg": "Q605_concreting_gravel",
            "gravoth": "Q606_other_gravel",
            "fill": "Q607_constructional_fill",
            "tot": "Q608_total",
            "lorm": "land_or_marine",
            "entno": "enterprise_ref",
            "GOR_DESC": "region_name",
        },
        inplace=True,
    )

    # calculate strata
    enriched_data = enriched_data.apply(_calculate_strata, axis=1)

    # add timeseries data
    enriched_data = enriched_data.apply(_timeseries, axis=1)

    return enriched_data
