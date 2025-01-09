import pandas as pd
import xml.etree.ElementTree as ET

PANDAS_DATATYPES = {
    'assetTypeNumber': "string",
    'assetNumber': "string",
    'reportingPeriodBeginningDate': "datetime",
    'reportingPeriodEndDate': "datetime",
    'originatorName': "string",
    'originationDate': "datetime",
    'originalLoanAmount': "Float64",
    'originalTermLoanNumber': "Int64",
    'maturityDate': "datetime",
    'originalInterestRatePercentage': 'Float64',
    'interestRateSecuritizationPercentage': 'Float64',
    'interestAccrualMethodCode': "Int64",
    'originalInterestRateTypeCode': "Int64",
    'originalInterestOnlyTermNumber': "Int64",
    'firstLoanPaymentDueDate': "datetime",
    'underwritingIndicator': "bool",
    'lienPositionSecuritizationCode': "Int64",
    'loanStructureCode': "string",
    'paymentTypeCode': "Int64",
    'periodicPrincipalAndInterestPaymentSecuritizationAmount': "Float64",
    'scheduledPrincipalBalanceSecuritizationAmount': "Float64",
    'paymentFrequencyCode': "Int64",
    'NumberPropertiesSecuritization': "Int64",
    'NumberProperties': "Int64",
    'graceDaysAllowedNumber': "Int64",
    'interestOnlyIndicator': "bool",
    'balloonIndicator': "bool",
    'prepaymentPremiumIndicator': "bool",
    'negativeAmortizationIndicator': "bool",
    'modifiedIndicator': "bool",
    'prepaymentLockOutEndDate': "datetime",
    'property': "string",
    'assetAddedIndicator': "bool",
    'reportPeriodModificationIndicator': "bool",
    'reportPeriodBeginningScheduleLoanBalanceAmount': "Float64",
    'totalScheduledPrincipalInterestDueAmount': "Float64",
    'reportPeriodInterestRatePercentage': "Float64",
    'servicerTrusteeFeeRatePercentage': "Float64",
    'scheduledInterestAmount': "Float64",
    'scheduledPrincipalAmount': "Float64",
    'unscheduledPrincipalCollectedAmount': "Float64",
    'reportPeriodEndActualBalanceAmount': "Float64",
    'reportPeriodEndScheduledLoanBalanceAmount': "Float64",
    'paidThroughDate': "datetime",
    'servicingAdvanceMethodCode': "Int64",
    'nonRecoverabilityIndicator': "bool",
    'totalPrincipalInterestAdvancedOutstandingAmount': "Float64",
    'totalTaxesInsuranceAdvancesOutstandingAmount': "Float64",
    'otherExpensesAdvancedOutstandingAmount': "Float64",
    'paymentStatusLoanCode': "Int64",
    'primaryServicerName': "string",
    'assetSubjectDemandIndicator': "bool",
    'originalAmortizationTermNumber': "Int64",
    'yieldMaintenanceEndDate': "datetime",
    'prepaymentPremiumsEndDate': "datetime",
    'firstPaymentAdjustmentDate': "datetime"
}

def parse_xml(f=None, xml_data=None):
    assert f is not None or xml_data is not None, "Either a file path or data must be provided"
    # XML data
    if xml_data is None:
        xml_data = open(f, 'r').read()

    # Parse the XML
    root = ET.fromstring(xml_data)

    # Create an empty list to store dictionaries representing each asset
    assets_list = []

    # Define the XML namespace
    ns = {'ns': 'http://www.sec.gov/edgar/document/absee/cmbs/assetdata'}

    # Loop through each 'assets' element and extract data into dictionaries
    for asset in root.findall('.//ns:assets', namespaces=ns):
        asset_dict = {}
        for child in asset:
            if child.tag.replace(f'{{{ns["ns"]}}}', '') == 'property':
                # Extract property data
                for prop_child in child:
                    prop_tag = prop_child.tag.replace(f'{{{ns["ns"]}}}', '')
                    asset_dict[prop_tag] = prop_child.text
            else:
                asset_dict[child.tag.replace(f'{{{ns["ns"]}}}', '')] = child.text

        # Append the dictionary to the list
        assets_list.append(asset_dict)

    # Create a pandas DataFrame from the list of dictionaries
    df = pd.DataFrame(assets_list)

    return df

def convert_dtypes(df, bool_to_int=False):
    for col in df:
        if col in PANDAS_DATATYPES:
            dtype = PANDAS_DATATYPES[col]
            if dtype.lower().startswith("float") or dtype.lower().startswith("int"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
                if dtype.lower().startswith("float") and ":" in dtype:
                    df[col] = df[col].astype(float).round(int(dtype.split(":")[1]))
                elif dtype.startswith("int"):
                    df[col] = df[col].round(0)
            
            if dtype.lower() == "datetime":
                df[col] = pd.to_datetime(df[col])
            elif dtype in ["bool", "boolean"]:
                df[col] = df[col].str.lower().replace({'true': True, '1': True, 'false': False, '0': False}).astype('boolean')
                if bool_to_int:
                    df[col] = df[col].astype("Int64")
            else:
                df[col] = df[col].astype(dtype)
            if dtype == "string":
                df[col] = df[col].str.strip()
    return df


if __name__ == "__main__":
    import glob

    files = glob.glob("data/*/*/*.xml")
    for f in files:
        df = parse_xml(f)
        df = convert_dtypes(df)
        print(df.dtypes)
        break

