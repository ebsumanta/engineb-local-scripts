import json

x = {
    "export_file_name": "Test.csv",
    "file_container_path": "data-ingestions/clgummed8363701rxq2jjxbak/share/zip/cdm.zip",
    "file_name": "glDetail.csv",
    "column_name": [
        "amount",
        "amountCreditDebitIndicator",
        "effectiveDate",
        "glAccountNumber",
        "jeHeaderDescription",
        "journalId",
        "journalIdLineNumber",
        "transactionId",
        "transactionTypeDescription",
        "transactionType",
        "reversalReason",
        "isNegativePostingIndicator",
        "reversalID",
        "reversalType",
        "reversalFiscalYear",
        "isInterCoFlag",
        "clearingDocumentNumber",
        "clearingDate",
        "customerNumber",
        "vendorNumber",
        "documentStatus",
        "documentDate",
        "documentType",
        "transactionKey",
        "accountType",
        "referenceTransaction",
        "businessTransactionCode",
        "postingKey",
        "isClearing",
        "referenceSubledgerDocumentNumber",
        "transactionCode",
        "amountCurrency",
        "approvedBy",
        "approvedDateTime",
        "businessUnitCode",
        "enteredBy",
        "enteredDateTime",
        "fiscalYear",
        "jeLineDescription",
        "journalEntryType",
        "lastModifiedBy",
        "lastModifiedDate",
        "localAmount",
        "localAmountCurrency",
        "period",
        "reportingAmount",
        "reportingAmountCurrency",
        "reversalIndicator",
        "reversalJournalId",
        "segment01",
        "segment02",
        "segment03",
        "segment04",
        "segment05",
        "sourceId",
        "tags",
        "taxCode",
        "userDefined1",
        "userDefined2"
    ],
    "start_with":"1",
    "start_source": "0",
    "operation": [
        {
            "order": 1,
            "file_name": "glDetail.csv",
            "operation_type": "Filter",
            "filter_column": "amount",
            "filter_condition": "gt",
            "input": "10000",
            "source": "0",
            "relation": "",
            "relation_with": "",
            "is_unique": "",
            "replace": "0",
            "next": 2
        },
        {
            "order": 2,
            "file_name": "glDetail.csv",
            "operation_type": "Filter",
            "filter_column": "amount",
            "filter_condition": "lt",
            "input": "-10000",
            "source": "0",
            "relation": "or",
            "relation_with": "1",
            "is_unique": "",
            "replace": "1",
            "next": 4
        },
        {
            "order": 3,
            "file_name": "glDetail.csv",
            "operation_type": "Count",
            "filter_column": "glAccountNumber",
            "filter_condition": "lt",
            "input": "",
            "source": "0",
            "relation": "",
            "relation_with": "",
            "replace": "0",
            "next": None,
            "is_unique": "1"
        },
        {
            "order": 4,
            "file_name": "glDetail.csv",
            "operation_type": "Net",
            "filter_column": "",
            "filter_condition": "lt",
            "input": "",
            "source": "0",
            "relation": "",
            "relation_with": "",
            "is_unique": "",
            "replace": "0",
            "next": 3
        }
    ]
}

k=dict()
for _index in range(len(x['operation'])):
    k[x['operation'][_index]['order']] = _index

p=[]
count = 1

while count is not None:
    data = x['operation'][k[count]]
    p.append(data)
    count = data['next']

print(p)