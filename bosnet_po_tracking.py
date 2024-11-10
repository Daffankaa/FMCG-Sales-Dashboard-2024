import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
from concurrent.futures import ThreadPoolExecutor
import numpy as np  # Ensure numpy is imported correctly

# SQL Server connection details
sql_server_connection_str = (
    "mssql+pyodbc://readuser:Read123$%@skintific.database.windows.net/MSYN-PRODUCTION?"
    "driver=ODBC Driver 17 for SQL Server"
)
# SQL query
sql_query = """
SELECT DISTINCT
    CASE  
        WHEN ISNULL(TRIM(so.szCustPoId), '') = '' THEN si.szFPoId
        ELSE ISNULL(TRIM(so.szCustPoId), '') 
    END AS purchase_order_number,
    ISNULL(po.dtmCreated, '1900-01-01') AS date_created,
    ISNULL(so.dtmCreated, '1900-01-01') AS po_on_review_date,
    ISNULL(so.dtmOrder, '1900-01-01') AS po_approve_lv1_date,
    si.szFSoId AS sales_order_number,
    cnclOrd.bAlreadyTransferred AS canceled_order,
    rejected.bRejected AS rejected_order,
    ISNULL(purchOrd.dtmTransferred, '1900-01-01') AS send_to_wms_date,
    si.szCustId AS customer_id,
    cust.szName AS customer_name,
    ISNULL(custCat.szDescription, '') AS customer_category_description,
    ISNULL(delv.outTime, '1900-01-01') AS delivered_date_feedback_wms,
    ISNULL (porec.dtmReceipt, '1900-01-01') AS received_date,
    CASE 
        WHEN si.szFPoId = '' THEN so.dtmCreated 
        ELSE po.dtmCreated 
    END AS poDate,
    CASE 
        WHEN TRIM(ISNULL(si.szFSoId, '')) = '' THEN poi.szProductId 
        ELSE soi.szProductId 
    END AS product_id,
    CASE 
        WHEN TRIM(ISNULL(si.szFSoId, '')) = '' THEN prod.szName 
        ELSE prod1.szName 
    END AS product_name,
    CASE 
        WHEN TRIM(ISNULL(si.szFSoId, '')) = '' THEN poi.decQty 
        ELSE soi.decQty 
    END AS qty_order,
    CASE 
        WHEN TRIM(ISNULL(si.szFSoId, '')) = '' THEN poi.decAmount 
        ELSE soi.decAmount 
    END AS amount_incl_ppn_before_discount,
    CASE 
        WHEN TRIM(ISNULL(si.szFSoId, '')) = '' 
        THEN poi.decAmount - ISNULL(bonus.decDiscFakturDPP, 0) - ISNULL(bonus.decDiscFakturTax, 0)
        ELSE soi.decAmount - ISNULL(bonus.decDiscFakturDPP, 0) - ISNULL(bonus.decDiscFakturTax, 0)
    END AS nett_amount_incl_ppn,
    ISNULL((
        SELECT TOP 1 cdn.bApplied 
        FROM MSYN_INV_CloseDN cdn
        LEFT JOIN MSYN_INV_CloseDNItem cdnItem ON cdn.szCloseDNId = cdnItem.szCloseDNId
        WHERE cdnItem.szDNId = si.szFDnId
        ORDER BY dtmLastUpdated DESC
    ), 0) AS bStatusDN,
    ISNULL(FDN.dtmLastUpdated, '1900-01-01') AS dn_closed_date,
    si.szfdnid
FROM msyn_sd_salesinquiry si
LEFT JOIN bos_sd_fso so ON si.szFSoId = so.szFSoId
LEFT JOIN BOS_PUR_FPo_s po ON si.szFPoId = po.szFPo_sId
LEFT JOIN BOS_SD_FSoItem soi ON soi.szFSoId = so.szFSoId
LEFT JOIN BOS_PUR_FPo_sItem poi ON poi.szFPo_sId = po.szFPo_sId
LEFT JOIN BOS_INV_Product prod ON poi.szProductId = prod.szProductId
LEFT JOIN BOS_INV_Product prod1 ON soi.szProductId = prod1.szProductId
LEFT JOIN bos_ar_customer cust ON cust.szCustId = si.szCustId
LEFT JOIN BOS_PUR_FPoReceipt_s Porec ON Porec.szPoId = po.szFPo_sId AND Porec.szPoId<>''
LEFT JOIN BOS_PUR_FPoReceipt_sItem PorecI ON PorecI.szFPoReceipt_sId = Porec.szFPoReceipt_sId 
AND PorecI.szProductId = poi.szProductId
LEFT JOIN BOS_PUR_FPoReceipt_sItem PorecI1 ON PorecI1.szFPoReceipt_sId = Porec.szFPoReceipt_sId 
AND PorecI1.szProductId = soi.szProductId

left join BOS_SD_FDo do on do.szDoId = si.szFDoId
left join MSYN_INT_WMS_DeliveryConfirmation delv on delv.custOrderNo = si.szFSoId and delv.custOrderNo<>'' and do.szDoId = delv.szDoBosnetId
left join (select custOrderNo,code,orderNo,sum(qty) qty from MSYN_INT_WMS_DeliveryConfirmationItem group by custOrderNo,code,orderNo) delvItemJoinSO on delvItemJoinSO.custOrderNo = delv.custOrderNo and delv.orderNo=delvItemJoinSO.orderNo and delv.custOrderNo<>'' and delvItemJoinSO.code = soi.szProductId

left join BOS_SD_FInvoice invoice on invoice.szFInvoiceId = do.szFInvoiceId
left join (select szFInvoiceId,sum(decQty) decqty,szProductId from BOS_SD_FInvoiceItem group by szFInvoiceId,szProductId)invItem on invItem.szFInvoiceId = invoice.szFInvoiceId and invItem.szProductId = soi.szProductId 
left join BOS_AR_Category custCat on custCat.szCategoryId = cust.szCategory_2
left join MSYN_INT_PurchaseCancelOrder cancelOrd on cancelOrd.szDocId = si.szFSoId and cancelOrd.bAlreadyTransferred = 1
left join MSYN_INT_PurchaseOrder purchOrd on purchOrd.szDocId = si.szFSoId and purchOrd.bAlreadyTransferred = 1
left join (
								select soItem.szFSoId,bonus.szProductId, bonus.szParentId, bonus.szPaymentType, bonus.szOrderItemTypeId,
								sum(case when soItem.szPrincipalDiscRefId  = 'Invoice Disc. Item' then 
								case when soItem.bTaxable = 1 and (soItem.decDPP+soItem.decTax) <> 0
								then bonus.decBonusAmount *  soitem.decdpp/(soItem.decDPP+soItem.decTax) else bonus.decBonusAmount end
								else 0 end)as decDisc1DPP,
								sum(case when soItem.szPrincipalDiscRefId  = 'Invoice Disc. Item' then 
								case when soItem.bTaxable = 1 and (soItem.decDPP+soItem.decTax) <> 0 
								then bonus.decBonusAmount *  soitem.decTax/(soItem.decDPP+soItem.decTax) else 0 end
								else 0 end)as decDisc1Tax,
								sum(case when soItem.szPrincipalDiscRefId  = 'Invoice Disc. Item 2' then 
								case when soItem.bTaxable = 1 and (soItem.decDPP+soItem.decTax) <> 0 
								then bonus.decBonusAmount *  soitem.decdpp/(soItem.decDPP+soItem.decTax) else bonus.decBonusAmount end
								else 0 end)as decDisc2DPP,
								sum(case when soItem.szPrincipalDiscRefId  = 'Invoice Disc. Item 2' then 
								case when soItem.bTaxable = 1 and (soItem.decDPP+soItem.decTax) <> 0
								then bonus.decBonusAmount *  soitem.decTax/(soItem.decDPP+soItem.decTax) else bonus.decBonusAmount end
								else 0 end)as decDisc2Tax,
								sum(case when soItem.szPrincipalDiscRefId  = 'Invoice Disc.' then 
								case when soItem.bTaxable = 1 and (soItem.decDPP+soItem.decTax) <> 0
								then bonus.decBonusAmount *  soitem.decdpp/(soItem.decDPP+soItem.decTax) else bonus.decBonusAmount end
								else 0 end)as decDiscFakturDPP,
								sum(case when soItem.szPrincipalDiscRefId  = 'Invoice Disc.' then 
								case when soItem.bTaxable = 1 and (soItem.decDPP+soItem.decTax) <> 0
								then bonus.decBonusAmount *  soitem.decTax/(soItem.decDPP+soItem.decTax) else bonus.decBonusAmount end
								else 0 end)as decDiscFakturTax
								from BOS_SD_FSoItem soItem
								left join bos_sd_fso so on soitem.szFsoId = so.szFSoId
								left join BOS_SD_FSoItemBonusSource bonus on bonus.szFSoId = soItem.szFSoId and soitem.shItemNumber = bonus.shItemNumber
								where bonus.szProductId is not null AND so.dtmorder >= '2024-11-01'   group by soItem.szFSoId,bonus.szProductId, bonus.szParentId, bonus.szPaymentType, bonus.szOrderItemTypeId

) bonus 
on bonus.szFSoId = soi.szFSoId and soi.szProductId = bonus.szProductId and bonus.szParentId=soi.szParentId
and bonus.szPaymentType=soi.szPaymentType and bonus.szOrderItemTypeId=soi.szOrderItemTypeId

left join (select top 1 btApprovedLevel,gdapprovedId from BOS_GEN_ApprovedItem BGADitem 
		 order by BGADitem.btApprovedLevel desc) approvedlvl on approvedlvl.gdApprovedId = so.gdApprovedId
left join BOS_GEN_ApprovedItem gd1 on gd1.gdApprovedId = so.gdApprovedId and gd1.btApprovedLevel =1
left join BOS_GEN_ApprovedItem gd2 on gd2.gdApprovedId = so.gdApprovedId and gd2.btApprovedLevel =2
left join BOS_GEN_ApprovedItem gd3 on gd3.gdApprovedId = so.gdApprovedId and gd3.btApprovedLevel =3
                                           
left join MSYN_INV_FDn FDN on FDN.szfdnid = si.szFDNId
left join MSYN_INV_FDnItem	fdnItem1 on FDN.szFDNId = fdnItem1.szFDNId and fdnItem1.szProductId = soi.szProductId
left join (select top 1 szDoId,szDoIdOld from MSYN_INV_HistoryDODn order by dtmLastUpdated desc) Hist on Hist.szDoIdOld = do.szDoId
left join BOS_SD_FDo doHist on Hist.szDoId = doHist.szDoId
left join BOS_SD_FInvoice invoiceHist on invoiceHist.szFInvoiceId = doHist.szFInvoiceId
left join MSYN_INT_WMS_OperateTime WOPT on WOPT.CustOrderNo = so.szFSoId
left join BOS_SD_FInvoiceItem invItemHist on invItemHist.szFInvoiceId = invoiceHist.szFInvoiceId and invItemHist.szProductId = soi.szProductId
LEFT JOIN MSYN_INT_PurchaseCancelOrder cnclOrd ON si.szFSoId = cnclOrd.szDocId
LEFT JOIN BOS_GEN_Approved rejected ON so.gdApprovedId = rejected.gdApprovedId
WHERE 1=1 
AND (soi.szOrderItemTypeId = 'JUAL' OR poi.purchaseItemTypeId = 'BELI' OR poi.purchaseItemTypeId = 'BELI-BERIKAT')
AND (
    (si.szFPoId <> '' AND po.dtmPO >= '2024-11-01' AND po.dtmPO < '2024-12-01')
    OR (si.szFPoId = '' AND so.dtmOrder >= '2024-11-01' AND so.dtmOrder < '2024-12-01')
) 
AND so.dtmCreated >= '2024-11-01' AND so.dtmCreated < '2024-12-01'
AND delv.outTime >= '2024-11-01' AND delv.outTime < '2024-12-01'
AND custCat.szDescription IN ('GT', 'MTI')
AND ISNULL(cnclOrd.bAlreadyTransferred, 0) <> 1
AND ISNULL(rejected.bRejected, 0) <> 1
"""

# Connect to SQL Server using SQLAlchemy
engine = create_engine(sql_server_connection_str)

def fetch_data(sql_query):
    """Fetch data from SQL Server using the provided query."""
    df = pd.read_sql(sql_query, engine)
    return df

# Fetch data from SQL Server
df = fetch_data(sql_query)
print("Data fetched from SQL Server successfully.")

# Convert the 'OrderDate' column to string format to avoid pyarrow conversion issues
if 'date_created' in df.columns:
    df['date_created'] = df['date_created'].astype(str)

# Google BigQuery connection details
credentials_gbq = 'skintific-data-warehouse-ea77119e2e7a.json'
project_id = 'skintific-data-warehouse'
dataset_id = 'bosnet'
table_id = 'po_tracking_mtd'

# Set up credentials for BigQuery
credentials = service_account.Credentials.from_service_account_file(credentials_gbq)
pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = project_id

# Initialize BigQuery client
client = bigquery.Client(credentials=credentials)

# Step 1: Clear the target table before inserting new data
def clear_target_table():
    query = f"DELETE FROM `{project_id}.{dataset_id}.{table_id}` WHERE TRUE"
    client.query(query).result()  # Execute the query and wait for the result

# Clear the table
clear_target_table()
print(f"Table {table_id} cleared successfully.")

# Step 2: Upload new data to the cleared table
def upload_to_gbq(df_chunk):
    pandas_gbq.to_gbq(df_chunk, f"{dataset_id}.{table_id}", project_id=project_id, if_exists='replace')

upload_to_gbq(df)
print("Data uploaded to BigQuery successfully.")