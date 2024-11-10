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
SELECT 
BOS_SD_FInvoice.szWorkplaceId AS WorkplaceId, 
ISNULL(BOS_GL_Workplace.szName,'') AS WorkplaceName, 
ISNULL(BOS_SD_FDo.szCustPoId , '')AS SoReference, 
ISNULL(BOS_SD_FDo.szFSoId, '')AS SoId, 
ISNULL(BOS_SD_FDo.szDoId, '') AS DoId, 
BOS_SD_FInvoice.szFInvoiceId AS InvoiceId, 
cast(BOS_SD_FInvoice.dtmPeriode as date) AS OrderDate, 
UPPER(BOS_SD_FInvoice.szSalesId) AS SalesID, 
Sales.szName AS EmployeeName, 
ISNULL(BOS_SD_FDo.szCustId, BOS_SD_FInvoice.szCustId) AS CustomerId, 
CASE WHEN BOS_SD_FDo.szCustId IS NOT NULL 
THEN ISNULL(CustomerDelivery.szName, '') ELSE ISNULL(BOS_AR_Customer.szName, '') END AS CustomerName, 
ISNULL(BOS_AR_Customer.CustszAddress_1, '') AS Address, 
BOS_AR_CustLocation.LocationszCity AS Muncipality, 
BOS_SD_FInvoiceItem.szProductId AS ProductID, 
isnull(BOS_INV_Product.szName,'') AS ProductName, 
ISNULL((BOS_SD_FInvoiceItem.decAmount / NULLIF(BOS_SD_FInvoiceItem.decQty, 0)), 0) AS Price, 
BOS_SD_FInvoiceItem.decQty AS Quantity, 
                    (
		                    CASE 
			                    WHEN BOS_SD_FInvoiceItem.bTaxable = 0
				                    THEN BOS_SD_FInvoiceItem.decAmount
			                    ELSE (
					                    CASE 
						                    WHEN BOS_SD_FInvoiceItem.decDPP + BOS_SD_FInvoiceItem.decTax <> 0
							                    THEN BOS_SD_FInvoiceItem.decDPP + (BOS_SD_FInvoiceItem.decDiscount * BOS_SD_FInvoiceItem.decDPP / (BOS_SD_FInvoiceItem.decDPP + BOS_SD_FInvoiceItem.decTax))
						                    ELSE BOS_SD_FInvoiceItem.decDPP + BOS_SD_FInvoiceItem.decTax
						                    END
					                    )
			                    END
		                    )
             AS Amount, 
             BOS_SD_FInvoiceItem.decTax + isnull(a.sumDecBonusAmountTax,0) AS Tax, 
                    (
		                    CASE 
			                    WHEN BOS_SD_FInvoiceItem.bTaxable = 0
				                    THEN BOS_SD_FInvoiceItem.decAmount-BOS_SD_FInvoiceItem.decDiscount-coalesce(a.sumDecBonusAmount,0)
			                    ELSE (
					                    CASE 
						                    WHEN BOS_SD_FInvoiceItem.decDPP + BOS_SD_FInvoiceItem.decTax <> 0
							                    THEN BOS_SD_FInvoiceItem.decDPP - (coalesce(a.sumDecBonusAmount,0) * BOS_SD_FInvoiceItem.decDPP / (BOS_SD_FInvoiceItem.decDPP + BOS_SD_FInvoiceItem.decTax))
						                    ELSE BOS_SD_FInvoiceItem.decDPP + BOS_SD_FInvoiceItem.decTax
						                    END
					                    )
			                    END
		                    )
             AS NettAmount, 
                    ( (
		                    CASE 
			                    WHEN BOS_SD_FInvoiceItem.bTaxable = 0
				                    THEN BOS_SD_FInvoiceItem.decAmount-BOS_SD_FInvoiceItem.decDiscount-coalesce(a.sumDecBonusAmount,0)
			                    ELSE (
					                    CASE 
						                    WHEN BOS_SD_FInvoiceItem.decDPP + BOS_SD_FInvoiceItem.decTax <> 0
							                    THEN BOS_SD_FInvoiceItem.decDPP - (coalesce(a.sumDecBonusAmount,0) * BOS_SD_FInvoiceItem.decDPP / (BOS_SD_FInvoiceItem.decDPP + BOS_SD_FInvoiceItem.decTax))
						                    ELSE BOS_SD_FInvoiceItem.decDPP + BOS_SD_FInvoiceItem.decTax
						                    END
					                    )
			                    END
		                    ) + BOS_SD_FInvoiceItem.decTax + isnull(a.sumDecBonusAmountTax,0) ) 
             AS NettAmountIncTax, 
                (
		                CASE 
			                WHEN BOS_SD_FInvoiceItem.bTaxable = 0
				                THEN BOS_SD_FInvoiceItem.decDiscount
			                ELSE (
					                CASE 
						                WHEN BOS_SD_FInvoiceItem.decDPP + BOS_SD_FInvoiceItem.decTax <> 0 THEN
							                BOS_SD_FInvoiceItem.decDiscount * BOS_SD_FInvoiceItem.decDPP / (BOS_SD_FInvoiceItem.decDPP + BOS_SD_FInvoiceItem.decTax)
						                ELSE 
                                            BOS_SD_FInvoiceItem.decDPP + BOS_SD_FInvoiceItem.decTax
						                END
					                )
			                END
		                ) + 
               (        CASE
                            WHEN BOS_SD_FInvoiceItem.bTaxable = 0
                                THEN coalesce(a.sumDecBonusAmount,0)
                        ELSE
                                CASE
                                    WHEN BOS_SD_FInvoiceItem.decDPP + BOS_SD_FInvoiceItem.decTax <> 0 THEN
                                        coalesce(a.sumDecBonusAmount,0) * BOS_SD_FInvoiceItem.decDPP / (BOS_SD_FInvoiceItem.decDPP + BOS_SD_FInvoiceItem.decTax)
                                    ELSE
                                        coalesce(a.sumDecBonusAmount,0)
                                    END
                        END
                )
             AS Discount, 
                        (
		                        CASE 
			                        WHEN BOS_SD_FInvoiceItem.bTaxable = 0
				                        THEN BOS_SD_FInvoiceItem.decDiscount
			                        ELSE (
					                        CASE 
						                        WHEN BOS_SD_FInvoiceItem.decDPP + BOS_SD_FInvoiceItem.decTax <> 0
							                        THEN BOS_SD_FInvoiceItem.decDiscount * BOS_SD_FInvoiceItem.decDPP / (BOS_SD_FInvoiceItem.decDPP + BOS_SD_FInvoiceItem.decTax)
						                        ELSE BOS_SD_FInvoiceItem.decDPP + BOS_SD_FInvoiceItem.decTax
						                        END
					                        )
			                        END
		                        )
             AS DiscountItem,   
                 CONCAT(BOS_SD_FInvoice.szFInvoiceId, '-', BOS_SD_FInvoiceItem.szProductId) AS InvoiceProductID
                    FROM BOS_SD_FInvoiceItem LEFT JOIN BOS_SD_FInvoice ON BOS_SD_FInvoice.szFInvoiceId = BOS_SD_FInvoiceItem.szFInvoiceId 
             		LEFT JOIN BOS_SD_FInvoice AS INV_CANCEL ON BOS_SD_FInvoice.szCanceledByInvId = INV_CANCEL.szFInvoiceId 
             		LEFT JOIN BOS_GL_Workplace ON BOS_GL_Workplace.szWorkplaceId = BOS_SD_FInvoice.szWorkplaceId 
             		LEFT JOIN BOS_SD_FDo ON BOS_SD_FDo.szDoId = BOS_SD_FInvoiceItem.szDoId 
             		LEFT JOIN BOS_SD_FDoReversal AS REVERSAL_DO ON BOS_SD_FDo.szDoId = REVERSAL_DO.szFDoId and REVERSAL_DO.bApplied = 1 
             		LEFT JOIN BOS_SD_FDoReversal AS REVERSAL_REVERSE_DO ON BOS_SD_FDo.szDoId = REVERSAL_REVERSE_DO.szReverseDoId and REVERSAL_REVERSE_DO.bApplied = 1 
             		LEFT JOIN BOS_AR_Customer CustomerDelivery ON CustomerDelivery.szCustId = BOS_SD_FDo.szCustId 
             		LEFT JOIN BOS_SD_DistrChannel ON BOS_SD_DistrChannel.szDistrChannelId = CustomerDelivery.szDistrChannelId 
             		LEFT JOIN BOS_AR_CustLocation ON BOS_AR_CustLocation.szCustId = CustomerDelivery.szDeliverToCustId and BOS_AR_CustLocation.btLocId = 0                     
                    LEFT JOIN BOS_AR_Category AS CATEGORY1 ON CATEGORY1.szCategoryId = CustomerDelivery.szCategory_1
                    LEFT JOIN BOS_AR_Category AS CATEGORY2 ON CATEGORY2.szCategoryId = CustomerDelivery.szCategory_2
                    LEFT JOIN BOS_AR_Category AS CATEGORY3 ON CATEGORY3.szCategoryId = CustomerDelivery.szCategory_3
                    LEFT JOIN BOS_AR_Category AS CATEGORY4 ON CATEGORY4.szCategoryId = CustomerDelivery.szCategory_4
                    LEFT JOIN BOS_AR_Category AS CATEGORY5 ON CATEGORY5.szCategoryId = CustomerDelivery.szCategory_5
                    LEFT JOIN BOS_AR_Category AS CATEGORY6 ON CATEGORY6.szCategoryId = CustomerDelivery.szCategory_6
                    LEFT JOIN BOS_AR_Category AS CATEGORY7 ON CATEGORY7.szCategoryId = CustomerDelivery.szCategory_7
                    LEFT JOIN BOS_AR_Category AS CATEGORY8 ON CATEGORY8.szCategoryId = CustomerDelivery.szCategory_8
                    LEFT JOIN BOS_AR_Category AS CATEGORY9 ON CATEGORY9.szCategoryId = CustomerDelivery.szCategory_9
                    LEFT JOIN BOS_AR_Category AS CATEGORY10 ON CATEGORY10.szCategoryId = CustomerDelivery.szCategory_10 
                    LEFT JOIN BOS_AR_Customer ON BOS_AR_Customer.szCustId = BOS_SD_FInvoice.szCustId 
                    LEFT JOIN BOS_GL_Workplace AS COLL_Workplace ON COLL_Workplace.szWorkplaceId = BOS_AR_Customer.szCollWorkplaceId 
                    LEFT JOIN BOS_TIN_CustTaxIndConfig AS BOS_TIN_CustTaxIndConfig ON BOS_TIN_CustTaxIndConfig.szCustId = BOS_AR_Customer.szCustId 
                    LEFT JOIN BOS_AR_Customer AS BAC_SOLD ON BAC_SOLD.szCustId = BOS_AR_Customer.szSoldToCustId 
                    LEFT JOIN BOS_AR_CustInvoice ON BOS_AR_CustInvoice.szCustId = BOS_AR_Customer.szInvoiceToCustId 
                    LEFT JOIN BOS_GL_Workplace AS DO_Workplace ON DO_Workplace.szWorkplaceId = coalesce(BOS_SD_FDo.szWorkplaceId, BOS_SD_FInvoice.szWorkplaceId) 
                    LEFT JOIN BOS_SD_SalesOrganization AS SO1 ON SO1.szSalesOrgId = DO_Workplace.szSalesOrgId 
                    LEFT JOIN BOS_SD_SalesOrganization AS SO2 ON SO2.szSalesOrgId = SO1.szParentSalesOrgId 
                    LEFT JOIN BOS_SD_SalesOrganization AS SO3 ON SO3.szSalesOrgId = SO2.szParentSalesOrgId 
                    LEFT JOIN BOS_INV_Product ON BOS_INV_Product.szProductId = BOS_SD_FInvoiceItem.szProductId                     
                    LEFT JOIN BOS_INV_ProductCategory prodCat1 ON prodCat1.szProductCategoryId = BOS_INV_Product.szCategory_1
                    LEFT JOIN BOS_INV_ProductCategory prodCat2 ON prodCat2.szProductCategoryId = BOS_INV_Product.szCategory_2
                    LEFT JOIN BOS_INV_ProductCategory prodCat3 ON prodCat3.szProductCategoryId = BOS_INV_Product.szCategory_3
                    LEFT JOIN BOS_INV_ProductCategory prodCat4 ON prodCat4.szProductCategoryId = BOS_INV_Product.szCategory_4
                    LEFT JOIN BOS_INV_ProductCategory prodCat5 ON prodCat5.szProductCategoryId = BOS_INV_Product.szCategory_5
                    LEFT JOIN BOS_INV_ProductCategory prodCat6 ON prodCat6.szProductCategoryId = BOS_INV_Product.szCategory_6
                    LEFT JOIN BOS_INV_ProductCategory prodCat7 ON prodCat7.szProductCategoryId = BOS_INV_Product.szCategory_7
                    LEFT JOIN BOS_INV_ProductCategory prodCat8 ON prodCat8.szProductCategoryId = BOS_INV_Product.szCategory_8
                    LEFT JOIN BOS_INV_ProductCategory prodCat9 ON prodCat9.szProductCategoryId = BOS_INV_Product.szCategory_9
                    LEFT JOIN BOS_INV_ProductCategory prodCat10 ON prodCat10.szProductCategoryId = BOS_INV_Product.szCategory_10 
                    LEFT JOIN BOS_INV_ProductPurchaseInfo ON BOS_INV_ProductPurchaseInfo.szProductId = BOS_INV_Product.szProductId 
                    LEFT JOIN BOS_AP_Supplier ON BOS_AP_Supplier.szSuppId = BOS_INV_ProductPurchaseInfo.szDefaultSuppId 
                    LEFT JOIN BOS_GEN_FlagCashCredit ON BOS_GEN_FlagCashCredit.bCash = CONVERT(tinyint, BOS_SD_FInvoice.bCash) 
                    LEFT JOIN BOS_GEN_YesNo AS BGY_Taxable ON BGY_Taxable.bYesNo = CONVERT(tinyint, BOS_SD_FInvoiceItem.bTaxable) 
                    LEFT JOIN BOS_SD_CustCollRoutine ON BOS_SD_CustCollRoutine.szCustId = BOS_SD_FInvoice.szCustId 
                    LEFT JOIN BOS_SD_Route ON BOS_SD_Route.szRouteId = BOS_SD_CustCollRoutine.szDefaultRouteId 
                    LEFT JOIN BOS_PI_Employee AS COLLECTOR ON COLLECTOR.szEmployeeId = BOS_SD_Route.szOpUserId 
                    LEFT JOIN BOS_PI_Employee AS Sales ON Sales.szEmployeeId = BOS_SD_FInvoice.szSalesId 
                    LEFT JOIN BOS_PI_Employee AS SupSales ON SupSales.szEmployeeId = Sales.szSupervisorId 
                    LEFT JOIN BOS_PI_Employee AS EmpSup2 ON EmpSup2.szEmployeeId = BOS_SD_FDo.szSalesSupervisorId_2 
                    LEFT JOIN BOS_PI_Employee AS EmpSup3 ON EmpSup3.szEmployeeId = BOS_SD_FDo.szSalesSupervisorId_3 
                    LEFT JOIN BOS_PI_Employee AS DRIVER ON DRIVER.szEmployeeId = BOS_SD_FDo.szDriverId 
                    LEFT JOIN BOS_BIZ_PeriodicCalendar PerCal ON PerCal.dtmDate = cast(BOS_SD_FInvoice.dtmPeriode as date) 
                    LEFT JOIN BOS_INV_CompUomItem on BOS_INV_CompUomItem.szCompUomId = BOS_INV_Product.szCompUomId and BOS_INV_CompUomItem.shItemNumber = 0 
                    LEFT JOIN BOS_GL_Company ON BOS_GL_Company.szCompanyId = DO_Workplace.szCompanyId 
                    LEFT JOIN BOS_INV_Vehicle ON BOS_SD_FDo.szVehicleId = BOS_INV_Vehicle.szVehicleId 
                    LEFT JOIN BOS_INV_VehicleCapacity ON BOS_INV_VehicleCapacity.szVehicleCapacityId = BOS_INV_Vehicle.szVehicleCapacityId 
                    LEFT JOIN BOS_INV_OrderItemType ON BOS_SD_FInvoiceItem.szOrderItemTypeId = BOS_INV_OrderItemType.szOrderItemTypeId
                	LEFT JOIN BOS_INV_ProductTechnicalSpecInfo specInfo on specInfo.szProductId = BOS_SD_FInvoiceItem.szProductId
                LEFT JOIN (
	                 SELECT BOS_SD_FInvItemBonusSource.szfinvoiceid, BOS_SD_FInvItemBonusSource.szproductid, 
                            BOS_SD_FInvItemBonusSource.szFDoId, BOS_SD_FInvItemBonusSource.szParentId, BOS_SD_FInvItemBonusSource.szPaymentType, 
                            BOS_SD_FInvItemBonusSource.szOrderItemTypeId
		                ,sum(BOS_SD_FInvItemBonusSource.decbonusamount) AS sumDecBonusAmount
                        ,sum(case when BOS_SD_FinvoiceItem.decDiscount=0 then 0 else BOS_SD_FInvItemBonusSource.decbonusamount * BOS_SD_FinvoiceItem.decTax / BOS_SD_FinvoiceItem.decDiscount end) as sumDecBonusAmountTax
	                FROM BOS_SD_FInvItemBonusSource 
                    JOIN BOS_SD_FinvoiceItem ON BOS_SD_FinvoiceItem.szFinvoiceId = BOS_SD_FInvItemBonusSource.szFInvoiceId
                        AND BOS_SD_FinvoiceItem.shItemNumber = BOS_SD_FInvItemBonusSource.shItemNumber
                    WHERE BOS_SD_FinvoiceItem.szPaymentType IN ('TDB', 'DOB')
	                GROUP BY BOS_SD_FInvItemBonusSource.szfinvoiceid, BOS_SD_FInvItemBonusSource.szproductid, BOS_SD_FInvItemBonusSource.szFDoId, 
                                BOS_SD_FInvItemBonusSource.szParentId, BOS_SD_FInvItemBonusSource.szPaymentType, BOS_SD_FInvItemBonusSource.szOrderItemTypeId
	                ) a ON a.szFInvoiceId = BOS_SD_FInvoiceItem.szFInvoiceId
	                AND a.szProductId = BOS_SD_FInvoiceItem.szProductId
                    AND a.szFDoId = BOS_SD_FInvoiceItem.szDoId
                    AND a.szParentId = BOS_SD_FInvoiceItem.szParentId
                    AND a.szPaymentType = BOS_SD_FInvoiceItem.szPaymentType
                    AND a.szOrderItemTypeId = BOS_SD_FInvoiceItem.szOrderItemTypeId
                LEFT JOIN BOS_GL_TaxType ON BOS_GL_TaxType.szTaxTypeId = BOS_INV_Product.szTaxTypeId
                LEFT JOIN NG_MART_ProductInfo on NG_MART_ProductInfo.productId = BOS_SD_FInvoiceItem.szProductId
                LEFT JOIN BOS_INV_Principal as princip on princip.szPrincipalId = NG_MART_ProductInfo.principalId  
                 LEFT MERGE JOIN 
                                            (
                                              SELECT szVehicleId = BOS_SD_FDo.szVehicleId, dtmDate = cast(BOS_SD_FInvoice.dtmPeriode as date) 
                                                   , decVolume1 = SUM(CASE WHEN BOS_SD_FInvoiceItem.decQty <= 0 THEN 0 ELSE BOS_SD_FInvoiceItem.decQty * ISNULL(specInfo.decVolume, 0) / 1000 END)
                                                   , decWeight1 = SUM(CASE WHEN BOS_SD_FInvoiceItem.decQty <= 0 THEN 0 ELSE BOS_SD_FInvoiceItem.decQty * ISNULL(specInfo.decWeight, 0) / 1000 END)
                                               FROM BOS_SD_FInvoiceItem LEFT JOIN BOS_SD_FInvoice ON BOS_SD_FInvoice.szFInvoiceId = BOS_SD_FInvoiceItem.szFInvoiceId LEFT JOIN BOS_SD_FInvoice AS INV_CANCEL ON BOS_SD_FInvoice.szCanceledByInvId = INV_CANCEL.szFInvoiceId LEFT JOIN BOS_GL_Workplace ON BOS_GL_Workplace.szWorkplaceId = BOS_SD_FInvoice.szWorkplaceId LEFT JOIN BOS_SD_FDo ON BOS_SD_FDo.szDoId = BOS_SD_FInvoiceItem.szDoId LEFT JOIN BOS_SD_FDoReversal AS REVERSAL_DO ON BOS_SD_FDo.szDoId = REVERSAL_DO.szFDoId and REVERSAL_DO.bApplied = 1 LEFT JOIN BOS_SD_FDoReversal AS REVERSAL_REVERSE_DO ON BOS_SD_FDo.szDoId = REVERSAL_REVERSE_DO.szReverseDoId and REVERSAL_REVERSE_DO.bApplied = 1 LEFT JOIN BOS_AR_Customer CustomerDelivery ON CustomerDelivery.szCustId = BOS_SD_FDo.szCustId LEFT JOIN BOS_SD_DistrChannel ON BOS_SD_DistrChannel.szDistrChannelId = CustomerDelivery.szDistrChannelId LEFT JOIN BOS_AR_CustLocation ON BOS_AR_CustLocation.szCustId = CustomerDelivery.szDeliverToCustId and BOS_AR_CustLocation.btLocId = 0                     
                    LEFT JOIN BOS_AR_Category AS CATEGORY1 ON CATEGORY1.szCategoryId = CustomerDelivery.szCategory_1
                    LEFT JOIN BOS_AR_Category AS CATEGORY2 ON CATEGORY2.szCategoryId = CustomerDelivery.szCategory_2
                    LEFT JOIN BOS_AR_Category AS CATEGORY3 ON CATEGORY3.szCategoryId = CustomerDelivery.szCategory_3
                    LEFT JOIN BOS_AR_Category AS CATEGORY4 ON CATEGORY4.szCategoryId = CustomerDelivery.szCategory_4
                    LEFT JOIN BOS_AR_Category AS CATEGORY5 ON CATEGORY5.szCategoryId = CustomerDelivery.szCategory_5
                    LEFT JOIN BOS_AR_Category AS CATEGORY6 ON CATEGORY6.szCategoryId = CustomerDelivery.szCategory_6
                    LEFT JOIN BOS_AR_Category AS CATEGORY7 ON CATEGORY7.szCategoryId = CustomerDelivery.szCategory_7
                    LEFT JOIN BOS_AR_Category AS CATEGORY8 ON CATEGORY8.szCategoryId = CustomerDelivery.szCategory_8
                    LEFT JOIN BOS_AR_Category AS CATEGORY9 ON CATEGORY9.szCategoryId = CustomerDelivery.szCategory_9
                    LEFT JOIN BOS_AR_Category AS CATEGORY10 ON CATEGORY10.szCategoryId = CustomerDelivery.szCategory_10 LEFT JOIN BOS_AR_Customer ON BOS_AR_Customer.szCustId = BOS_SD_FInvoice.szCustId LEFT JOIN BOS_GL_Workplace AS COLL_Workplace ON COLL_Workplace.szWorkplaceId = BOS_AR_Customer.szCollWorkplaceId LEFT JOIN BOS_TIN_CustTaxIndConfig AS BOS_TIN_CustTaxIndConfig ON BOS_TIN_CustTaxIndConfig.szCustId = BOS_AR_Customer.szCustId LEFT JOIN BOS_AR_Customer AS BAC_SOLD ON BAC_SOLD.szCustId = BOS_AR_Customer.szSoldToCustId LEFT JOIN BOS_AR_CustInvoice ON BOS_AR_CustInvoice.szCustId = BOS_AR_Customer.szInvoiceToCustId LEFT JOIN BOS_GL_Workplace AS DO_Workplace ON DO_Workplace.szWorkplaceId = coalesce(BOS_SD_FDo.szWorkplaceId, BOS_SD_FInvoice.szWorkplaceId) LEFT JOIN BOS_SD_SalesOrganization AS SO1 ON SO1.szSalesOrgId = DO_Workplace.szSalesOrgId LEFT JOIN BOS_SD_SalesOrganization AS SO2 ON SO2.szSalesOrgId = SO1.szParentSalesOrgId LEFT JOIN BOS_SD_SalesOrganization AS SO3 ON SO3.szSalesOrgId = SO2.szParentSalesOrgId LEFT JOIN BOS_INV_Product ON BOS_INV_Product.szProductId = BOS_SD_FInvoiceItem.szProductId                     
                    LEFT JOIN BOS_INV_ProductCategory prodCat1 ON prodCat1.szProductCategoryId = BOS_INV_Product.szCategory_1
                    LEFT JOIN BOS_INV_ProductCategory prodCat2 ON prodCat2.szProductCategoryId = BOS_INV_Product.szCategory_2
                    LEFT JOIN BOS_INV_ProductCategory prodCat3 ON prodCat3.szProductCategoryId = BOS_INV_Product.szCategory_3
                    LEFT JOIN BOS_INV_ProductCategory prodCat4 ON prodCat4.szProductCategoryId = BOS_INV_Product.szCategory_4
                    LEFT JOIN BOS_INV_ProductCategory prodCat5 ON prodCat5.szProductCategoryId = BOS_INV_Product.szCategory_5
                    LEFT JOIN BOS_INV_ProductCategory prodCat6 ON prodCat6.szProductCategoryId = BOS_INV_Product.szCategory_6
                    LEFT JOIN BOS_INV_ProductCategory prodCat7 ON prodCat7.szProductCategoryId = BOS_INV_Product.szCategory_7
                    LEFT JOIN BOS_INV_ProductCategory prodCat8 ON prodCat8.szProductCategoryId = BOS_INV_Product.szCategory_8
                    LEFT JOIN BOS_INV_ProductCategory prodCat9 ON prodCat9.szProductCategoryId = BOS_INV_Product.szCategory_9
                    LEFT JOIN BOS_INV_ProductCategory prodCat10 ON prodCat10.szProductCategoryId = BOS_INV_Product.szCategory_10 LEFT JOIN BOS_INV_ProductPurchaseInfo ON BOS_INV_ProductPurchaseInfo.szProductId = BOS_INV_Product.szProductId LEFT JOIN BOS_AP_Supplier ON BOS_AP_Supplier.szSuppId = BOS_INV_ProductPurchaseInfo.szDefaultSuppId LEFT JOIN BOS_GEN_FlagCashCredit ON BOS_GEN_FlagCashCredit.bCash = CONVERT(tinyint, BOS_SD_FInvoice.bCash) LEFT JOIN BOS_GEN_YesNo AS BGY_Taxable ON BGY_Taxable.bYesNo = CONVERT(tinyint, BOS_SD_FInvoiceItem.bTaxable) LEFT JOIN BOS_SD_CustCollRoutine ON BOS_SD_CustCollRoutine.szCustId = BOS_SD_FInvoice.szCustId LEFT JOIN BOS_SD_Route ON BOS_SD_Route.szRouteId = BOS_SD_CustCollRoutine.szDefaultRouteId LEFT JOIN BOS_PI_Employee AS COLLECTOR ON COLLECTOR.szEmployeeId = BOS_SD_Route.szOpUserId LEFT JOIN BOS_PI_Employee AS Sales ON Sales.szEmployeeId = BOS_SD_FInvoice.szSalesId LEFT JOIN BOS_PI_Employee AS SupSales ON SupSales.szEmployeeId = Sales.szSupervisorId LEFT JOIN BOS_PI_Employee AS EmpSup2 ON EmpSup2.szEmployeeId = BOS_SD_FDo.szSalesSupervisorId_2 LEFT JOIN BOS_PI_Employee AS EmpSup3 ON EmpSup3.szEmployeeId = BOS_SD_FDo.szSalesSupervisorId_3 LEFT JOIN BOS_PI_Employee AS DRIVER ON DRIVER.szEmployeeId = BOS_SD_FDo.szDriverId LEFT JOIN BOS_BIZ_PeriodicCalendar PerCal ON PerCal.dtmDate = cast(BOS_SD_FInvoice.dtmPeriode as date) LEFT JOIN BOS_INV_CompUomItem on BOS_INV_CompUomItem.szCompUomId = BOS_INV_Product.szCompUomId and BOS_INV_CompUomItem.shItemNumber = 0 LEFT JOIN BOS_GL_Company ON BOS_GL_Company.szCompanyId = DO_Workplace.szCompanyId LEFT JOIN BOS_INV_Vehicle ON BOS_SD_FDo.szVehicleId = BOS_INV_Vehicle.szVehicleId LEFT JOIN BOS_INV_VehicleCapacity ON BOS_INV_VehicleCapacity.szVehicleCapacityId = BOS_INV_Vehicle.szVehicleCapacityId LEFT JOIN BOS_INV_OrderItemType ON BOS_SD_FInvoiceItem.szOrderItemTypeId = BOS_INV_OrderItemType.szOrderItemTypeId
                LEFT JOIN BOS_INV_ProductTechnicalSpecInfo specInfo on specInfo.szProductId = BOS_SD_FInvoiceItem.szProductId
                LEFT JOIN (
	                 SELECT BOS_SD_FInvItemBonusSource.szfinvoiceid, BOS_SD_FInvItemBonusSource.szproductid, 
                            BOS_SD_FInvItemBonusSource.szFDoId, BOS_SD_FInvItemBonusSource.szParentId, BOS_SD_FInvItemBonusSource.szPaymentType, 
                            BOS_SD_FInvItemBonusSource.szOrderItemTypeId
		                ,sum(BOS_SD_FInvItemBonusSource.decbonusamount) AS sumDecBonusAmount
                        ,sum(case when BOS_SD_FinvoiceItem.decDiscount=0 then 0 else BOS_SD_FInvItemBonusSource.decbonusamount * BOS_SD_FinvoiceItem.decTax / BOS_SD_FinvoiceItem.decDiscount end) as sumDecBonusAmountTax
	                FROM BOS_SD_FInvItemBonusSource 
                    JOIN BOS_SD_FinvoiceItem ON BOS_SD_FinvoiceItem.szFinvoiceId = BOS_SD_FInvItemBonusSource.szFInvoiceId
                        AND BOS_SD_FinvoiceItem.shItemNumber = BOS_SD_FInvItemBonusSource.shItemNumber
                    WHERE BOS_SD_FinvoiceItem.szPaymentType IN ('TDB', 'DOB')
	                GROUP BY BOS_SD_FInvItemBonusSource.szfinvoiceid, BOS_SD_FInvItemBonusSource.szproductid, BOS_SD_FInvItemBonusSource.szFDoId, 
                                BOS_SD_FInvItemBonusSource.szParentId, BOS_SD_FInvItemBonusSource.szPaymentType, BOS_SD_FInvItemBonusSource.szOrderItemTypeId
	                ) a ON a.szFInvoiceId = BOS_SD_FInvoiceItem.szFInvoiceId
	                AND a.szProductId = BOS_SD_FInvoiceItem.szProductId
                    AND a.szFDoId = BOS_SD_FInvoiceItem.szDoId
                    AND a.szParentId = BOS_SD_FInvoiceItem.szParentId
                    AND a.szPaymentType = BOS_SD_FInvoiceItem.szPaymentType
                    AND a.szOrderItemTypeId = BOS_SD_FInvoiceItem.szOrderItemTypeId
                LEFT JOIN BOS_GL_TaxType ON BOS_GL_TaxType.szTaxTypeId = BOS_INV_Product.szTaxTypeId
                LEFT JOIN NG_MART_ProductInfo on NG_MART_ProductInfo.productId = BOS_SD_FInvoiceItem.szProductId
                LEFT JOIN BOS_INV_Principal as princip on princip.szPrincipalId = NG_MART_ProductInfo.principalId  
                  WHERE  ((BOS_SD_FInvoiceItem.szProductId <> '' AND BOS_SD_FInvoiceItem.szInvItemType = 'DOR') OR BOS_SD_FInvoiceItem.szInvItemType <> 'DOR') AND BOS_SD_FInvoice.dtmPeriode >= '06/01/2024' AND BOS_SD_FInvoice.dtmPeriode <= '11/30/2024' AND DO_Workplace.szWorkplaceId = 'HO-G2G' AND REVERSAL_DO.szFReversalId is null and REVERSAL_REVERSE_DO.szFReversalId is null  AND BOS_SD_FInvoice.bVoid = 0 AND BOS_SD_FInvoice.bApplied = 1  
                                              GROUP BY BOS_SD_FDo.szVehicleId, cast(BOS_SD_FInvoice.dtmPeriode as date)
                                             ) AS TableCapacityTotal ON TableCapacityTotal.szVehicleId = BOS_SD_FDo.szVehicleId AND TableCapacityTotal.dtmDate = cast(BOS_SD_FInvoice.dtmPeriode as date) LEFT JOIN BOS_SM_ConfigTemplateItem ConfigTemplateLokiCostTable ON 
                                                        ConfigTemplateLokiCostTable.szConfigTemplateId = 'com.kontinum.bos.sd.orDoSoComparison3' 
            				                        AND ConfigTemplateLokiCostTable.szConfigItemTypeId = 'WOR'
            						                AND ConfigTemplateLokiCostTable.szConfigItemId = '00001'
                                              LEFT JOIN BOS_SM_ConfigInfoItem ConfigInfoLokiCostTable ON 
                                                        ConfigInfoLokiCostTable.szConfigTemplateId = ConfigTemplateLokiCostTable.szConfigTemplateId
            						                AND ConfigInfoLokiCostTable.szConfigItemTypeId = ConfigTemplateLokiCostTable.szConfigItemTypeId 
            						                AND ConfigInfoLokiCostTable.szConfigItemId = ConfigTemplateLokiCostTable.szConfigItemId
            						                AND ConfigInfoLokiCostTable.szConfigItemTypeValue = DO_Workplace.szWorkplaceId LEFT JOIN BOS_SM_ConfigTemplateItem ConfigTemplateVehicleLokiFactorTable ON 
                                                        ConfigTemplateVehicleLokiFactorTable.szConfigTemplateId = 'com.kontinum.bos.sd.orDoSoComparison3' 
            				                        AND ConfigTemplateVehicleLokiFactorTable.szConfigItemTypeId = 'WOR'
            						                AND ConfigTemplateVehicleLokiFactorTable.szConfigItemId = '00002'
                                              LEFT JOIN BOS_SM_ConfigInfoItem ConfigInfoVehicleLokiFactorTable ON 
                                                        ConfigInfoVehicleLokiFactorTable.szConfigTemplateId = ConfigTemplateVehicleLokiFactorTable.szConfigTemplateId
            						                AND ConfigInfoVehicleLokiFactorTable.szConfigItemTypeId = ConfigTemplateVehicleLokiFactorTable.szConfigItemTypeId 
            						                AND ConfigInfoVehicleLokiFactorTable.szConfigItemId = ConfigTemplateVehicleLokiFactorTable.szConfigItemId
            						                AND ConfigInfoVehicleLokiFactorTable.szConfigItemTypeValue = DO_Workplace.szWorkplaceId LEFT JOIN BOS_SM_ConfigTemplateItem ConfigTemplateCurrentRateTable ON 
                                                        ConfigTemplateCurrentRateTable.szConfigTemplateId = 'com.kontinum.bos.sd.orDoSoComparison3' 
            				                        AND ConfigTemplateCurrentRateTable.szConfigItemTypeId = 'WOR'
            						                AND ConfigTemplateCurrentRateTable.szConfigItemId = '00003'
                                              LEFT JOIN BOS_SM_ConfigInfoItem ConfigInfoCurrentRateTable ON 
                                                        ConfigInfoCurrentRateTable.szConfigTemplateId = ConfigTemplateCurrentRateTable.szConfigTemplateId
            						                AND ConfigInfoCurrentRateTable.szConfigItemTypeId = ConfigTemplateCurrentRateTable.szConfigItemTypeId 
            						                AND ConfigInfoCurrentRateTable.szConfigItemId = ConfigTemplateCurrentRateTable.szConfigItemId
            						                AND ConfigInfoCurrentRateTable.szConfigItemTypeValue = DO_Workplace.szWorkplaceId LEFT JOIN BOS_SM_ConfigTemplateItem ConfigTemplateCurrentLokiFactorTable ON 
                                                        ConfigTemplateCurrentLokiFactorTable.szConfigTemplateId = 'com.kontinum.bos.sd.orDoSoComparison3' 
            				                        AND ConfigTemplateCurrentLokiFactorTable.szConfigItemTypeId = 'WOR'
            						                AND ConfigTemplateCurrentLokiFactorTable.szConfigItemId = '00004'
                                              LEFT JOIN BOS_SM_ConfigInfoItem ConfigInfoCurrentLokiFactorTable ON 
                                                        ConfigInfoCurrentLokiFactorTable.szConfigTemplateId = ConfigTemplateCurrentLokiFactorTable.szConfigTemplateId
            						                AND ConfigInfoCurrentLokiFactorTable.szConfigItemTypeId = ConfigTemplateCurrentLokiFactorTable.szConfigItemTypeId 
            						                AND ConfigInfoCurrentLokiFactorTable.szConfigItemId = ConfigTemplateCurrentLokiFactorTable.szConfigItemId
            						                AND ConfigInfoCurrentLokiFactorTable.szConfigItemTypeValue = DO_Workplace.szWorkplaceId LEFT JOIN BOS_SM_ConfigTemplateItem ConfigTemplateCurrentVolumeUtilizationTable ON 
                                                        ConfigTemplateCurrentVolumeUtilizationTable.szConfigTemplateId = 'com.kontinum.bos.sd.orDoSoComparison3' 
            				                        AND ConfigTemplateCurrentVolumeUtilizationTable.szConfigItemTypeId = 'WOR'
            						                AND ConfigTemplateCurrentVolumeUtilizationTable.szConfigItemId = '00005'
                                              LEFT JOIN BOS_SM_ConfigInfoItem ConfigInfoCurrentVolumeUtilizationTable ON 
                                                        ConfigInfoCurrentVolumeUtilizationTable.szConfigTemplateId = ConfigTemplateCurrentVolumeUtilizationTable.szConfigTemplateId
            						                AND ConfigInfoCurrentVolumeUtilizationTable.szConfigItemTypeId = ConfigTemplateCurrentVolumeUtilizationTable.szConfigItemTypeId 
            						                AND ConfigInfoCurrentVolumeUtilizationTable.szConfigItemId = ConfigTemplateCurrentVolumeUtilizationTable.szConfigItemId
            						                AND ConfigInfoCurrentVolumeUtilizationTable.szConfigItemTypeValue = DO_Workplace.szWorkplaceId LEFT JOIN BOS_SM_ConfigTemplateItem ConfigTemplateCurrentWeightUtilizationTable ON 
                                                        ConfigTemplateCurrentWeightUtilizationTable.szConfigTemplateId = 'com.kontinum.bos.sd.orDoSoComparison3' 
            				                        AND ConfigTemplateCurrentWeightUtilizationTable.szConfigItemTypeId = 'WOR'
            						                AND ConfigTemplateCurrentWeightUtilizationTable.szConfigItemId = '00006'
                                              LEFT JOIN BOS_SM_ConfigInfoItem ConfigInfoCurrentWeightUtilizationTable ON 
                                                        ConfigInfoCurrentWeightUtilizationTable.szConfigTemplateId = ConfigTemplateCurrentWeightUtilizationTable.szConfigTemplateId
            						                AND ConfigInfoCurrentWeightUtilizationTable.szConfigItemTypeId = ConfigTemplateCurrentWeightUtilizationTable.szConfigItemTypeId 
            						                AND ConfigInfoCurrentWeightUtilizationTable.szConfigItemId = ConfigTemplateCurrentWeightUtilizationTable.szConfigItemId
            						                AND ConfigInfoCurrentWeightUtilizationTable.szConfigItemTypeValue = DO_Workplace.szWorkplaceId LEFT JOIN BOS_SM_ConfigTemplateItem ConfigTemplateIncentivePerLokiTable ON 
                                                        ConfigTemplateIncentivePerLokiTable.szConfigTemplateId = 'com.kontinum.bos.sd.orDoSoComparison3' 
            				                        AND ConfigTemplateIncentivePerLokiTable.szConfigItemTypeId = 'WOR'
            						                AND ConfigTemplateIncentivePerLokiTable.szConfigItemId = '00011'
                                              LEFT JOIN BOS_SM_ConfigInfoItem ConfigInfoIncentivePerLokiTable ON 
                                                        ConfigInfoIncentivePerLokiTable.szConfigTemplateId = ConfigTemplateIncentivePerLokiTable.szConfigTemplateId
            						                AND ConfigInfoIncentivePerLokiTable.szConfigItemTypeId = ConfigTemplateIncentivePerLokiTable.szConfigItemTypeId 
            						                AND ConfigInfoIncentivePerLokiTable.szConfigItemId = ConfigTemplateIncentivePerLokiTable.szConfigItemId
            						                AND ConfigInfoIncentivePerLokiTable.szConfigItemTypeValue = DO_Workplace.szWorkplaceId  
            						                WHERE  ((BOS_SD_FInvoiceItem.szProductId <> '' 
            						                AND BOS_SD_FInvoiceItem.szInvItemType = 'DOR') OR BOS_SD_FInvoiceItem.szInvItemType <> 'DOR') 
            						                AND BOS_SD_FInvoice.dtmPeriode >= '11/01/2024' 
            						                AND BOS_SD_FInvoice.dtmPeriode < '12/01/2024' 
            						                AND DO_Workplace.szWorkplaceId LIKE '%DST%' 
            						                AND REVERSAL_DO.szFReversalId is null 
            						                and REVERSAL_REVERSE_DO.szFReversalId is null  
            						                AND BOS_SD_FInvoice.bVoid = 0 
            						                AND BOS_SD_FInvoice.bApplied = 1     
            						                ORDER BY BOS_SD_FInvoice.szWorkplaceId ASC, 
            						                ISNULL(BOS_GL_Workplace.szName,'') ASC, 
            						                ISNULL(BOS_SD_FDo.szFSoId, '') ASC, 
            						                ISNULL(BOS_SD_FDo.szDoId, '') ASC, 
            						                BOS_SD_FInvoice.szFInvoiceId ASC, 
            						                OrderDate ASC, UPPER(BOS_SD_FInvoice.szSalesId) ASC, 
            						                Sales.szName ASC, ISNULL(BOS_SD_FDo.szCustId, BOS_SD_FInvoice.szCustId) ASC, CASE WHEN BOS_SD_FDo.szCustId IS NOT NULL 
                                   THEN ISNULL(CustomerDelivery.szName, '') 
                                   ELSE ISNULL(BOS_AR_Customer.szName, '')
                                   END ASC, ISNULL(BOS_AR_Customer.CustszAddress_1, '') ASC, 
                                   BOS_AR_CustLocation.LocationszCity ASC, 
                                   BOS_SD_FInvoiceItem.szProductId ASC, 
                                   isnull(BOS_INV_Product.szName,'') ASC, 
                                   Price ASC, 
                                   BOS_SD_FInvoiceItem.decQty ASC, 
                                   Amount ASC, 
                                   BOS_SD_FInvoiceItem.decTax + isnull(a.sumDecBonusAmountTax,0) ASC, 
                                   NettAmount ASC, 
                                   NettAmountIncTax ASC, 
                                   Discount ASC, 
                                   DiscountItem ASC
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
if 'OrderDate' in df.columns:
    df['OrderDate'] = df['OrderDate'].astype(str)

# Google BigQuery connection details
credentials_gbq = 'skintific-data-warehouse-ea77119e2e7a.json'
project_id = 'skintific-data-warehouse'
dataset_id = 'bosnet'
table_id = 'st_data_mtd_v2'

# Set up credentials for BigQuery
credentials = service_account.Credentials.from_service_account_file(credentials_gbq)
pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = project_id

# Initialize BigQuery client
client = bigquery.Client(credentials=credentials, project=project_id)

# Check if the table exists and create it if it doesn't
table_ref = f"{project_id}.{dataset_id}.{table_id}"
try:
    client.get_table(table_ref)  # Check if the table exists
    print(f"Table {table_ref} already exists.")
except Exception as e:
    print(f"Table {table_ref} not found. Creating table...")
    # Define schema based on the DataFrame's column types
    schema = [
        bigquery.SchemaField(column_name, bigquery.enums.SqlTypeNames.STRING) if dtype == 'object' else
        bigquery.SchemaField(column_name, bigquery.enums.SqlTypeNames.FLOAT64) if dtype in ['float64', 'float'] else
        bigquery.SchemaField(column_name, bigquery.enums.SqlTypeNames.INTEGER)
        for column_name, dtype in zip(df.columns, df.dtypes)
    ]

    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)
    print(f"Table {table_ref} created successfully.")

# Clear existing data from the target BigQuery table
query_delete = f"DELETE FROM `{table_ref}` WHERE TRUE"
pandas_gbq.read_gbq(query_delete, project_id=project_id, credentials=credentials, location="US")
print(f"Existing data cleared from {dataset_id}.{table_id}.")


# Define a function to upload data in parallel chunks
def upload_to_gbq(df_chunk):
    pandas_gbq.to_gbq(df_chunk, f"{dataset_id}.{table_id}", project_id=project_id, if_exists='append')


# Split the DataFrame into chunks
num_chunks = 8  # Number of parallel chunks (adjust based on your system's capability)
df_chunks = np.array_split(df, num_chunks)

# Upload data to Google BigQuery in parallel
with ThreadPoolExecutor(max_workers=num_chunks) as executor:
    executor.map(upload_to_gbq, df_chunks)

print("Data upload to Google BigQuery is complete.")
