SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO
-- drop PROCEDURE [dbo].[getBranchStats]
CREATE PROCEDURE [dbo].[getBranchStats]
WITH EXEC AS CALLER
AS
SET NOCOUNT ON;

DECLARE @position int = 5;
DECLARE @CurrentFYStarts DATE = DATEADD(year, DATEDIFF(month, '19010701', GETDATE()) / 12, '19010701');
DECLARE @LastFYStarts DATE = DATEADD(year, -1, @CurrentFYStarts);

BEGIN
   
   BEGIN TRY
   
   IF OBJECT_ID('tempdb..#LastFY') IS NOT NULL 
          BEGIN
             DROP TABLE #LastFY;
          END;
          
   SET @Position = 10;
   
   SELECT ii.OfficeCode branchcode
        , ol.OfficeName branchname
        , COUNT(DISTINCT jo.client_id) clientsLastYear
        , COUNT(DISTINCT jo.Candidate_id) employeesPaidLastYear
        , SUM(ii.Quantity) hoursSoldLastYear
        , SUM(ii.ItemSubTotal) revenueLastYear
   INTO #LastFY
   FROM Live_RSF.dbo.ftv_InvoiceItem ii JOIN
        Live_RSF.dbo.ftv_InvoiceHeader ih ON ii.Invoice_id = ih.Invoice_id 
                                          AND ii.ItemDate BETWEEN @LastFYStarts AND DATEADD(day, -1, @CurrentFYStarts)
                                          AND ih.Status_no = 'closed'
                                          AND ih.InvoiceFormatType_no like 'Timesheet Based %' JOIN
        Live_RSF.dbo.ftv_BillCodeList bcl ON ii.BillCode_id = bcl.BillCode_id 
                                          AND bcl.BillCodeType_no <> 'Allowance' JOIN
        Live_RSF.dbo.ftv_JobOrder jo ON ii.JobOrder_id = jo.JobOrder_id JOIN
        Live_RSF.dbo.ftv_OfficeList ol ON ii.OfficeCode = ol.Office_id
   GROUP BY ii.OfficeCode
          , ol.OfficeName;
          
   IF OBJECT_ID('tempdb..#CurrentFY') IS NOT NULL 
          BEGIN
             DROP TABLE #CurrentFY;
          END;
          
   SET @Position = 20;
   
   SELECT ii.OfficeCode branchcode
        , ol.OfficeName branchname
        , COUNT(DISTINCT jo.client_id) clients
        , COUNT(DISTINCT jo.Candidate_id) employeesPaid
        , SUM(ii.Quantity) hoursSold
        , SUM(ii.ItemSubTotal) revenue
   INTO #CurrentFY
   FROM Live_RSF.dbo.ftv_InvoiceItem ii JOIN
        Live_RSF.dbo.ftv_InvoiceHeader ih ON ii.Invoice_id = ih.Invoice_id 
                                          AND ii.ItemDate >= @CurrentFYStarts
                                          AND ih.Status_no = 'closed'
                                          AND ih.InvoiceFormatType_no like 'Timesheet Based %' JOIN
        Live_RSF.dbo.ftv_BillCodeList bcl ON ii.BillCode_id = bcl.BillCode_id 
                                          AND bcl.BillCodeType_no <> 'Allowance' JOIN
        Live_RSF.dbo.ftv_JobOrder jo ON ii.JobOrder_id = jo.JobOrder_id JOIN
        Live_RSF.dbo.ftv_OfficeList ol ON ii.OfficeCode = ol.Office_id
   GROUP BY ii.OfficeCode
          , ol.OfficeName;
   
   IF OBJECT_ID('tempdb..#Branch') IS NOT NULL 
          BEGIN
             DROP TABLE #Branch;
          END;
    
    SET @Position = 30;
    
    SELECT *
    INTO #Branch
    FROM (SELECT ly.branchcode
               , ly.branchName
          FROM #LastFY ly
          UNION
          SELECT cy.branchcode
               , cy.branchName
          FROM #CurrentFY cy) tb;
           
    SET @Position = 40;
    
    SELECT DISTINCT b.*
                  , cy.clients
                  , ly.clientsLastYear
                  , cy.employeesPaid
                  , ly.employeesPaidLastYear
                  , cy.hoursSold
                  , ly.HoursSoldLastYear
                  , cy.Revenue
                  , ly.RevenueLastYear
    FROM #Branch b LEFT JOIN
         #LastFY ly ON b.branchcode = ly.branchcode LEFT JOIN
         #CurrentFY cy ON b.branchcode = cy.branchcode
    ORDER BY b.branchcode;
                 
   END TRY
      BEGIN CATCH
            
         IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
                  
            DECLARE @errseverity int;
            DECLARE @errstate int;
            DECLARE @errmsg varchar(MAX);

            SELECT @errseverity = ERROR_SEVERITY()
                 , @errstate = CASE WHEN ERROR_STATE() = 0 THEN 1 ELSE ERROR_STATE() END
                 , @errmsg = 'Position ' + CAST(@Position AS VARCHAR(4)) + ', error no. ' + CAST(ERROR_NUMBER() AS VARCHAR(10)) + ', on line ' + CAST(ERROR_LINE() AS VARCHAR(10)) + ': ' + ERROR_MESSAGE();
            
            RAISERROR (@errmsg, -- Message text.  
                       @errseverity, -- Severity.  
                       @errstate -- State.  
                       );  
              
      END CATCH;
END
GO