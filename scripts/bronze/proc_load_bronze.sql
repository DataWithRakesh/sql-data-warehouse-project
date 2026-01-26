/*
================================================================================
Procedure Name : bronze.load_bronze
Purpose        :
    This stored procedure loads raw data from CSV files into the Bronze layer
    tables of the data warehouse. The Bronze layer acts as the landing zone for
    source system data and stores it in its original, untransformed form.

    The procedure performs the following actions:
      - Truncates existing Bronze tables to ensure a fresh load
      - Loads CRM and ERP source data using BULK INSERT
      - Captures start and end time for each table load
      - Logs per-table load duration for operational visibility
      - Captures total batch execution time
      - Provides structured logging using PRINT statements
      - Handles runtime errors using TRY/CATCH blocks

Source Systems :
    - CRM (cust_info, prd_info, sales_details)
    - ERP (cust_az12, loc_a101, px_cat_g1v2)

Layer          : Bronze
Load Type      : Full Refresh (Truncate & Load)
Execution Mode : Manual / Scheduled (SQL Agent / Orchestration Tool)

================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================';

        PRINT '--------------------------------';
        PRINT 'LOADING CRM Tables';
        PRINT '--------------------------------';

        ----------- CRM : cust_info -----------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/data/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (crm_cust_info): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>>----------------';

        ----------- CRM : prd_info ------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/data/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (crm_prd_info): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>>----------------';

        ----------- CRM : sales_details -------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/data/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (crm_sales_details): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>>----------------';

        PRINT '--------------------------------';
        PRINT 'LOADING ERP Tables';
        PRINT '--------------------------------';

        ----------- ERP : cust_az12 -----------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/data/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (erp_cust_az12): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>>----------------';

        ----------- ERP : loc_a101 ------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/data/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (erp_loc_a101): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>>----------------';

        ----------- ERP : px_cat_g1v2 ---------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/var/opt/mssql/data/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (erp_px_cat_g1v2): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>>----------------';

        SET @batch_end_time = GETDATE();
        PRINT '================================';
        PRINT 'Bronze Layer Load Completed';
        PRINT ' - Total Load Duration: ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================';

    END TRY
    BEGIN CATCH
        PRINT '================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR STATE  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================';
    END CATCH;
END;
