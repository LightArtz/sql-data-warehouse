/*
============================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
============================================
Script Purpose:
This stored procedure loads data into 'bronze' layer from 6 CSV files.
It performs the following actions:
- Truncates the bronze tables before loading data
- Uses the 'BULK INSERT' command to load data from CSV files to bronze tables.

Parameters:
None. This stored procedure does not accept any parameters or return any values.

Usage example:
EXEC bronze.load_bronze;
============================================
*/

-- This file is to insert 6 CSV files to each table. Since normally we do this every day, we can use stored procedure.
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time_whole DATETIME, @end_time_whole DATETIME
	DECLARE @start_time DATETIME, @end_time DATETIME
	BEGIN TRY -- Use try & catch for error handling. If it fails running the TRY block, it will run the CATCH block.
		SET @start_time_whole = GETDATE()
		PRINT '============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '============================================';

		PRINT '--------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------';

		-- Insert bronze.crm_cust_info
		SET @start_time = GETDATE() -- Track ETL duration: helps to identify bottlenecks, optimize performance, monitor trends, detect issues
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info; -- (if accidentally inserted a few times)

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Projects\08. Data Engineer\01. Data Warehouse SQL\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK --TABLOCK is used for locking the whole table when inserting the data to the table -> performance
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------';

		-- Insert bronze.crm_prd_info
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Projects\08. Data Engineer\01. Data Warehouse SQL\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------';

		-- Insert bronze.crm_sales_details
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Projects\08. Data Engineer\01. Data Warehouse SQL\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------';

		PRINT '--------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------------------';

		-- Insert bronze.erp_cust_az12
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Projects\08. Data Engineer\01. Data Warehouse SQL\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------';

		-- Insert bronze.erp_loc_a101
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Projects\08. Data Engineer\01. Data Warehouse SQL\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------';

		-- Insert bronze.erp_px_cat_g1v2
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Projects\08. Data Engineer\01. Data Warehouse SQL\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------';

		SET @end_time_whole = GETDATE()
		PRINT '=================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT 'Total Load Duration (Whole Batch): ' + CAST(DATEDIFF(second, @start_time_whole, @end_time_whole) AS NVARCHAR) + ' seconds';
		PRINT '=================================';
	END TRY
	BEGIN CATCH
		PRINT '=================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=================================';
	END CATCH
END
