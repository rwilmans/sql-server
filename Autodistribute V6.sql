CREATE OR ALTER FUNCTION raoul.for_x_in (
	@array_of_strings VARCHAR(MAX),
	@operation VARCHAR(200),
	@concat VARCHAR(10) = ','
) RETURNS VARCHAR(MAX) AS
BEGIN
	/*
	FUNCTION for_x_in

	AUTHOR:
	-------
	R. Wilmans-Sangwienwong (2021)

	PURPOSE:
	--------
	To mimic the common 'for x in ' construct with ARRAYs. 
	In MS SQL Server no arrays are available, so here mimic this using a comma-separated string of elements.

	PARAMETERS:
	-----------
	array_of_strings : a string of comma-separated elements
	Datatype: VARCHAR(MAX)
	Default: -
	
	operation : The string containing the operation where '$' is replaced by the element-at-hand (element from the 'array')
	Datatype: VARCHAR(200)
	Default: -
	
	concat : the separator between the modified elements. 
	Datatype: VARCHAR(10)
	Default: ','

	RETURNS:
	--------
	A concatenated string of modified elements from a string-of-elements.

	REMARKS:
	--------
	In the operation, use '$' (without quotes) as a placeholder for the element

	Example:
	-- ---------------------------------------------------------------
	for_x_in('date_column_1,datetimestamp_2','ins.$ < cur.$','AND')
	SELECT for_x_in('eerste,tweede','in1.$ = out1.$',DEFAULT);
	SELECT for_x_in('eerste,tweede','in1.$ = out1.$','AND');
	-- ---------------------------------------------------------------
	*/
	DECLARE @result VARCHAR(MAX) = '';
	SELECT @result = @result + CASE WHEN @result = '' THEN '' ELSE ' ' + @concat + ' ' END + REPLACE(@operation,'$',value)
	FROM STRING_SPLIT(@array_of_strings,',')
	;
	RETURN @result;
END
;
GO
;

-- Function to mimic the distribution of records on temporal tables 
CREATE OR ALTER PROCEDURE raoul.stp_autodistribute (
	@trigger_id INTEGER = 0,
	@insert_table_name VARCHAR(128) = '#inserted',
	@debug CHAR(1) = 'N' -- Y/N
) AS
BEGIN

	/*
	PROCEDURE stp_autodistribute

	AUTHOR:
	-------
	R. Wilmans-Sangwienwong (2021)

	PURPOSE:
	--------
	Procedure to facilitate era-partitioned tables. Past, present (and future not implemented for now).
	To insert records and keep historical records, without copying and updating or inserting records actively - this is all done 
	behind the scenes.
	In MS SQL Server, table inheritance like in PostgreSQL is not implemented. So here, instead of a base table, make use of a VIEW
	('<table>') with a ON INSERT DO INSTEAD. The current situation is stored in <table>__curr, the historical records in <table>__hist.
	If needed, the current state can be obtained from <table>__curr - this will be faster than going through the entire set including 
	historical records.

	PARAMETERS:
	-----------
	trigger_id : the object-id (OID) of the calling trigger. This is used to determine which table (+__curr/__hist) is being inserted into. REQUIRED
	Datatype: INTEGER
	Default: 0 (=no object)
	
	insert_table_name : The name of the TEMP table that is a 1:1 copy of the 'magic' INSERTED table. This is necessary, as Dynamic SQL is being used,
						and the magic table is not reacheable by Dynamic SQL (because of context).
	Datatype: VARCHAR(128)
	Default: '#inserted'
	
	debug : If extra (debug!) information is needed (like the actual queries that are being performed), then set debug to 'Y'.
	Datatype: CHAR(1)
	Default: 'N'

	RETURNS:
	--------
	Nothing, but updates the <table>__curr and <table>__hist (if necessary), which is reflected in the <table>-VIEW.

	REMARKS:
	--------
	<table>__futr NOT YET implemented.
	Function raoul.for_x_in REQUIRED!

	Example:
	-- ---------------------------------------------------------------
	DROP VIEW IF EXISTS raoul.double_pk;
	DROP TABLE IF EXISTS raoul.double_pk__hist;
	DROP TABLE IF EXISTS raoul.double_pk__curr;
	DROP SEQUENCE IF EXISTS raoul.seq_double_pk__double_pk_1_id;
	DROP SEQUENCE IF EXISTS raoul.seq_double_pk__double_pk_2_id;

	-- Sequence [meta].[seq_rule__rule_id]
	CREATE SEQUENCE raoul.seq_double_pk__double_pk_1_id AS int START WITH 1 INCREMENT BY 1 CACHE;
	CREATE SEQUENCE raoul.seq_double_pk__double_pk_2_id AS int START WITH 1 INCREMENT BY 1 CACHE;

	-- The table representing the current (or most recent) situation 
	CREATE TABLE raoul.double_pk__curr (
		double_pk_1_id INT NOT NULL DEFAULT NEXT VALUE FOR raoul.seq_double_pk__double_pk_1_id,
		double_pk_2_id INT NOT NULL DEFAULT NEXT VALUE FOR raoul.seq_double_pk__double_pk_2_id,
		meta_valid_from DATE NOT NULL DEFAULT GETDATE(),
		meta_insert_ts DATETIME2(3) NOT NULL DEFAULT GETDATE(),
		meta_is_valid BIT NOT NULL DEFAULT 'TRUE',
		meta_is_historical AS CAST(0 AS BIT),
		rest_of_vars VARCHAR(20) NOT NULL DEFAULT '---',
		CONSTRAINT pk_double_pk_curr PRIMARY KEY CLUSTERED (double_pk_1_id, double_pk_2_id)
	);

	-- The table with historical data (or past states)
	CREATE TABLE raoul.double_pk__hist (
		double_pk_1_id INT NOT NULL,
		double_pk_2_id INT NOT NULL,
		meta_valid_from DATE NOT NULL DEFAULT GETDATE(),
		meta_insert_ts DATETIME2(3) NOT NULL DEFAULT GETDATE(),
		meta_is_valid BIT NOT NULL DEFAULT 'TRUE',
		meta_is_historical AS CAST(1 AS BIT),
		rest_of_vars VARCHAR(20) NOT NULL DEFAULT '---',
		CONSTRAINT pk_double_pk_hist PRIMARY KEY CLUSTERED (meta_valid_from, double_pk_1_id, double_pk_2_id),
		CONSTRAINT fk_double_pk_hist__double_pk_curr FOREIGN KEY (double_pk_1_id, double_pk_2_id) REFERENCES raoul.double_pk__curr (double_pk_1_id, double_pk_2_id) ON UPDATE CASCADE ON DELETE CASCADE--  NOT FOR REPLICATION
	);

	GO
	;

	-- View combining both the current and historical states
	CREATE OR ALTER VIEW raoul.double_pk AS
	SELECT	double_pk_1_id,
			double_pk_2_id,
			meta_valid_from,
			meta_insert_ts,
			meta_is_valid,
			meta_is_historical,
			rest_of_vars
	FROM raoul.double_pk__curr
	UNION
	SELECT	double_pk_1_id,
			double_pk_2_id,
			meta_valid_from,
			meta_insert_ts,
			meta_is_valid,
			meta_is_historical,
			rest_of_vars
	FROM raoul.double_pk__hist
	;
	
	-- The actual INSTEAD - trigger on the VIEW
	CREATE OR ALTER TRIGGER raoul.trg_double_pk__ins 
		ON raoul.double_pk 
		INSTEAD OF INSERT 
	AS
	BEGIN
		SET NOCOUNT ON;
		-- ------------------------------------
		-- Get everything from 'magic' table into TEMP table, so we can access it in the procedure and Dynamic SQL...
		SELECT	* 
		INTO #inserted_double_pk
		FROM INSERTED;
		;

		EXECUTE raoul.stp_autodistribute @trigger_id = @@PROCID, @insert_table_name = '#inserted_double_pk';
	END
	;
	GO
	;

	-- --------------------------------
	-- First record - new ID assigned
	INSERT INTO raoul.double_pk (meta_valid_from, rest_of_vars)
	VALUES ('20200101', 'AA')
	;
	-- --------------------------------
	-- Additional records - new ID
	INSERT INTO raoul.double_pk (meta_valid_from, rest_of_vars)
	VALUES ('20200101', 'ZZ')
	;
	-- --------------------------------
	-- New current state; copy to hist + update
	INSERT INTO raoul.double_pk (double_pk_1_id, double_pk_2_id, meta_valid_from, rest_of_vars)
	VALUES (1,1,'20200222', 'BB')
	;
	-- --------------------------------
	-- New current state; copy to hist + update
	INSERT INTO raoul.double_pk (double_pk_1_id, double_pk_2_id, meta_valid_from, rest_of_vars)
	VALUES (1,1,'20200313', 'CC')
	;
	-- --------------------------------
	-- Update historical record
	INSERT INTO raoul.double_pk (double_pk_1_id, double_pk_2_id, meta_valid_from, rest_of_vars)
	VALUES (1,1,'20200101', 'DD')
	;
	-- --------------------------------
	-- Update current record
	INSERT INTO raoul.double_pk (double_pk_1_id, double_pk_2_id, meta_valid_from, rest_of_vars)
	VALUES (1,1,'20200313', 'QQ')
	;
	-- --------------------------------
	-- List from-to for certain record
	SELECT * 
	FROM raoul.double_pk
	;

	SELECT	double_pk_1_id, double_pk_2_id,
			meta_valid_from,
			LEAD(meta_valid_from,1,'9999/01/01') OVER (PARTITION BY double_pk_1_id, double_pk_2_id ORDER BY meta_valid_from) AS meta_valid_to,
			d.*
	FROM raoul.double_pk AS d
	WHERE double_pk_1_id =1 AND double_pk_2_id = 1
	;
	-- --------------------------------
	-- next-value to-be-generated by sequence 1 higher than MAX(id)?
	INSERT INTO raoul.double_pk (double_pk_1_id, double_pk_2_id, meta_valid_from, rest_of_vars)
	VALUES (5,1,'20200313', 'YY')
	;
	SELECT  CAST(CURRENT_VALUE AS INT) AS next_value
	FROM SYS.sequences 
	WHERE name = 'seq_double_pk__double_pk_1_id'
	;
	-- --------------------------------
	
	-- --------------------------------------------------------------
	*/
	
	SET NOCOUNT ON;

	DECLARE @pk_column VARCHAR(1200) = '';
	DECLARE @pk_column_type VARCHAR(1200) = '';
	DECLARE @pk_column_default VARCHAR(1200) = '';
	DECLARE @seq_id_arr VARCHAR(1400) = '';;

	DECLARE @sec_pk_col VARCHAR(128) = '';
	DECLARE @sec_pk_col_type VARCHAR(128) = '';
	DECLARE @sec_pk_col_default VARCHAR(200) = '';

	DECLARE @meta_ts VARCHAR(128) = '';
	DECLARE @meta_ts_type VARCHAR(128) = '';
	DECLARE @meta_ts_default VARCHAR(200) = '';

	-- 8K should be enough... (so as not to use 'MAX') - this can be changed, of course...
	DECLARE @non_pk_cols VARCHAR(8000) = '';
	DECLARE @non_pk_cols_defaults VARCHAR(8000) = '';

	DECLARE @table_curr_id INT;
	DECLARE @table_hist_id INT;
	DECLARE @table_curr VARCHAR(257); -- 128 + '.' + 128
	DECLARE @table_hist VARCHAR(257);

	DECLARE @sql NVARCHAR(MAX) = '';

	-- -----------------------------------------------------------------------------
	-- Retrieve the basics....
	SELECT	@pk_column = @pk_column + IIF(@pk_column = '','',',') + LTRIM(col.name),
			@pk_column_default = @pk_column_default + IIF(@pk_column_default = '','',',') + col.name + ' = COALESCE(' + col.name + ',' + e.dflt + ')' ,
			@pk_column_type = @pk_column_type + IIF(@pk_column_type = '','',',') + col.name + ' ' + e.datatype,
			@seq_id_arr = @seq_id_arr + IIF(@seq_id_arr = '','',',') + LTRIM(OBJECT_ID(SUBSTRING(e.dflt, e2.pat_pos, LEN(e.dflt) - CHARINDEX(']',REVERSE(e.dflt)) - e2.pat_pos+2))) + '#' + LTRIM(col.name),
			@table_curr_id = t.table_curr_id, 
			@table_hist_id = t.table_hist_id,
			@table_curr = t.table_curr,
			@table_hist = t.table_hist
	FROM (	SELECT	OBJECT_ID(e.table_curr) AS table_curr_id,
					OBJECT_ID(e.table_hist) AS table_hist_id,
					e.table_curr,
					e.table_hist
			FROM SYS.triggers
			CROSS APPLY (SELECT OBJECT_SCHEMA_NAME(parent_id) + '.' + OBJECT_NAME(parent_id) + '__curr' AS table_curr,
								OBJECT_SCHEMA_NAME(parent_id) + '.' + OBJECT_NAME(parent_id) + '__hist' AS table_hist
						) AS e
			WHERE object_id = @trigger_id
			) AS t
	INNER JOIN	
			SYS.tables AS b
	ON	b.object_id = t.table_curr_id

	INNER JOIN 
			SYS.key_constraints AS kc
	ON	kc.parent_object_id = b.object_id AND 
		kc.type = 'PK'
	
	INNER JOIN 
			SYS.index_columns AS ic 
	ON	kc.parent_object_id = ic.object_id AND
		kc.unique_index_id = ic.index_id
	INNER JOIN 
			SYS.columns AS col 
	ON	ic.object_id = col.object_id AND 
		ic.column_id = col.column_id
	CROSS APPLY (	SELECT	CASE 
								WHEN UPPER(tp.name) LIKE '%CHAR%' THEN UPPER(tp.name) + '('+ LTRIM(col.max_length) + ')' 
								WHEN UPPER(tp.name) LIKE '%TIME%' AND col.scale != 0 THEN UPPER(tp.name) + '('+ LTRIM(col.scale) + ')' 
								ELSE UPPER(tp.name)
							END AS datatype,
							OBJECT_DEFINITION(col.default_object_id) AS dflt
					FROM SYS.types AS tp
					WHERE col.system_type_id = tp.system_type_id
					) AS e			
	CROSS APPLY (	SELECT PATINDEX('%[[]%].[[]%]%', e.dflt) AS pat_pos
					) AS e2
	;

	-- -----------------------------------------------------------------------------
	-- Secondary PK column
	-- So, the column that is part of the PK in __hist, but NOT part of PK in __curr
	-- Therefore: 'meta_valid_from' (or whatever that's called)
 	SELECT	@sec_pk_col = col.name,
			@sec_pk_col_default = col.name + ' = COALESCE(' + col.name + ', ' +  COALESCE(OBJECT_DEFINITION(tid.default_object_id),'CAST(GETDATE() AS VARCHAR)') + ')',
			@sec_pk_col_type = col.name + ' ' + e.datatype
	FROM	SYS.key_constraints AS kc
	INNER JOIN 
			SYS.index_columns AS ic 
	ON	kc.parent_object_id = ic.object_id AND
		kc.unique_index_id = ic.index_id
	INNER JOIN 
			SYS.columns AS col 
	ON	ic.object_id = col.object_id AND 
		ic.column_id = col.column_id
	INNER JOIN 
			SYS.columns AS tid -- From the __curr-table; to make sure that this column is available in BOTH tables!
	ON	tid.object_id =  @table_curr_id AND
		tid.name = col.name AND 
		--col.name NOT IN (@pk_column) -- This doesn't work in SQL Server.... *-SIGH-*
		',' + @pk_column + ',' NOT LIKE '%,' + col.name + ',%' -- ... but NOT a defined as PK in __curr!
	CROSS APPLY (	SELECT	CASE 
								WHEN UPPER(tp.name) LIKE '%CHAR%' THEN UPPER(tp.name) + '('+ LTRIM(tid.max_length) + ')' 
								WHEN UPPER(tp.name) LIKE '%TIME%' AND col.scale != 0 THEN UPPER(tp.name) + '('+ LTRIM(tid.scale) + ')' 
								ELSE UPPER(tp.name) -- Probably just a regular INT (or BIGINT)
							END AS datatype
					FROM SYS.types AS tp
					WHERE tid.system_type_id = tp.system_type_id
					) AS e			
	WHERE	kc.parent_object_id = @table_hist_id AND 
			kc.type = 'PK' -- Primary Key
	;


	-- -----------------------------------------------------------------------------
	-- Meta info: timestamp for insertion (/update)
	SELECT	@meta_ts = col.name,
			@meta_ts_default =	col.name + ' = COALESCE(' + col.name + ', ' +  COALESCE(OBJECT_DEFINITION(col.default_object_id),'CAST(GETDATE() AS VARCHAR)') + ')',
			@meta_ts_type = col.name + ' ' + e.datatype
	FROM	SYS.columns as col
	INNER JOIN 
			SYS.types AS tp
	ON	col.system_type_id = tp.system_type_id AND
		UPPER(tp.name) LIKE 'DATETIME%'
	CROSS APPLY (	SELECT	CASE 
								WHEN UPPER(tp.name) LIKE '%CHAR%' THEN UPPER(tp.name) + '('+ LTRIM(col.max_length) + ')' 
								WHEN UPPER(tp.name) LIKE '%TIME%' AND col.scale != 0 THEN UPPER(tp.name) + '('+ LTRIM(col.scale) + ')' 
								ELSE UPPER(tp.name)
							END AS datatype
					FROM SYS.types AS tp
					WHERE col.system_type_id = tp.system_type_id
					) AS e	
	WHERE	col.object_id = @table_curr_id AND
			',' + @pk_column + ',' + @sec_pk_col + ',' NOT LIKE '%,' + col.name + ',%' AND --So, NONE of the PK-columns or Secondary PK-column
			UPPER(col.name) LIKE 'META_%'
	;

	-- -----------------------------------------------------------------------------
	-- Rest of columns
	SELECT	@non_pk_cols = @non_pk_cols + IIF(@non_pk_cols = '','',',') + name,
			@non_pk_cols_defaults = @non_pk_cols_defaults + IIF(@non_pk_cols_defaults = '','',',') + name + ' = COALESCE(' + name + ',' + OBJECT_DEFINITION(default_object_id) + ')' 
	FROM SYS.columns
	WHERE	object_id = @table_curr_id AND
			',' + @pk_column + ',' + @sec_pk_col + ',' + @meta_ts + ',' NOT LIKE '%,' + name + ',%' AND --So, NONE of so-far found special columns
			is_computed = 0
	ORDER BY column_id
	; 

	/* ************************************************************************************************ */
	/* Check if necessary columns are given (aside from key columns, for which we will insert defaults) */
	/* ************************************************************************************************ */
	-- Update required columns if needed 
	-- However, not the PK, as this will be the automagical NEXT VALUE FOR sequence...
	SET @sql = N'	UPDATE ins
					SET	' + @sec_pk_col_default + ', 
						' + @meta_ts_default + ',
						' + @non_pk_cols_defaults + '
					FROM ' + @insert_table_name + ' AS ins 
					';
	IF (@debug = 'Y') BEGIN
		PRINT @sql;
	END;
	EXECUTE(@sql);

	-- -----------------------------------------------------------------------------
	-- Already exists and new record is newer (or as new, but with different values) than 'current'
	-- So, copy current state to __hist
	-- Create TEMP table to store affected IDs
	-- Yes, yes, SQL injection... however, the variables are generated from... SYS, so , SQL injections will be VERY DIFFICULT!
	-- (by entering code for table / column names - which will be prevented by the DB)
	-- NEW RECORD, safe history
	SET @sql = N'   CREATE TABLE #insert_ids (' +@pk_column_type + ');
					INSERT INTO ' + @table_hist + ' (' + @pk_column + ', ' + @sec_pk_col +  ', ' + @meta_ts +  ', ' + @non_pk_cols + ')
					OUTPUT ' + raoul.for_x_in (@pk_column, 'INSERTED.$',DEFAULT) + '
					INTO #insert_ids (' + @pk_column + ')
					SELECT	' +
							raoul.for_x_in (@pk_column, 'incur.$',DEFAULT) + ', '+ 
							raoul.for_x_in (@sec_pk_col, 'incur.$',DEFAULT) + ', '+ 
							raoul.for_x_in (@meta_ts, 'incur.$',DEFAULT) + ', '+ 
							raoul.for_x_in (@non_pk_cols, 'incur.$',DEFAULT) + '
					FROM	' + @insert_table_name + ' AS ins 
					INNER JOIN
							' + @table_curr + ' AS incur
					ON	' + 
						-- ..._id  - so, id MUST already exist!
						raoul.for_x_in(@pk_column,'ins.$ = incur.$','AND') +  
						' AND ' +  
						raoul.for_x_in(@sec_pk_col,'ins.$ > incur.$','AND') +  ' -- meta_valid_from AFTER current state, so NEW current state!
					WHERE	BINARY_CHECKSUM(' + 
											raoul.for_x_in(@non_pk_cols,'ins.$',DEFAULT) + 
											') -- all other columns
							!=  
							BINARY_CHECKSUM(' + 
											raoul.for_x_in(@non_pk_cols,'incur.$',DEFAULT) + 
											')
					;

					/*  ... and then update current state to new state */ 
					UPDATE cur
					SET ' + 
						raoul.for_x_in(@sec_pk_col,'$ = new.$',DEFAULT) + ', ' +  
						@meta_ts + ' = GETDATE(), ' +
						raoul.for_x_in(@non_pk_cols,'$ = new.$',DEFAULT) + '
					
					FROM	' + @table_curr + ' AS cur
					INNER JOIN
							#insert_ids AS ins 
					ON ' + raoul.for_x_in(@pk_column,'cur.$ = ins.$','AND') + '
					INNER JOIN 
							' + @insert_table_name + ' AS new
					ON ' + raoul.for_x_in(@pk_column,'ins.$ = new.$','AND') + '
					;
					/* Drop TEMP table #insert_ids */
					DROP TABLE IF EXISTS #insert_ids;
					';

	IF (@debug = 'Y') BEGIN
		PRINT @sql;
	END;
	EXECUTE (@sql);


	-- Updated record, ID already known AND meta_valid_from is THE SAME as in ...__curr
	-- So, update ...__curr
	-- SQL injection: yah-dee-yah...
	-- UPDATE CURRENT RECORD
	SET @sql = N'	UPDATE incur
					SET ' + @meta_ts + ' = GETDATE(), ' + 
						raoul.for_x_in(@non_pk_cols,'$ = ins.$',DEFAULT) + '
					FROM	' + @table_curr + ' AS incur
					INNER JOIN
							' + @insert_table_name + ' AS ins 							
					ON	' + 
						-- ..._id  - so, id MUST already exist!
						raoul.for_x_in(@pk_column,'ins.$ = incur.$','AND') +  
						' AND ' + 
						raoul.for_x_in(@sec_pk_col,'ins.$ = incur.$','AND') + '   --  meta_valid_from EQUAL TO current state
					WHERE	-- all other columns
							BINARY_CHECKSUM(' + raoul.for_x_in(@non_pk_cols,'ins.$',DEFAULT) + ') 
							!=  
							BINARY_CHECKSUM(' + raoul.for_x_in(@non_pk_cols,'incur.$',DEFAULT) + ')
					;
					';
	IF (@debug = 'Y') BEGIN
		PRINT @sql;
	END;
	EXECUTE (@sql);


	-- Historical record, ID already known AND historically also already known (so: update historical table)
	-- Update historical state to newly-delivered historical state
	-- SQL injection: yah-dee-yah...
	-- UPDATE HISTORY
	SET @sql = N'	UPDATE inhist
					SET ' + @meta_ts + ' = GETDATE(), ' + 
						raoul.for_x_in(@non_pk_cols,'$ = ins.$',DEFAULT) + '
					FROM	' + @insert_table_name + ' AS ins 
					INNER JOIN
							' + @table_curr + ' AS incur
					ON	' + 
						-- ..._id  - so, id MUST already exist!
						raoul.for_x_in(@pk_column,'ins.$ = incur.$','AND') + 
						' AND '+ 
						--  meta_valid_from BEFORE current state
						raoul.for_x_in(@sec_pk_col,'ins.$ < incur.$','AND')+ '   -- meta_valid_from BEFORE current state, so historical
					INNER JOIN
							' + @table_hist + ' AS inhist
					ON ' + 
						raoul.for_x_in(@pk_column,'ins.$ = inhist.$','AND') + ' AND '+ 
						-- meta_valid_from exactly as in __hist-table!
						raoul.for_x_in(@sec_pk_col,'ins.$ = inhist.$','AND') + ' -- meta_valid_from EQUAL to historical state
					WHERE	BINARY_CHECKSUM(' + raoul.for_x_in(@non_pk_cols,'ins.$',DEFAULT) + ') 
							!=  
							BINARY_CHECKSUM(' + raoul.for_x_in(@non_pk_cols,'inhist.$',DEFAULT) + ')
					;
					';
	IF (@debug = 'Y') BEGIN
		PRINT @sql;
	END;
	EXECUTE (@sql);
	
	-- Historical record, ID already known, but historically not already known (so: meta_valid_from is in the past, but not already in database --> insert)
	-- unknown HISTORICAL 
	SET @sql = N'   INSERT INTO ' + @table_hist + ' (' + @pk_column + ', ' + @sec_pk_col + ', ' + @meta_ts +  ', ' + @non_pk_cols + ')
					SELECT  ' + 
							raoul.for_x_in(@pk_column,'ins.$',DEFAULT) + ', ' +  
							raoul.for_x_in(@sec_pk_col,'ins.$',DEFAULT) + ', ' +  
							raoul.for_x_in(@meta_ts,'ins.$',DEFAULT) + ', ' +  
							raoul.for_x_in(@non_pk_cols,'ins.$',DEFAULT) + '
					FROM	' + @insert_table_name + ' AS ins 
					INNER JOIN
							' + @table_curr + ' AS cur
					ON	' + 
						raoul.for_x_in(@pk_column,'ins.$ = cur.$','AND') + ' AND ' + --  ..._id
						raoul.for_x_in(@sec_pk_col,'ins.$ < cur.$','AND') + ' AND ' +
						raoul.for_x_in(@sec_pk_col,'ins.$ IS NOT NULL','AND') + ' 
					WHERE	' + 
							raoul.for_x_in(@pk_column,'ins.$ IS NOT NULL','AND') + ' AND
							NOT EXISTS (SELECT 1 
										FROM  ' + @table_hist + ' AS inhist
										WHERE	' + 
												-- xxx_id
												raoul.for_x_in(@pk_column,'ins.$ = inhist.$','AND') + 
												' AND ' + 
												-- meta_valid_from
												raoul.for_x_in(@sec_pk_col,'ins.$ = inhist.$','AND') + ' 
										)
					;';
	IF (@debug = 'Y') BEGIN
		PRINT @sql;
	END;
	EXECUTE (@sql);

	-- Completely new record, ID NOT NULL and not already in ...__curr  --> insert!
	-- Do we want to be able to enter a non-pre-existing ID?
	SET @sql = N'   INSERT INTO ' + @table_curr + ' (' + @pk_column + ', ' + @sec_pk_col +  ', ' + @meta_ts +  ', ' + @non_pk_cols + ')
					SELECT ' +
							@pk_column + ', ' +  
							@sec_pk_col  + ', ' +  
							@meta_ts + ', '	+ 
							@non_pk_cols + '
					FROM	' + @insert_table_name + ' AS ins 
					WHERE	' + 
							raoul.for_x_in(@pk_column,'$ IS NOT NULL','AND') + ' AND
							NOT EXISTS (SELECT 1
										FROM ' + @table_curr + ' AS incur
										WHERE ' + raoul.for_x_in(@pk_column,'ins.$ = incur.$','AND') + '
										)
					;

					DECLARE @next_val INT = 1;
					DECLARE @curr_val INT = 1;

					DECLARE @seq_id INT;
					DECLARE @pk_col VARCHAR(128);
					DECLARE @crsr CURSOR;

					SET @crsr = CURSOR FOR	SELECT	CONVERT(INT,SUBSTRING(VALUE,1,CHARINDEX(''#'',VALUE)-1)) AS id,
													SUBSTRING(VALUE,CHARINDEX(''#'',VALUE)+1, LEN(VALUE) - CHARINDEX(''#'',VALUE)) AS pk_column
											FROM STRING_SPLIT(''' + @seq_id_arr +  ''','','')
											
					OPEN @crsr;
					FETCH NEXT FROM @crsr INTO @seq_id, @pk_col;
					WHILE @@FETCH_STATUS = 0
					BEGIN
						DECLARE @inception NVARCHAR(500) = '''';

						SET @inception = N''SELECT @next_v = COALESCE(MAX('' + @pk_col + ''),0) + 1
											FROM ' + @table_curr + '
											;'';
						EXECUTE sp_executesql @inception , N''@pk_col VARCHAR(128), @next_v INT OUTPUT'', @pk_col = @pk_col, @next_v = @next_val OUTPUT;
						
						SELECT  @curr_val  = CAST(CURRENT_VALUE AS INT) 
						FROM SYS.sequences 
						WHERE object_id = @seq_id
						;

						IF (CONVERT(INT,@next_val) > CONVERT(INT,@curr_val)) BEGIN
							IF (@debug = ''Y'') BEGIN
								PRINT(''MAX(id) found ('' + OBJECT_NAME(@seq_id) + '') to be greater than NEXT VALUE() ---> adjusted, now OK!'');
							END;						
							SET @inception = N''ALTER SEQUENCE '' + OBJECT_SCHEMA_NAME(@seq_id) + ''.'' + OBJECT_NAME(@seq_id) + '' RESTART WITH '' + LTRIM(@next_val);
							EXECUTE(@inception);
						END
						ELSE BEGIN
							IF (@debug = ''Y'') BEGIN
								PRINT(''Sequence '' + OBJECT_NAME(@seq_id) + '' is still adequate'');
							END;
						END;
						;
	
						FETCH NEXT FROM @crsr INTO @seq_id, @pk_col;
					END;

					CLOSE @crsr;
					DEALLOCATE @crsr;		
					';

	IF (@debug = 'Y') BEGIN
		PRINT @sql;
	END;
	EXECUTE sp_executesql @sql , N'@debug CHAR(1)', @debug = @debug;

	-- Completely new record, ID is NULL --> insert!
	SET @sql = N'   INSERT INTO ' + @table_curr + ' (' + @sec_pk_col +  ', ' + @meta_ts +  ', ' + @non_pk_cols + ')
					SELECT	' + 
							raoul.for_x_in(@sec_pk_col,'ins.$',DEFAULT) + ', ' + 
							raoul.for_x_in(@meta_ts,'ins.$',DEFAULT) + ', '	+ 
							raoul.for_x_in(@non_pk_cols,'ins.$',DEFAULT) + '
					FROM	' + @insert_table_name + ' AS ins
					WHERE	' + raoul.for_x_in(@pk_column,'$ IS NULL','AND') + ' 
					;';
	IF (@debug = 'Y') BEGIN
		PRINT @sql;
	END;
	EXECUTE (@sql);

END
;
GO
;