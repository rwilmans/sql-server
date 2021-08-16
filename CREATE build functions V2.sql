 /* ********************************************************************************************************************************************************************************** */ 
-- =======================================================================================
-- Table: raoul.operation - ERA-partitioned
-- DROP TABLE IF EXISTS raoul.operation;
CREATE SEQUENCE raoul.seq_operation__operation_id AS INTEGER START WITH 1 INCREMENT BY 1 CACHE;

CREATE TABLE raoul.operation (
	operation_id INTEGER NOT NULL DEFAULT NEXT VALUE FOR raoul.seq_operation__operation_id,
	meta_valid_from DATETIME2(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
	meta_insert_ts DATETIME2(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
	
	left_expression_type CHAR(1) NOT NULL,
	CONSTRAINT chk_operation__l_e_t CHECK (left_expression_type IN ('O', 'C', 'N', 'A', 'F')), -- O: Operation (-id), C: Column, N: Numeric, A: Character (Alphanumeric), F: Function
	
	left_expression VARCHAR(100) NULL,
	CONSTRAINT chk_operation__l_e CHECK (left_expression IS NOT NULL AND left_expression_type IN ('C', 'N', 'A', 'F') OR left_expression_type = 'O' AND left_expression IS NULL),

	parameter_id INTEGER NULL,
	CONSTRAINT chk_operation__parameter CHECK (left_expression_type = 'F' AND (left_expression_operation_id IS NOT NULL OR parameter_id IS NOT NULL) OR left_expression_type != 'F' AND parameter_id IS NULL),

	filter_condition_id INTEGER NULL,
	
	left_expression_operation_id INTEGER NULL,
	--CONSTRAINT chk_operation__l_e_o_id CHECK (left_expression_type = 'O' AND left_expression_operation_id IS NOT NULL OR left_expression_type != 'O' AND left_expression_operation_id IS NULL),
	CONSTRAINT chk_operation__l_e_o_id CHECK (left_expression_type IN ('O','F') AND (left_expression_operation_id IS NOT NULL OR parameter_id IS NOT NULL) OR  left_expression_type NOT IN ('O','F') AND left_expression_operation_id IS NULL),
	
	operator VARCHAR(100) NULL,
	
	right_expression_operation_id INTEGER NULL,
			
	CONSTRAINT pk_operation PRIMARY KEY CLUSTERED (operation_id),
	--CONSTRAINT pk_operation PRIMARY KEY CLUSTERED (operation_id, meta_valid_from DESC), -- Only where ERA-partitioned and __hist
	CONSTRAINT fk_operation__operation__left FOREIGN KEY (left_expression_operation_id) REFERENCES raoul.operation (operation_id), 
	CONSTRAINT fk_operation__operation__right FOREIGN KEY (right_expression_operation_id) REFERENCES raoul.operation (operation_id)
)
;

-- ---------------------------------------------------------------------------------------
-- Table: raoul.parameter - ERA-partitioned
-- DROP TABLE IF EXISTS raoul.parameter;
CREATE SEQUENCE raoul.seq_parameter__parameter_id AS INTEGER START WITH 1 INCREMENT BY 1 CACHE;

CREATE TABLE raoul.parameter (
	parameter_id INTEGER NOT NULL DEFAULT NEXT VALUE FOR raoul.seq_parameter__parameter_id,
	meta_valid_from DATETIME2(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
	meta_insert_ts DATETIME2(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,	
	meta_is_active BIT NOT NULL DEFAULT 'TRUE',
	parameter_sequence_number SMALLINT NOT NULL DEFAULT 1,
	operation_id INTEGER NOT NULL,
			
	CONSTRAINT pk_parameter PRIMARY KEY CLUSTERED (parameter_id, parameter_sequence_number),
	--CONSTRAINT pk_parameter PRIMARY KEY CLUSTERED (parameter_id, parameter_sequence_number, meta_valid_from DESC), -- Only where ERA-partitioned and __hist
	CONSTRAINT fk_parameter__operation FOREIGN KEY (operation_id) REFERENCES raoul.operation (operation_id)
)
;

--ALTER TABLE raoul.operation ADD CONSTRAINT fk_operation__parameter FOREIGN KEY (parameter_id) REFERENCES raoul.parameter (parameter_id);


-- =======================================================================================
-- Table: raoul.condition_expression - ERA-partitioned
-- DROP TABLE IF EXISTS raoul.condition_expression;
CREATE SEQUENCE raoul.seq_condition_expression__condition_expression_id AS INTEGER START WITH 1 INCREMENT BY 1 CACHE;

CREATE TABLE raoul.condition_expression (
	condition_expression_id INTEGER NOT NULL DEFAULT NEXT VALUE FOR raoul.seq_condition_expression__condition_expression_id,
	meta_valid_from DATETIME2(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
	meta_insert_ts DATETIME2(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
	
	left_expression_negated BIT NOT NULL DEFAULT 'FALSE',
	left_expression VARCHAR(100) NULL,
	
	left_expression_operation_id INTEGER NULL,
	CONSTRAINT chk_condition_expression__l_e_o_id CHECK (left_expression IS NULL AND left_expression_operation_id IS NOT NULL OR left_expression IS NOT NULL AND left_expression_operation_id IS NULL),
	
	comparator VARCHAR(8) NULL, -- EQ, NE, GT, GE, LT, LE, LIKE, IS NULL, IS TRUE, IS FALSE, <NULL>
	CONSTRAINT chk_condition_expression__comparator CHECK (comparator IN  ('EQ', 'NE', 'GT', 'GE', 'LT', 'LE', 'LIKE', 'IS NULL', 'IS TRUE', 'IS FALSE') OR comparator IS NULL),
	
	right_expression_operation_id INTEGER NULL,
	CONSTRAINT chk_condition_expression__r_e_o_id CHECK (right_expression_operation_id IS NOT NULL AND comparator IN ('EQ', 'NE', 'GT', 'GE', 'LT', 'LE', 'LIKE') OR right_expression_operation_id IS NULL),
	
	CONSTRAINT pk_condition_expression PRIMARY KEY CLUSTERED (condition_expression_id DESC), 
	-- CONSTRAINT pk_condition_expression PRIMARY KEY CLUSTERED (condition_expression_id, meta_valid_from DESC), -- Only if ERA-partitioned AND __hist
	CONSTRAINT fk_condition_expression__operation__left FOREIGN KEY (left_expression_operation_id) REFERENCES raoul.operation (operation_id),
	CONSTRAINT fk_condition_expression__operation__right FOREIGN KEY (right_expression_operation_id) REFERENCES raoul.operation (operation_id)
)
;


-- =======================================================================================
-- Table: raoul.condition - ERA-partitioned
-- DROP TABLE IF EXISTS raoul.condition;
CREATE SEQUENCE raoul.seq_condition__condition_id AS INTEGER START WITH 1 INCREMENT BY 1 CACHE;

CREATE TABLE raoul.condition (
	condition_id INTEGER NOT NULL DEFAULT NEXT VALUE FOR raoul.seq_condition__condition_id,
	meta_valid_from DATETIME2(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
	meta_insert_ts DATETIME2(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
	
	left_operand_expression_id INTEGER NULL,
	left_operand_condition_id INTEGER NULL,
	CONSTRAINT chk_condition__left_id CHECK (left_operand_expression_id IS NOT NULL AND left_operand_condition_id IS NULL OR left_operand_expression_id IS NULL AND left_operand_condition_id IS NOT NULL),

	boolean_operator VARCHAR(3) NULL, --  COMMENT 'OR, AND, <NULL>',
	CONSTRAINT chk_condition__bool_op CHECK (boolean_operator IN ('OR', 'AND') OR boolean_operator IS NULL),

	right_operand_expression_id INTEGER NULL,
	right_operand_condition_id INTEGER NULL,
	CONSTRAINT chk_condition__right_id CHECK (
		(right_operand_expression_id IS NOT NULL AND right_operand_condition_id IS NULL OR right_operand_expression_id IS NULL AND right_operand_condition_id IS NOT NULL) AND boolean_operator IS NOT NULL OR
		(right_operand_expression_id IS NULL AND right_operand_condition_id IS NULL) AND boolean_operator IS NULL
	),


	CONSTRAINT pk_condition PRIMARY KEY CLUSTERED (condition_id),
	-- CONSTRAINT pk_condition PRIMARY KEY (condition_id, meta_valid_from DESC), -- Only if ERA-partitioned AND __hist

	CONSTRAINT fk_condition__condition_expression__left FOREIGN KEY (left_operand_expression_id) REFERENCES raoul.condition_expression (condition_expression_id),
	CONSTRAINT fk_condition__condition_expression__right FOREIGN KEY (right_operand_expression_id) REFERENCES raoul.condition_expression (condition_expression_id), 
	CONSTRAINT fk_condition__condition__left FOREIGN KEY (left_operand_condition_id) REFERENCES raoul.condition (condition_id),
	CONSTRAINT fk_condition__condition__right FOREIGN KEY (right_operand_condition_id) REFERENCES raoul.condition (condition_id)
)
;

-- =======================================================================================
-- Table: raoul.rule_set - ERA-partitioned
-- DROP TABLE IF EXISTS raoul.rule_set;

CREATE SEQUENCE raoul.seq_rule_set__rule_set_id AS INTEGER START WITH 1 INCREMENT BY 1 CACHE;

CREATE TABLE raoul.rule_set (
	rule_set_id INTEGER NOT NULL DEFAULT NEXT VALUE FOR raoul.seq_rule_set__rule_set_id,
	meta_valid_from DATETIME2(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
	meta_insert_ts DATETIME2(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
	meta_is_active BIT NOT NULL DEFAULT 'TRUE',

	in_schema_name VARCHAR(128) NOT NULL,
	in_table_name VARCHAR(128) NOT NULL,
	out_schema_name VARCHAR(128) NOT NULL,
	out_table_name VARCHAR(128) NOT NULL,

	description VARCHAR(250),

	CONSTRAINT pk_rule_set_description PRIMARY KEY CLUSTERED (rule_set_id),
	-- CONSTRAINT pk_rule_set_description PRIMARY KEY CLUSTERED (rule_set_id, meta_valid_from DESC),

	CONSTRAINT unq_rule_set__in_out UNIQUE (in_schema_name, in_table_name, out_schema_name, out_table_name)
)
;

-- =======================================================================================
-- Table: raoul.rule - ERA-partitioned
-- DROP TABLE IF EXISTS raoul.[rule];

CREATE SEQUENCE raoul.seq_rule__rule_id AS INTEGER START WITH 1 INCREMENT BY 1 CACHE;

CREATE TABLE raoul.[rule] (
	rule_id INTEGER NOT NULL DEFAULT NEXT VALUE FOR raoul.seq_rule__rule_id,
	meta_valid_from DATETIME2(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
	meta_insert_ts DATETIME2(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
	meta_is_active BIT NOT NULL DEFAULT 'TRUE',

	rule_set_id INTEGER NOT NULL,
	operation_id INTEGER NOT NULL,
	condition_id INTEGER NULL,
	out_column VARCHAR(128) NOT NULL,

	CONSTRAINT pk_rule PRIMARY KEY CLUSTERED (rule_id),
	-- CONSTRAINT pk_rule PRIMARY KEY CLUSTERED (rule_id, meta_valid_from DESC),

	CONSTRAINT unq_rule__out_table_id_out_column UNIQUE (rule_set_id, out_column),
	-- CONSTRAINT unq_rule__out_table_id_out_column UNIQUE (rule_set_id, out_column, meta_valid_from DESC),

	CONSTRAINT fk_rule__rule_set FOREIGN KEY (rule_set_id) REFERENCES raoul.rule_set (rule_set_id),
	CONSTRAINT fk_rule__condition__condition_id FOREIGN KEY (condition_id) REFERENCES raoul.condition (condition_id),
	CONSTRAINT fk_rule__operation FOREIGN KEY (operation_id) REFERENCES raoul.operation (operation_id)
)
;

 /* ********************************************************************************************************************************************************************************** */ 
 /* ********************************************************************************************************************************************************************************** */ 
 /* ********************************************************************************************************************************************************************************** */ 

CREATE OR ALTER FUNCTION raoul.build_parameter_list (
	@parameter_id INT
) RETURNS VARCHAR(MAX) AS
BEGIN
	DECLARE @parmlist VARCHAR(MAX) ='';

	SELECT	@parmlist = @parmlist +	CASE 
										WHEN @parmlist = '' THEN raoul.build_expression(operation_id)
										ELSE ', ' + raoul.build_expression(operation_id)
									END
	FROM raoul.parameter
	WHERE	parameter_id = @parameter_id AND
			meta_is_active = 'TRUE'
	;

	RETURN (@parmlist);
END
;
GO
;

SELECT raoul.build_parameter_list(2);


-- DROP FUNCTION IF EXISTS raoul.build_expression;
-- GO
-- ;
GO
;
CREATE OR ALTER FUNCTION raoul.build_expression (
	@operation_id INT
) RETURNS VARCHAR(MAX) AS
BEGIN
	RETURN (
		-- Start building the expression recursively
		SELECT	-- Start with the basics...
				CASE
					WHEN left_expression_type = 'N' THEN left_expression
					WHEN left_expression_type = 'A' THEN '''' + left_expression + ''''
					WHEN left_expression_type = 'C' THEN '[' + left_expression + ']'  -- Maybe skip the square brackets altogether...
					WHEN left_expression_type = 'O' THEN raoul.build_expression(left_expression_operation_id)
					WHEN left_expression_type = 'F' THEN	left_expression + 
															--'('+ 
															--CASE 
															--	WHEN left_expression_operation_id IS NOT NULL THEN raoul.build_expression(left_expression_operation_id)
															--	ELSE  raoul.build_parameter_list(parameter_id)  
															--END + 
															--')' 
															-- OR: 
															CASE 
																WHEN filter_condition_id IS NOT NULL THEN 
																	'(CASE WHEN (' + raoul.build_expression(filter_condition_id) + ') THEN ' + '('+ 
																	CASE 
																		WHEN left_expression_operation_id IS NOT NULL THEN raoul.build_expression(left_expression_operation_id)
																		ELSE  raoul.build_parameter_list(parameter_id)  
																	END 
																	+ 
																	') END)' 
																ELSE	'(' + 
																		CASE 
																			WHEN left_expression_operation_id IS NOT NULL THEN raoul.build_expression(left_expression_operation_id)
																			ELSE  raoul.build_parameter_list(parameter_id)  
																		END  + 
																		')'
															END
					ELSE ''
				END 
				+	
				-- Adding the right-hand-side operation, if relevant
				CASE 
--					WHEN (operator != '') THEN ' ' + operator + ' (' + raoul.build_expression(right_expression_operation_id) + ')'
					WHEN (operator != '') THEN ' ' + operator + ' ' + raoul.build_expression(right_expression_operation_id)
					ELSE ''
				END
		FROM raoul.operation
		WHERE operation_id = @operation_id
	);
END
;
GO
;

SELECT raoul.build_expression(4);
SELECT raoul.build_expression(5);
SELECT raoul.build_expression(6);

 -- ########################################################

-- DROP FUNCTION IF EXISTS raoul.build_condition_expression;
-- GO
-- ;
GO
;
CREATE OR ALTER FUNCTION raoul.build_condition_expression (
	@condition_expression_id INT
) RETURNS VARCHAR(MAX) AS
BEGIN
	RETURN (
		-- Start building the expression recursively
		SELECT	'(' +
				CASE	
					-- Only TWO flavours: either a COLUMN or an OPERATION
					WHEN left_expression IS NOT NULL THEN '[' + left_expression  + ']'
					ELSE '(' + raoul.build_expression(left_expression_operation_id) + ')'
				END + 

				-- Comparator
				CASE 
					WHEN left_expression_negated = 0 THEN 
						-- the normal comparator
						CASE comparator
							WHEN 'EQ' THEN ' = '
							WHEN 'NE' THEN ' != '
							WHEN 'GE' THEN ' >= '
							WHEN 'GT' THEN ' > '
							WHEN 'LE' THEN ' <= '
							WHEN 'LT' THEN ' < '
							WHEN 'LIKE' THEN 'LIKE'
							WHEN 'IS NULL' THEN ' IS NULL '
							WHEN 'IS FALSE' THEN ' = ''FALSE'' '
							WHEN 'IS TRUE' THEN ' = ''TRUE'' '
							ELSE ''
						END
					ELSE
						-- the NEGATED version
						CASE comparator
							WHEN 'EQ' THEN ' != '
							WHEN 'NE' THEN ' = '
							WHEN 'GE' THEN ' < '
							WHEN 'GT' THEN ' <= '
							WHEN 'LE' THEN ' > '
							WHEN 'LT' THEN ' >= '
							WHEN 'LIKE' THEN ' NOT LIKE '
							WHEN 'IS NULL' THEN ' IS NOT NULL '
							WHEN 'IS FALSE' THEN ' = ''TRUE'' '
							WHEN 'IS TRUE' THEN ' = ''FALSE'' '
							ELSE ''
						END
				END +
				CASE 
					WHEN right_expression_operation_id IS NOT NULL THEN '(' + raoul.build_expression(right_expression_operation_id) + ')'
					ELSE ''
				END +
				')'

		FROM raoul.condition_expression
		WHERE condition_expression_id = @condition_expression_id
	);
END
;
GO
;

SELECT raoul.build_condition_expression(1);

 -- ########################################################

-- DROP FUNCTION IF EXISTS raoul.build_condition;
-- GO
-- ;
GO;

CREATE OR ALTER FUNCTION raoul.build_condition (
	@condition_id INT
) RETURNS VARCHAR(MAX) AS
BEGIN
	RETURN (
		-- Start building the expression recursively
		SELECT	--left hand part
				CASE	
					WHEN left_operand_expression_id IS NOT NULL THEN raoul.build_condition_expression(left_operand_expression_id)
					WHEN left_operand_condition_id IS NOT NULL THEN raoul.build_condition(left_operand_condition_id)
					ELSE ''
				END
				+
				CASE 
					-- OR / AND ??
					WHEN boolean_operator IN ('OR', 'AND') THEN
						' ' + boolean_operator + ' ' + 
						-- right hand part
						CASE 
							WHEN right_operand_condition_id IS NOT NULL THEN raoul.build_condition(right_operand_condition_id)
							WHEN right_operand_expression_id IS NOT NULL THEN raoul.build_condition_expression(right_operand_expression_id)
							ELSE ''
						END
					ELSE ''
				END

		FROM raoul.condition
		WHERE condition_id = @condition_id
	);
END
;
GO;

SELECT raoul.build_condition(1);

 -- ########################################################
 -- ########################################################


GO
;

CREATE OR ALTER PROCEDURE raoul.autogenerate_br ( 
	@rule_set_id INTEGER = 0 
)
AS
SET NOCOUNT ON;
BEGIN 
	/*
	PROCEDURE autogenerate_br

	AUTHOR:
	-------
	R. Wilmans-Sangwienwong (2021)

	PURPOSE:
	--------
	Fill table from a source-table, using business rules, defined in external metadata tables.

	PARAMETERS:
	-----------
	rule_set_id : The set of business rules, defined for this combination of output- and input table.
	Datatype: INTEGER
	Default: -

	RETURNS:
	--------
	- (but fills output table, based on the business rules)

	REMARKS:
	--------
	Requires business rules tables to be defined and filled properly.
	Does NOT (yet?) implement window functions (OVER PARTITION BY)
	
	REQUIRES:
	---------
	FUNCTION (recursive) build_condition
	FUNCTION (recursive) build_condition_expression
	FUNCTION (recursive) build_expression
	FUNCTION build_parameter_list

	Example:
	-- ---------------------------------------------------------------
	 --  IN
	CREATE TABLE raoul.br_test_in (
		br_test_in_id INTEGER NOT NULL,
		tekst VARCHAR(10),
		getal INTEGER
	);

	INSERT INTO raoul.br_test_in (br_test_in_id , tekst, getal)
	VALUES	(1, '1_AAA', 12),
			(2, '2_BBB', 30),
			(3, '3_CCC', 48),
			(4, '4_DAD', NULL),
			(5, '5_EEA', 10)
	;

	 -- OUT
	CREATE TABLE raoul.br_test_out (
		out_id INTEGER,
		lower_third_char VARCHAR(10),
		ten_times INTEGER
	);

	----Try and replicate this: 
	--SELECT	br_test_in_id AS out_id,
	--		LOWER(SUBSTRING(tekst,CHARINDEX('_',tekst)+1,1)) AS lower_third_char,
	--		CASE 
	--			WHEN br_test_in_id >= 3 THEN 10*br_test_in_id 
	--		END AS ten_times
	--FROM raoul.br_test_in
	--;

	INSERT INTO raoul.operation (operation_id, left_expression_type, left_expression, left_expression_operation_id, parameter_id, operator, right_expression_operation_id)
	VALUES	(1, 'A', '_',				NULL,	NULL,	NULL,	NULL),
			(2, 'C', 'tekst',			NULL,	NULL,	NULL,	NULL),
			(3, 'N', '1' ,				NULL,	NULL,	NULL,	NULL),
			(4, 'F', 'CHARINDEX',		NULL,	1,		'+',	3),
			(5, 'F', 'SUBSTRING',		NULL,	2,		NULL,	NULL),
			(6, 'F', 'LOWER',			5,		NULL,	NULL,	NULL),
			(7, 'C', 'br_test_in_id',	NULL,	NULL,	NULL,	NULL),
			(8, 'N', '10',				NULL,	NULL,	'*',	7),
			(9, 'N', '3',				NULL,	NULL,	NULL,	NULL)
	;

	INSERT INTO raoul.parameter (parameter_id, parameter_sequence_number, operation_id)
	VALUES	(1, 1, 1),
			(1, 2, 2),
			(2, 1, 2),
			(2, 2, 4),
			(2, 3, 3)
	;


	INSERT INTO raoul.condition_expression (condition_expression_id, left_expression_negated, left_expression, left_expression_operation_id, comparator, right_expression_operation_id)
	VALUES	(1, 'FALSE', NULL, 7, 'GE', 9)
	;

	INSERT INTO raoul.condition (condition_id, left_operand_expression_id, left_operand_condition_id, boolean_operator, right_operand_expression_id, right_operand_condition_id)
	VALUES	(1, 1, NULL, NULL, NULL, NULL)
	;

	INSERT INTO raoul.rule_set (rule_set_id, in_schema_name, in_table_name, out_schema_name, out_table_name, description)
	VALUES (1, 'raoul', 'br_test_in', 'raoul', 'br_test_out', 'ruleset for demonstration purposes')
	;

	INSERT INTO raoul.[rule] (rule_set_id, out_column, operation_id, condition_id)
	VALUES	(1, 'out_id',			7,	NULL),
			(1, 'lower_third_char', 6,	NULL),
			(1, 'ten_times',		8,	1)
	;
	EXEC raoul.autogenerate_br @rule_set_id= 1;
	
	-- ---------------------------------------------------------------
	*/
	-- ------------------------------------------------------
	DECLARE @sql_build NVARCHAR(MAX) = '';
	DECLARE @vars_out VARCHAR(MAX) = '';
	DECLARE @vars_list VARCHAR(MAX) = '';
	DECLARE @current_var VARCHAR(128) = '';
	DECLARE @out_oid INTEGER;
	DECLARE @in_oid INTEGER;

	-- ------------------------------------------------------
	-- Get matched variables from output table vs. defined by rules
	SELECT	@vars_out = @vars_out + CASE WHEN @vars_out = '' THEN '' ELSE ',' END + rl.out_column,
			@vars_list = @vars_list + CASE WHEN @vars_list = '' THEN '' ELSE ',' END + rl.out_column,
			@out_oid = out_table_oid,
			@in_oid = in_oid
	FROM (	-- Current situation from RULE_SET
			SELECT	meta_valid_from,
					OBJECT_ID(out_schema_name + '.' + out_table_name) AS out_table_oid,
					OBJECT_ID(in_schema_name + '.' + in_table_name) AS in_oid,
					LEAD(meta_valid_from,1,'9999/01/01') OVER (ORDER BY meta_valid_from) AS meta_valid_to
			FROM raoul.rule_set 
			WHERE	rule_set_id = @rule_set_id AND
					meta_is_active = 'TRUE' 
			) AS rs
	
	-- Only columns actually defined in output table
	INNER JOIN 
			sys.columns AS col
	ON col.object_id = rs.out_table_oid

	INNER JOIN 
		(	-- Current situation from RULE
			SELECT	out_column,
					meta_valid_from,
					LEAD(meta_valid_from,1,'9999/01/01') OVER (PARTITION BY out_column ORDER BY meta_valid_from) AS meta_valid_to
			FROM raoul.[rule] 
			WHERE	rule_set_id = @rule_set_id AND
					meta_is_active = 'TRUE'
			) AS rl
	ON	UPPER(col.name) = UPPER(rl.out_column)
	
	WHERE	-- Current state of affairs on RULE_SET
			rs.meta_valid_from <= GETDATE() AND 
			rs.meta_valid_to > GETDATE() 
			AND
			-- Current state of affairs on RULE
			rl.meta_valid_from <= GETDATE() AND 
			rl.meta_valid_to > GETDATE()
	;
	
	-- ------------------------------------------------------
	-- Unfortunately, we cannot iterate over the result of a query, nor over an array...
	WHILE @vars_out != '' BEGIN
		-- Determine current column
		SET @current_var =	CASE		
								WHEN CHARINDEX(',',@vars_out) > 0 THEN SUBSTRING(@vars_out,1,CHARINDEX(',',@vars_out) - 1)
								ELSE @vars_out
							END;

		-- Here the actualy building of the statements
		--PRINT '@current_var: ' + @current_var;

		SELECT	@sql_build =	CASE
									WHEN @sql_build = '' THEN ''
									ELSE @sql_build + ', ' + CHAR(13)
								END 
								+ 
								CASE 
									WHEN condition_id IS NOT NULL THEN 'CASE WHEN ' + raoul.build_condition(condition_id) + ' THEN (' + raoul.build_expression(operation_id) + ') END'
									ELSE raoul.build_expression(operation_id)
								END
								+
								' AS ' 
								+ 
								out_column
		FROM raoul.[rule]
		WHERE	out_column = @current_var AND
				rule_set_id = @rule_set_id
		;

		--PRINT( 'Intermediate @sql_build: ' + @sql_build);

		 -- Next column, if relevant
		SET @vars_out = CASE 
							WHEN CHARINDEX(',',@vars_out) > 0 THEN RIGHT(@vars_out,LEN(@vars_out) - CHARINDEX(',',@vars_out))
							ELSE '' 
						END;
	END;


	-- ------------------------------------------------------
	SELECT @sql_build = 'INSERT INTO ' + OBJECT_SCHEMA_NAME(@out_oid) + '.' + OBJECT_NAME(@out_oid) + ' (' + @vars_list + ') ' + CHAR(13) +
						'SELECT ' + CHAR(13) +
						@sql_build + CHAR(13) +
						' FROM '+ OBJECT_SCHEMA_NAME(@in_oid) + '.' + OBJECT_NAME(@in_oid) + CHAR(13) +
						';'
						;

	PRINT ('SQL: ' + @sql_build);

	EXEC SP_EXECUTESQL @sql_build;
    /*-------------------------------------------------------------------------------------*/
END 
;

GO
;
