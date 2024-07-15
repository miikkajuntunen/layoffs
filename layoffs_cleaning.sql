-- Exploring what columns and values are in the dataset
SELECT * FROM  layoffs;

-- Creating a staging table to do data cleaning
CREATE TABLE layoffs_staging LIKE layoffs;

-- Making sure everything's ok
SELECT * FROM layoffs_staging;

-- Creating a CTE of duplicate values
WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off,
'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs)

-- Checking the values from CTE
SELECT * FROM duplicate_cte;

-- Adding a row_num column to the table, in order to delete duplicates later
ALTER TABLE layoffs_staging
ADD COLUMN `row_num` INT;

-- Inserting all the values from the original table, including the added row_nums
INSERT INTO layoffs_staging
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off,
'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs;

-- Exploring the data, making sure everything's ok
SELECT * FROM layoffs_staging;

-- Checking for duplicate values
SELECT * FROM layoffs_staging
WHERE row_num > 1;

-- Removing duplicate values from the table
DELETE FROM layoffs_staging
WHERE row_num > 1;

-- Making sure duplicates are deleted
SELECT * FROM layoffs_staging
WHERE row_num > 1;

-- Standardizing the data
-- Trimming evident whitespaces
SELECT company, TRIM(company)
FROM layoffs_staging;

UPDATE layoffs_staging
SET company = TRIM(company);

-- Standardize values that have the same meaning
SELECT DISTINCT industry
FROM layoffs_staging;

SELECT *
FROM layoffs_staging
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Checking and updating locations
SELECT DISTINCT location
FROM layoffs_staging
ORDER BY 1;

-- Replace 'DÃ¼sseldorf' with Düsseldorf
UPDATE layoffs_staging
SET location = 'Düsseldorf'
WHERE location LIKE 'D%sseldorf';

SELECT DISTINCT country
FROM layoffs_staging
ORDER BY 1; 

-- Removing the trailing dots from country values
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging
ORDER BY 1;

UPDATE layoffs_staging
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Turning date column into DATE datatype
SELECT date,
STR_TO_DATE(date, '%m/%d/%Y')
FROM layoffs_staging; 

UPDATE layoffs_staging
SET date = STR_TO_DATE(date, '%m/%d/%Y');

ALTER TABLE layoffs_staging
MODIFY COLUMN date DATE;

SELECT * FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Changing the blanks to nulls
UPDATE layoffs_staging
SET industry = NULL
WHERE industry = '';

SELECT * FROM layoffs_staging
WHERE industry IS NULL
OR industry = '';

SELECT * FROM layoffs_staging
WHERE company LIKE 'Bally%';

-- Updating the industries that are null or blank
SELECT * FROM layoffs_staging l1
JOIN layoffs_staging l2 ON l1.company = l2.company
AND l1.location = l2.location
WHERE (l1.industry IS NULL)
AND l2.industry IS NOT NULL;

UPDATE layoffs_staging t1
JOIN layoffs_staging t2 ON t1.company = t2.company
AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

-- Deleting data that have no value in the EDA
SELECT * FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Drop any unnecessary columns
ALTER TABLE layoffs_staging
DROP COLUMN row_num;

-- Summary report of cleaning efforts
SELECT 'Total rows cleaned' AS metric,
COUNT(*) AS value 
 FROM layoffs_staging;