# Project of layoffs

# Creating a working table

SELECT *
FROM layoffs_proyect_1;

CREATE TABLE layoffs_staging
LIKE layoffs_proyect_1;

INSERT layoffs_staging
SELECT *
FROM layoffs_proyect_1;

SELECT *
FROM layoffs_staging;

# Deliting duplicated data
-- In this project doesn't exist a type of data that allows to easily identify duplicated data, so is necessary to create a unique row number

SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

# Deliting extra row to use only the original data

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

# Data standardization 

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_staging2
SET industry = 'crypto'
WHERE industry LIKE 'crypto%';

SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country DESC;

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

# Searching and deleting NULL and Blanks

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Airbnb%';

UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';

SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT t1.industry, t2.industry
FROM layoffs_staging2 AS t1
	JOIN layoffs_staging2 AS t2
		ON t1.company = t2.company
WHERE (t1.industry IS NULL or t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

# Deliting not useful columns

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;

# Data exploring

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT company, SUM(total_laid_off) AS total_lo
FROM layoffs_staging2
GROUP BY company
order by total_lo DESC;

SELECT industry, SUM(total_laid_off) AS total_lo
FROM layoffs_staging2
GROUP BY industry
order by total_lo DESC;

SELECT country, SUM(total_laid_off) AS total_lo
FROM layoffs_staging2
GROUP BY country
ORDER BY total_lo DESC;

SELECT YEAR(`date`), SUM(total_laid_off) AS total_lo
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY total_lo DESC;

SELECT stage, SUM(total_laid_off) AS total_lo
FROM layoffs_staging2
GROUP BY stage
ORDER BY total_lo DESC;

#ORDER BY MONTH

SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY `MONTH` ASC;

#Rolling total

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_LO
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY `MONTH` ASC
)
SELECT `MONTH`, total_LO,
SUM(total_LO) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;

#ORDER BY MONTH

WITH Month_order AS
(
SELECT SUBSTRING(`date`, 1,7) AS `YEAR`, SUM(total_laid_off) AS TLO
FROM layoffs_staging2
WHERE  SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `YEAR`
ORDER BY `YEAR` ASC
)
SELECT SUBSTRING(`YEAR`,6,2) AS `MONTH`, SUM(TLO)
FROM Month_order
GROUP BY `MONTH`
ORDER BY `MONTH` ASC;

# LAID OF BY COMANY AND YEAR

WITH company_years (company, years, total_laid_off) AS 
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
WHERE year(`date`) IS NOT NULL
GROUP BY company, year(`date`)
), company_year_rank AS
(
SELECT *, 
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM company_years
WHERE years IS NOT NULL
)
SELECT *
FROM company_year_rank
WHERE Ranking <= 5;
