-- DATA CLEANIMG--


select*
from layoffs;

-- REMOVE DUPLICATES
-- STANDARDIZE DATA
-- NULL OR BLANK VALUES
-- REMOVE UNNECESSARY COLUMNS

CREATE TABLE layoffs_stagging
like layoffs;

select*
from layoffs_stagging; 

insert layoffs_stagging
select*
from layoffs;

select*,
row_number() over(
partition by company,industry,total_laid_off,percentage_laid_off,`date`) as row_num
from layoffs_stagging;


with duplicate_cte as
(
select*,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_stagging
)
select*
from duplicate_cte
where row_num >1
;

select*
from layoffs_stagging
where company= 'casper'
;

with duplicate_cte as
(
select*,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_stagging
)
delete
from duplicate_cte
where row_num >1
;


CREATE TABLE `layoffs_stagging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select*
from layoffs_stagging2;

insert into layoffs_stagging2
select*,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_stagging;

select*
from layoffs_stagging2
where row_num >1
;

delete
from layoffs_stagging2
where row_num >1
;

select*
from layoffs_stagging2
where row_num >1
;

-- Standardizing

select company,trim(company)
from layoffs_stagging2;

update layoffs_stagging2
set company = trim(company)
;

select*
from layoffs_stagging2
where industry like 'crypto%'
;

update layoffs_stagging2
set industry = 'crypto'
where industry like 'crypto%';

select industry
from layoffs_stagging2;

select `date`
from layoffs_stagging2;

update layoffs_stagging2
set `date` =  str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_stagging2
modify column`date` DATE;

select*
from layoffs_stagging2
where total_laid_off is null
and percentage_laid_off is null
;

select*
from layoffs_stagging2
where industry is null 
or industry = ''
;

select*
from layoffs_stagging2
where company like 'Bally%';



select st1.industry,st2.industry
from layoffs_stagging2 as st1
join layoffs_stagging2 as st2
	on st1.company = st2.company
    and st1.location = st2.location
where (st1.industry is null or st1.industry = '') 
and st2.industry is not null;

update layoffs_stagging2
set industry = null
 where industry = '';

update layoffs_stagging2 as st1
join layoffs_stagging2 as st2
	on st1.company = st2.company
 set st1.industry = st2.industry 
  where st1.industry is null 
  and st2.industry is not null;
  
  delete
from layoffs_stagging2
where total_laid_off is null
and percentage_laid_off is null;

select*
from layoffs_stagging2;

alter table layoffs_stagging2
drop column row_num
;


