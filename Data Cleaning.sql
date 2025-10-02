-- Projet SQL - Nettoyage des données

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022






SELECT * 
FROM world_layoffs.layoffs;



-- La première chose à faire est de créer une table de staging. C’est celle sur laquelle nous allons travailler et nettoyer les données. Nous voulons garder une table avec les données brutes au cas où quelque chose arriverait
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;


-- 1. vérifier les doublons et en supprimer
-- 2. standardiser les données et corriger les erreurs
-- 3. examiner les valeurs nulles et voir quoi en faire
-- 4. supprimer les colonnes et lignes non nécessaires



-- 1. Supprimer les doublons

# Commençons par vérifier les doublons



SELECT *
FROM world_layoffs.layoffs_staging
;

SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		world_layoffs.layoffs_staging;



SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
-- Regardons simplement Oda pour confirmer
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda'
;
-- Il semble que ce soient toutes des entrées légitimes et qu’elles ne doivent pas être supprimées. Nous devons vraiment examiner chaque ligne pour être précis

-- Voici nos vrais doublons 
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- Ce sont ceux que nous voulons supprimer lorsque le numéro de ligne est > 1 ou essentiellement ≥ 2

-- Vous pouvez maintenant l’écrire comme ceci :
WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
;


WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

-- Une solution, que je trouve bonne, est de créer une nouvelle colonne et d’y ajouter ces numéros de ligne.
-- Ensuite, supprimer les lignes dont les numéros de ligne sont supérieurs à 2, puis supprimer cette colonne

ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;


SELECT *
FROM world_layoffs.layoffs_staging
;

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

-- Maintenant que nous avons cela, nous pouvons supprimer les lignes où row_num est supérieur ou égal à 2

DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;







-- 2. Standardiser les données

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- Si nous regardons la colonne industry, nous avons des valeurs NULL et des lignes vides ; examinons-les
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Jetons un coup d’œil à celles-ci
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';
-- Rien d’anormal ici
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

-- Il semble que Airbnb soit dans le voyage (travel), mais cette ligne n’est simplement pas renseignée.
-- écrire une requête qui, s’il existe une autre ligne avec le même nom d’entreprise, mettra à jour la valeur industry NULL avec la valeur non NULL
-- Cela facilite les choses : s’il y en avait des milliers, nous n’aurions pas à tout vérifier manuellement

-- Nous devrions remplacer les vides par des NULL car ils sont généralement plus faciles à manipuler
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Maintenant, si nous vérifions, elles sont toutes à NULL

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Nous devons maintenant renseigner ces NULL si possible

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Et si nous vérifions, il semble que Bally's était la seule sans ligne renseignée pour permettre de remplir cette valeur NULL
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- ---------------------------------------------------

-- J’ai également remarqué que Crypto a plusieurs variations. Nous devons standardiser cela — choisissons toutes “Crypto”
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Maintenant que c’est réglé :
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- --------------------------------------------------
-- Nous devons aussi regarder 

SELECT *
FROM world_layoffs.layoffs_staging2;

-- Tout semble correct sauf que nous avons certains “United States” et certains “United States.” avec un point final. Standardisons cela.
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Si nous relançons, c’est corrigé
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;


-- Corrigeons aussi les colonnes de dates :
SELECT *
FROM world_layoffs.layoffs_staging2;

-- Nous pouvons utiliser STR_TO_DATE pour mettre à jour ce champ
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Nous pouvons maintenant convertir correctement le type de données
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM world_layoffs.layoffs_staging2;





-- 3. Examiner les valeurs NULL

-- Les valeurs NULL dans total_laid_off, percentage_laid_off et funds_raised_millions semblent normales.



-- 4. Supprimer toutes les colonnes et lignes dont nous n’avons pas besoin

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Supprimer les données inutiles que nous ne pouvons pas vraiment exploiter
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM world_layoffs.layoffs_staging2;
