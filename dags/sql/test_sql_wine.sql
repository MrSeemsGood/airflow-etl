DROP TABLE IF EXISTS wine_quality.quality 
;

CREATE TABLE wine_quality.quality
(
	  wine_type VARCHAR
	, avg_quality DECIMAL(10, 5)
)
;

INSERT INTO wine_quality.quality
(
	SELECT
		  'red' AS wine_type
		, AVG(quality) as avg_quality
	FROM wine_quality.red_wine
	UNION ALL
	SELECT
		  'white' AS wine_type
		, AVG(quality) as avg_quality
	FROM wine_quality.white_wine
)
;
