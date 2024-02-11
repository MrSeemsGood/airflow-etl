DROP SCHEMA IF EXISTS wine_quality CASCADE
;

CREATE SCHEMA IF NOT EXISTS wine_quality
;

DROP TABLE IF EXISTS wine_quality.red_wine
;

CREATE TABLE wine_quality.red_wine
(
    fixed_acidity DECIMAL(10, 5)
  , volatile_acidity DECIMAL(10, 5)
  , citric_acid DECIMAL(10, 5)
  , residual_sugar DECIMAL(10, 5)
  , chlorides DECIMAL(10, 5)
  , free_sulfur_dioxide DECIMAL(10, 5)
  , total_sulfur_dioxide DECIMAL(10, 5)
  , density DECIMAL(10, 5)
  , ph DECIMAL(10, 5)
  , sulphates DECIMAL(10, 5)
  , alcohol DECIMAL(10, 5)
  , quality INTEGER
)
;

DROP TABLE IF EXISTS wine_quality.white_wine
;

CREATE TABLE wine_quality.white_wine
(
    fixed_acidity DECIMAL(10, 5)
  , volatile_acidity DECIMAL(10, 5)
  , citric_acid DECIMAL(10, 5)
  , residual_sugar DECIMAL(10, 5)
  , chlorides DECIMAL(10, 5)
  , free_sulfur_dioxide DECIMAL(10, 5)
  , total_sulfur_dioxide DECIMAL(10, 5)
  , density DECIMAL(10, 5)
  , ph DECIMAL(10, 5)
  , sulphates DECIMAL(10, 5)
  , alcohol DECIMAL(10, 5)
  , quality INTEGER
)
;