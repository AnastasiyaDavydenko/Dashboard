-- создаём 4 таблицы для источников, загружаем в них данные из csv-файлов

CREATE TABLE transfers_to_ad_systems
	(user_id INTEGER,
	ad_system VARCHAR(9),
	account_id INTEGER,
	date_payed TIMESTAMP WITH TIME ZONE,
	price NUMERIC,
	currency VARCHAR(3));

CREATE TABLE ad_systems_accounts
	(user_id INTEGER,
	ad_system VARCHAR(9),
	account_id INTEGER,
	account_date_created TIMESTAMP WITH TIME ZONE);

CREATE TABLE exchange_rates
	(date DATE,
	target_currency VARCHAR(3),
	base_currency VARCHAR(3),
	rate FLOAT4);

CREATE TABLE users_registrations
	(user_id INTEGER,
	user_date_registration TIMESTAMP WITH TIME ZONE,
	user_country VARCHAR(50),
	utm_medium VARCHAR(50),
	utm_source VARCHAR(50),
	utm_campaign VARCHAR(50)
);

-- создаём таблицы справочников для модели данных, которые будем поджключеть в Power BI, наполняем их данными

-- ad_systems
CREATE TABLE ad_systems
(ad_system VARCHAR(9),
ad_system_id SERIAL);

INSERT INTO ad_systems (ad_system)
SELECT DISTINCT ad_system FROM ad_systems_accounts;

-- multisystems
CREATE TABLE multisystems
(multisystem SMALLINT);

INSERT INTO multisystems (multisystem)
VALUES (0), (1);

-- users_country
CREATE TABLE Country
(country_id VARCHAR(3),
country VARCHAR(50));

INSERT INTO country (country_id, country)
VALUES ('ARM', 'Армения'),
 ('UKR', 'Украина'),
 ('MDA', 'Молдова'),
 ('TKM', 'Туркменистан'),
 ('KAZ', 'Казахстан'),
 ('AZE', 'Азербайджан'),
 ('TJK', 'Таджикистан'),
 ('RUS', 'Россия'),
 ('KGZ', 'Кыргызстан'),
 ('USA', 'США'),
 ('BLR', 'Беларусь'), 
 ('UZB', 'Узбекистан');


-- utm_medium
CREATE TABLE utm_medium
(utm_medium VARCHAR(9),
utm_medium_id SERIAL);

INSERT INTO utm_medium (utm_medium)
SELECT DISTINCT utm_medium FROM users_registrations;

-- utm_source
CREATE TABLE utm_source
(utm_source VARCHAR(50),
utm_source_id SERIAL);

INSERT INTO utm_source (utm_source)
SELECT DISTINCT utm_source FROM users_registrations;

-- utm_campaign
CREATE TABLE utm_campaign
(utm_campaign VARCHAR(50),
utm_campaign_id SERIAL);

INSERT INTO utm_campaign (utm_campaign)
SELECT DISTINCT utm_campaign FROM users_registrations;

-- price_category
CREATE TABLE price_category
(price_category VARCHAR(50),
min_price FLOAT4,
max_price FLOAT4,
price_category_id SERIAL);

INSERT INTO price_category (price_category, min_price, max_price)
VALUES ('менее 60000', 0, 59999), 
('от 60000 до 120000', 60000, 119999), 
('от 120000 до 180000', 120000, 179999),
('от 180000 до 239999', 180000, 239999),
('больше 240000', 240000, 2000000000);

-- amount_category
CREATE TABLE amount_category
(amount_category VARCHAR(50),
min_price FLOAT4,
max_price FLOAT4,
amount_category_id SERIAL);

INSERT INTO amount_category (amount_category, min_price, max_price)
VALUES ('low', 0, 41800000), 
('medium', 41800000, 83800000), 
('high', 83800000, 9000000000);

-- users
CREATE TABLE users
(user_id INTEGER,
 user_date_registration DATE,
 country_id VARCHAR(3),
 utm_medium_id INTEGER,
 utm_source_id INTEGER,
 utm_campaign_id INTEGER,
 amount_category_id INTEGER,
 multy INTEGER);

INSERT INTO users
(user_id,
 user_date_registration,
 country_id,
 utm_medium_id,
 utm_source_id,
 utm_campaign_id,
 amount_category_id,
 multy
)
SELECT 
 ur.user_id,
 DATE(user_date_registration) AS user_date_registration,
 country_id,
 utm_medium_id,
 utm_source_id,
 utm_campaign_id,
 amount_category_id,
 multy
FROM users_registrations AS ur
LEFT JOIN utm_medium ON ur.utm_medium = utm_medium.utm_medium
LEFT JOIN country ON ur.user_country = country.country_id
LEFT JOIN utm_source ON ur.utm_source = utm_source.utm_source
LEFT JOIN utm_campaign ON ur.utm_campaign = utm_campaign.utm_campaign
LEFT JOIN 
 (SELECT 
  user_id,
  amount_category_id
 FROM 
  (SELECT 
   user_id,
   SUM(price) AS amount
  FROM transfers_to_ad_systems
  GROUP BY user_id) AS t1
 LEFT JOIN amount_category AS ac
 ON t1.amount BETWEEN ac.min_price AND ac.max_price) AS t2 ON ur.user_id = t2.user_id
LEFT JOIN ( 
 SELECT
  user_id,
  CASE WHEN MAX(per) <= 0.7 THEN 1 ELSE 0 END AS multy
 FROM(
  SELECT 
   user_id,
   amount_by_system / SUM(amount_by_system) OVER(PARTITION BY user_id) AS per
  FROM (
   SELECT 
    user_id,
    ad_system,
    SUM(price) AS amount_by_system
   FROM transfers_to_ad_systems
   GROUP BY user_id, ad_system
   ORDER BY user_id, ad_system) AS t1) AS t2
 GROUP BY user_id ) AS t3 ON ur.user_id = t3.user_id;


-- создаём таблицу фактов fact_transfers_to_ad_systems и заполняем её данными
CREATE TABLE fact_transfers_to_ad_systems 
(user_id INTEGER,
ad_system_id INTEGER,
date_payed DATE,
price_rub NUMERIC,
price_usd NUMERIC,
price_category_id INTEGER,
chern_users_in_systems INTEGER,
chern_users INTEGER);

INSERT INTO fact_transfers_to_ad_systems
(user_id,
 ad_system_id,
 date_payed,
 price_rub,
 price_usd,
 price_category_id,
 chern_users_in_systems,
 chern_users
)
SELECT 
user_id,
    ad_system_id,
    date_payed,
    price_rub,
    price_usd,
 	price_category_id,
 	CASE WHEN ((next_event_date_in_system > 30 OR next_event_date_in_system IS NULL) AND date_payed < (SELECT DATE(MAX(date_payed) - INTERVAL '30 days') FROM transfers_to_ad_systems)) 
 		THEN 1 
 		ELSE 0 
 		END AS chern_users_in_systems,
 	CASE WHEN ((users_next_event_date > 30 OR users_next_event_date IS NULL) AND date_payed < (SELECT DATE(MAX(date_payed) - INTERVAL '30 days') FROM transfers_to_ad_systems)) 
 		THEN 1 
 		ELSE 0 
 		END AS chern_users
FROM
(SELECT
    user_id,
    ad_system_id,
    DATE(date_payed) AS date_payed,
    price AS price_rub,
    price / rate AS price_usd,
 	pc.price_category_id,
 	LEAD(DATE(date_payed)) OVER(PARTITION BY user_id, ft.ad_system  ORDER BY DATE(date_payed))- DATE(date_payed) AS next_event_date_in_system,
 	LEAD(DATE(date_payed)) OVER(PARTITION BY user_id  ORDER BY DATE(date_payed))- DATE(date_payed) AS users_next_event_date 	
FROM
    transfers_to_ad_systems AS ft
    LEFT JOIN ad_systems AS ads ON ft.ad_system = ads.ad_system 
    LEFT JOIN exchange_rates AS er ON DATE(ft.date_payed) = er.date
 LEFT JOIN price_category AS pc ON ft.price BETWEEN pc.min_price AND pc.max_price) AS t1;
