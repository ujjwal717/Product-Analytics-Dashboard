-- EDA 

--1) How many new customer we acquired from each segment based on latest available YTD window

WITH latest_account_created AS ( -- Identify the most recent account creation date (used to set reporting window)
SELECT MAX(created_at) AS latest_date 
FROM dim_accounts), 

account_created_window AS ( -- Define reporting window (YTD): 
-- If only January data exists for the latest year, fall back to previous year

SELECT CASE WHEN EXTRACT(MONTH FROM latest_date) = 1 THEN 
CONCAT(EXTRACT(YEAR FROM latest_date) - 1, '-01-01')::date ELSE 
CONCAT(EXTRACT (YEAR FROM latest_date), '-01-01')::date END AS year_beginning, 

CASE WHEN EXTRACT(MONTH FROM latest_date) = 1 THEN 
CONCAT(EXTRACT (YEAR FROM latest_date) - 1, '-12-31')::date ELSE

(SELECT MAX(date) 
FROM dim_date AS dd 
JOIN latest_account_created AS la ON EXTRACT(YEAR FROM dd.date) = EXTRACT(YEAR FROM la.latest_date) AND 
EXTRACT(MONTH FROM dd.date) = EXTRACT(MONTH FROM la.latest_date))::date END AS latest_date_window

FROM latest_account_created)

-- Counts customer by segment between the set reporting window (YTD))
SELECT segment, count(DISTINCT account_sk) AS customer_count
FROM dim_accounts AS da 
JOIN account_created_window AS ac ON 1=1
WHERE da.created_at BETWEEN year_beginning AND latest_date_window
GROUP BY segment
ORDER BY customer_count DESC 


--2) How many total accounts, churned accounts and churn rate we have for the latest window YTD

WITH latest_date_cte AS ( --Identify the most recent subscription start date (used to set reporting window)
SELECT MAX(start_date) AS latest_date 
FROM fact_subscriptions), 

year_start_cte AS (  -- Pick the starting date of the latest year (reporting window YTD)
SELECT CONCAT(EXTRACT(YEAR FROM latest_date), '-01-01')::date AS year_begin_date
FROM latest_date_cte),

concerned_accounts AS (  -- Fetch accounts in the reporting window YTD
SELECT account_sk, product_sk, start_date, end_date, status 
FROM fact_subscriptions AS fs 
JOIN dim_accounts AS dm USING (account_sk)
JOIN year_start_cte AS ys ON 1=1
JOIN latest_date_cte AS ld ON 1=1 
WHERE start_date <= year_begin_date AND (end_date IS NULL OR (end_date >=year_begin_date))), 

churned_non_churned_accounts AS (  -- Categorize accounts as churned or not churned
-- If account has even one subscription as active, it is not churned else churned
SELECT DISTINCT account_sk, agg_status, 

CASE WHEN agg_status LIKE '%Active%' THEN 'Not Churned' ELSE 'Churned' END AS churned_status -- Categorized accounts as churned or not churned

FROM (SELECT DISTINCT account_sk, string_agg(status, ',') AS agg_status  -- Aggregating sub string to check cumulative status of multipe subscriptions

FROM concerned_accounts
GROUP BY account_sk) AS m 
JOIN concerned_accounts AS c USING (account_sk))

-- Count total accounts, churned accounts, and churn rate
SELECT count(DISTINCT account_sk) AS total_accounts, SUM(CASE WHEN churned_status = 'Churned' THEN 1 ELSE 0 END) AS churned_accounts,

(SUM(CASE WHEN churned_status = 'Churned' THEN 1 ELSE 0 END)::FLOAT/count(DISTINCT account_sk)) * 100 AS churn_rate
FROM churned_non_churned_accounts


-- 3) Fetch Subscriptions that are due renewal within 90 days

SELECT account_sk, subscription_id, product_name, plan_name,fs.status, renewal_date -- Fetch relevant subscription details to reach out to customer regarding renewals

FROM fact_subscriptions AS fs

JOIN dim_accounts AS da USING (account_sk) 
JOIN dim_products AS dp USING (product_sk)
JOIN dim_plans AS dl USING (plan_sk)

WHERE fs.status = 'Active' AND 
renewal_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '90 Days'


-- 4) Show the cohort survival retention for subscription acquired between Jan' 24 -> Dec '24 for the latest year and months


WITH latest_date_cte AS ( -- Fetches latest date
SELECT MAX(start_date) AS latest_date 
FROM fact_subscriptions),

previous_year AS (  -- Fetches previous year beginning and ending for efficient window YTD
SELECT concat(EXTRACT(YEAR FROM latest_date) -1, '-01-01')::DATE AS previous_year_beginning, 

concat(EXTRACT(YEAR FROM latest_date) -1, '-12-31')::DATE AS previous_year_ending 

FROM latest_date_cte), 

cohort_cte AS (  -- Fetches subscription details that began between window YTD (Jan '2024 ->  Dec '2024)
SELECT * 
FROM fact_subscriptions AS fs 
JOIN previous_year AS py ON 1=1 
WHERE fs.start_date BETWEEN previous_year_beginning AND previous_year_ending AND 
(end_date IS NULL OR end_date NOT BETWEEN previous_year_beginning AND previous_year_ending)), 

starting_subs AS (
SELECT py.previous_year_ending, count(subscription_id) AS sub_count
FROM cohort_cte AS cc 
JOIN previous_year AS py ON 1=1
GROUP BY py.previous_year_ending

UNION ALL 

-- Fetches cancelled subscription count for each month acorss latest year window
SELECT CONCAT(EXTRACT(YEAR FROM latest_date), '-0', EXTRACT(MONTH FROM end_date), '-01')::date , 

CASE WHEN COALESCE(EXTRACT(MONTH FROM end_date),0) = 0 THEN count(subscription_id)  -- Calculates cancelled subscriptions negatively to use it further for cohort survival
ELSE count(subscription_id) * (-1) END AS sub_count 

FROM cohort_cte
JOIN latest_date_cte AS ld ON 1=1
JOIN previous_year AS py ON 1=1

WHERE status = 'Cancelled'

GROUP BY EXTRACT(MONTH FROM end_date), status, latest_date )

-- Calculates effective subscription count for each successive month for latest year (cohort survival)
SELECT previous_year_ending AS date, SUM(sub_count) OVER(ORDER BY previous_year_ending) AS effective_sub_count
FROM starting_subs


-- 5) Calculate the Adoption Rate for Products and relevant features 

WITH active_accounts AS (  -- Fetches latest date 
SELECT DISTINCT account_sk , product_sk 
FROM fact_subscriptions 
WHERE status = 'Active' AND (end_date IS NULL OR end_date >= CURRENT_DATE)),

feature_users_cte AS (  -- Fetches accounts that use products and specific features
SELECT DISTINCT fe.account_sk, fe.product_sk, fe.feature_sk
FROM fact_events AS fe 
JOIN active_accounts AS aa ON aa.account_sk = fe.account_sk AND aa.product_sk = fe.product_sk 
WHERE fe.feature_sk IS NOT NULL), 

effective_users_per_feature AS (  -- Counts account based on features
SELECT product_sk, feature_sk, count(DISTINCT fu.account_sk) AS feature_users
FROM feature_users_cte AS fu 
GROUP BY product_sk, feature_sk), 

effective_total_users AS (  -- Counts accounts for each products
SELECT product_sk, count(DISTINCT account_sk) AS users_per_product
FROM active_accounts
GROUP BY product_sk)

-- Calculates feature adoption rate
SELECT product_sk, feature_sk, feature_users, users_per_product, round((feature_users::NUMERIC/users_per_product::NUMERIC)*100,2) AS feature_adoption
FROM effective_users_per_feature AS eu 
JOIN effective_total_users AS et USING (product_sk)



-- Various Materialized Views including detailed analysis for each report page including - executive report page, revenue report page, customer page, and cohort and product page





-- Materialized View for MRR and ARR snapshot

CREATE MATERIALIZED VIEW mrr_arr_snapshot AS (

WITH latest_sub_date AS (
SELECT MAX(start_date) AS latest_date FROM fact_subscriptions)

SELECT account_sk, product_sk, industry, segment, hq_country, net_mrr_usd
FROM fact_subscriptions AS fs 
JOIN latest_sub_date AS ld ON 1=1
JOIN dim_accounts AS da USING (account_sk)
WHERE start_date <= latest_date AND
status = 'Active' AND (end_date IS NULL OR end_date >= latest_date))


-- Dimension materialized view for segments

CREATE MATERIALIZED VIEW dim_segment_view AS (

SELECT DISTINCT segment 
FROM dim_accounts)


-- Dimension materialized view for industry

CREATE MATERIALIZED VIEW dim_industry_view AS (

SELECT DISTINCT industry 
FROM dim_accounts)


-- Fact materialized view to calculate mrr trends and active accounts

CREATE MATERIALIZED VIEW fact_mrr_accounts AS (

SELECT account_sk, product_sk, industry, segment, hq_country, start_date, net_mrr_usd 
FROM dim_accounts AS da 
JOIN fact_subscriptions AS fs USING (account_sk)
WHERE status = 'Active' AND end_date IS NULL)


-- Materialized view for previous month mrr to calculate MRR month-over-month growth:

CREATE MATERIALIZED VIEW fact_previous_month_net_mrr AS(

WITH latest_date_cte AS (
SELECT MAX(start_date) AS latest_date 
FROM fact_subscriptions), 

previous_month_year AS (
SELECT CASE WHEN EXTRACT(MONTH FROM latest_date) = 1 THEN 12 ELSE EXTRACT(MONTH FROM latest_date) - 1 END AS previous_month, 

CASE WHEN EXTRACT(MONTH FROM latest_date) = 1 THEN EXTRACT(YEAR FROM latest_date) - 1 ELSE 
EXTRACT(YEAR FROM latest_date) END AS effective_year

FROM latest_date_cte), 

previous_final_date AS (
SELECT max(d.date) AS latest_previous_date 
FROM dim_date AS d 
JOIN previous_month_year ON EXTRACT(YEAR FROM d.date) = effective_year AND 
EXTRACT(MONTH FROM d.date) = previous_month)

SELECT account_sk, product_sk, industry, segment, hq_country, start_date, net_mrr_usd 
FROM fact_subscriptions  AS fs
JOIN dim_accounts AS da USING (account_sk)
JOIN previous_final_date AS pf ON 1=1 
WHERE start_date <= latest_previous_date AND 
(end_date IS NULL OR end_date >= latest_previous_date) AND status = 'Active'
ORDER BY start_date DESC)


-- Materialized view for previous year mrr for december:

CREATE MATERIALIZED VIEW previous_year_mrr AS ( 

WITH latest_date AS (
SELECT MAX(start_date) AS latest_date 
FROM fact_subscriptions), 

previous_year_latest_date AS (
SELECT CONCAT(EXTRACT(YEAR FROM latest_date) - 1, '-','12-31')::date AS dates 
FROM latest_date)

SELECT account_sk, product_sk, industry, segment, hq_country, net_mrr_usd
FROM fact_subscriptions AS fs 
JOIN dim_accounts AS da USING (account_sk)
JOIN previous_year_latest_date AS py ON 1=1
WHERE start_date <= dates AND status = 'Active' AND 
(end_date IS NULL OR end_date >= dates))


-- Materialized view for older window to calculate NRR:

CREATE MATERIALIZED VIEW previous_mrr_for_nrr AS (

WITH latest_date_cte AS (
SELECT MAX(start_date) AS latest_date, (MAX(start_date) - INTERVAL '11 MONTH') AS previous_date, 
EXTRACT (MONTH FROM (MAX(start_date) - INTERVAL '11 MONTH')) AS previous_month, 
EXTRACT (YEAR FROM (MAX(start_date) - INTERVAL '11 MONTH')) AS previous_year
FROM fact_subscriptions), 

previous_latest_date_cte AS (
SELECT MAX(date) AS previous_latest_date
FROM latest_date_cte AS ld 
JOIN dim_date AS dd ON 1=1 
WHERE EXTRACT(MONTH FROM date) = previous_month AND EXTRACT(YEAR FROM date) = previous_year)

SELECT account_sk, product_sk, industry, segment, hq_country, net_mrr_usd
FROM fact_subscriptions AS fs 
JOIN dim_accounts AS da USING (account_sk)
JOIN previous_latest_date_cte AS pl ON 1=1
WHERE start_date <= previous_latest_date AND status = 'Active' AND 
(end_date IS NULL OR end_date >= previous_latest_date))


-- Materialized view for current mrr snapshot based on previous mrr window to calculate NRR:

CREATE MATERIALIZED VIEW current_mrr_for_nrr AS (

WITH latest_date_cte AS (
SELECT MAX(start_date) AS latest_date
FROM fact_subscriptions )


SELECT fs.account_sk, pm.product_sk, pm.industry, pm.segment, pm.hq_country, fs.net_mrr_usd 
FROM fact_subscriptions AS fs 
JOIN previous_mrr_for_nrr AS pm USING (account_sk)
JOIN latest_date_cte AS ld ON 1=1
WHERE status = 'Active' AND start_date <= latest_date AND 
(end_date IS NULL OR end_date >= latest_date))


-- Materialized view for Churn Rate:

CREATE MATERIALIZED VIEW mv_churn_rate AS (
WITH latest_date_cte AS (
SELECT MAX(start_date) AS latest_date 
FROM fact_subscriptions), 

year_start_cte AS (
SELECT CONCAT(EXTRACT(YEAR FROM latest_date), '-01-01')::date AS year_begin_date
FROM latest_date_cte),

concerned_accounts AS (
SELECT account_sk, product_sk, start_date, end_date, status 
FROM fact_subscriptions AS fs 
JOIN dim_accounts AS dm USING (account_sk)
JOIN year_start_cte AS ys ON 1=1
JOIN latest_date_cte AS ld ON 1=1 
WHERE start_date <= year_begin_date AND (end_date IS NULL OR (end_date >=year_begin_date))), 

churned_non_churned_accounts AS (
SELECT DISTINCT account_sk, agg_status, 

CASE WHEN agg_status LIKE '%Active%' THEN 'Not Churned' ELSE 'Churned' END AS churned_status 

FROM (SELECT DISTINCT account_sk, string_agg(status, ',') AS agg_status 
FROM concerned_accounts
GROUP BY account_sk) AS m 
JOIN concerned_accounts AS c USING (account_sk))

SELECT (SUM(CASE WHEN churned_status = 'Churned' THEN 1 ELSE 0 END)::FLOAT/count(DISTINCT account_sk)) * 100 AS churn_rate
FROM churned_non_churned_accounts)


-- Materialized view for Stickiness (DAU/MAU) for the latest day and month

CREATE MATERIALIZED VIEW mv_stickiness AS (
WITH latest_event_time AS (
  SELECT 
    MAX(event_timestamp)::date AS latest_event_timestamp,
    MAX(event_timestamp)::TIME AS latest_time
  FROM fact_events
),
month_starting AS (
  SELECT DATE_TRUNC('month', latest_event_timestamp::TIMESTAMP) AS latest_month_beginning
  FROM latest_event_time
),

-- DAU users (feature events only) on the latest day up to latest_time
dau_cte AS (
  SELECT 
    fe.product_sk,
    fe.contact_sk AS dau_contact
  FROM fact_events AS fe
  JOIN dim_contacts AS dc USING (contact_sk)
  JOIN dim_accounts AS da ON fe.account_sk = da.account_sk
  JOIN latest_event_time AS le ON 1=1
  WHERE fe.event_type NOT IN ('login','logout','page_view')
    AND fe.event_timestamp::date = le.latest_event_timestamp
    AND fe.event_timestamp::TIME <= le.latest_time
  GROUP BY fe.product_sk, fe.contact_sk
),

-- MAU users (feature events only) from month start up to latest moment
mau_cte AS (
SELECT fe.product_sk, fe.contact_sk AS mau_contact
FROM fact_events AS fe
JOIN dim_contacts AS dc USING (contact_sk)
JOIN dim_accounts AS da ON fe.account_sk = da.account_sk
JOIN latest_event_time AS le ON 1=1
JOIN month_starting AS ms ON 1=1
WHERE fe.event_type NOT IN ('logout','login','page_view')
AND fe.event_timestamp >= ms.latest_month_beginning
AND (fe.event_timestamp::date <  le.latest_event_timestamp OR (fe.event_timestamp::date = le.latest_event_timestamp
AND fe.event_timestamp::TIME <= le.latest_time))
GROUP BY fe.product_sk, fe.contact_sk
)

SELECT m.product_sk, -- DAU per product counted once, avoiding join duplication
(SELECT COUNT(DISTINCT d2.dau_contact)
FROM dau_cte d2
WHERE d2.product_sk = m.product_sk)::FLOAT/ NULLIF(COUNT(DISTINCT m.mau_contact), 0)::FLOAT AS stickiness
FROM mau_cte m
GROUP BY m.product_sk
ORDER BY m.product_sk)


-- View for flagging each account as either "Churned" or "Not Churned":

CREATE VIEW mc_churned_non_churned_status AS (

WITH concerned_accounts AS (
SELECT account_sk, product_sk, start_date, end_date, status 
FROM fact_subscriptions AS fs), 

churned_non_churned_accounts AS (
SELECT DISTINCT account_sk, agg_status, 

CASE WHEN agg_status LIKE '%Active%' THEN 'Not Churned' ELSE 'Churned' END AS churned_status 

FROM (SELECT DISTINCT account_sk, string_agg(status, ',') AS agg_status 
FROM concerned_accounts
GROUP BY account_sk) AS m 
JOIN concerned_accounts AS c USING (account_sk))

SELECT * FROM churned_non_churned_accounts)


--Materialized view for New Users acquired since latest year's beginning:

CREATE MATERIALIZED VIEW mv_active_users AS (
WITH latest_date_cte AS (
SELECT MAX(start_date) AS latest_date 
FROM fact_subscriptions), 

year_start_cte AS (
SELECT CONCAT(EXTRACT(YEAR FROM latest_date), '-01-01')::date AS year_begin_date
FROM latest_date_cte), 

all_new_accounts_cte AS (
SELECT * 
FROM dim_accounts AS da 
JOIN year_start_cte AS ys ON 1=1
WHERE created_at >= year_begin_date), 

new_users AS (
SELECT * 
FROM all_new_accounts_cte AS an 
LEFT JOIN mc_churned_non_churned_status AS mc USING (account_sk)
WHERE churned_status = 'Not Churned')

SELECT * FROM new_users)


-- Materialized view for Churned users since latest year beginning:

CREATE MATERIALIZED VIEW mv_churned_users AS (

WITH latest_date_cte AS (
SELECT MAX(start_date) AS latest_date 
FROM fact_subscriptions), 

year_start_cte AS (
SELECT CONCAT(EXTRACT(YEAR FROM latest_date), '-01-01')::date AS year_begin_date
FROM latest_date_cte), 

all_new_accounts_cte AS (
SELECT * 
FROM dim_accounts AS da 
JOIN year_start_cte AS ys ON 1=1
WHERE created_at >= year_begin_date), 

churned_users AS (
SELECT * 
FROM all_new_accounts_cte AS an 
LEFT JOIN mc_churned_non_churned_status AS mc USING (account_sk)
WHERE churned_status = 'Churned')

SELECT * FROM new_users) 


-- Materialized view for overall seat utilization:

CREATE MATERIALIZED VIEW mv_overall_utilization AS (
SELECT SUM(seat_commit) AS total_seat_commits, SUM(seat_provisioned) AS total_seat_provisions 
FROM fact_mrr_accounts AS fm 
JOIN fact_subscriptions AS fs USING (account_sk))


--Materialized view for specific seat utilization based on product, industry, segment, country:

CREATE MATERIALIZED VIEW mv_specific_utilization AS (

SELECT fm.product_sk, fm.industry, fm.segment, fm.hq_country,
SUM(seat_commit) AS specific_seat_commits, SUM(seat_provisioned) AS specific_seat_provisions 
FROM fact_mrr_accounts AS fm 
JOIN fact_subscriptions AS fs USING (account_sk)
GROUP BY fm.product_sk, fm.industry, fm.segment, fm.hq_country)


-- View for renewal pipeline (accounts that have renewal date within 90 days from current date):

CREATE VIEW mv_renewal_pipeline AS (
SELECT account_sk, product_name, plan_name,fs.status, renewal_date
FROM fact_subscriptions AS fs
JOIN dim_accounts AS da USING (account_sk) 
JOIN dim_products AS dp USING (product_sk)
JOIN dim_plans AS dl USING (plan_sk)
WHERE fs.status = 'Active' AND 
renewal_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '90 Days')



-- Isolated view for disabled auto renew:

CREATE MATERIALIZED VIEW mv_renew_disable AS (

WITH renew_disabled_cte AS (
SELECT COUNT(DISTINCT subscription_id) AS renew_disabled_count, product_sk,industry, segment, hq_country
FROM fact_subscriptions AS fs 
JOIN dim_accounts AS da USING (account_sk)
WHERE is_auto_renew IS FALSE AND status = 'Active'
GROUP BY product_sk,industry, segment, hq_country)

SELECT * FROM renew_disabled_cte)



-- Isolated view for enabled auto renew:

CREATE MATERIALIZED VIEW mv_renew_enable AS (

renew_enabled_cte AS (
SELECT COUNT(DISTINCT subscription_id) AS renew_enabled_count, product_sk,industry, segment, hq_country
FROM fact_subscriptions AS fs 
JOIN dim_accounts AS da USING (account_sk)
WHERE is_auto_renew IS TRUE AND status = 'Active'
GROUP BY product_sk,industry, segment, hq_country)

SELECT * FROM renew_enabled_cte)


-- View for calculating month wise cancelled for latest year

CREATE VIEW mv_monthwise_cancelled AS (

WITH latest_date_cte AS (
SELECT max(start_date) AS latest_date
FROM fact_subscriptions), 

previous_year_cte AS (
SELECT CONCAT(EXTRACT(YEAR FROM latest_date) - 1, '-01-01')::date AS previous_year_beginning, 

CONCAT(EXTRACT(YEAR FROM latest_date) - 1, '-12-31')::date AS previous_year_ending

FROM latest_date_cte ), 

previous_year_cohort AS (
SELECT * 
FROM fact_subscriptions AS fs
JOIN previous_year_cte AS py ON 1=1
WHERE start_date BETWEEN previous_year_beginning AND previous_year_ending AND (end_date IS NULL OR end_date NOT BETWEEN previous_year_beginning AND previous_year_ending)),

month_wise_cancelled AS (
SELECT COALESCE(EXTRACT(MONTH FROM end_date),0) AS month_num, status, count(subscription_id) AS sub_count
FROM previous_year_cohort 
GROUP BY EXTRACT(MONTH FROM end_date), status)


SELECT * FROM month_wise_cancelled)


-- Materialized view for Cohort Retention (Dec,2024 --> Latest Month, 2025):

CREATE MATERIALIZED VIEW mv_sub_trend AS (

WITH latest_date_cte AS (
SELECT MAX(start_date) AS latest_date 
FROM fact_subscriptions),

previous_year AS (
SELECT concat(EXTRACT(YEAR FROM latest_date) -1, '-01-01')::DATE AS previous_year_beginning, 

concat(EXTRACT(YEAR FROM latest_date) -1, '-12-31')::DATE AS previous_year_ending 

FROM latest_date_cte), 

cohort_cte AS (
SELECT * 
FROM fact_subscriptions AS fs 
JOIN previous_year AS py ON 1=1 
WHERE fs.start_date BETWEEN previous_year_beginning AND previous_year_ending AND 
(end_date IS NULL OR end_date NOT BETWEEN previous_year_beginning AND previous_year_ending)), 

starting_subs AS (
SELECT py.previous_year_ending, count(subscription_id) AS sub_count
FROM cohort_cte AS cc 
JOIN previous_year AS py ON 1=1
GROUP BY py.previous_year_ending

UNION ALL 


SELECT CONCAT(EXTRACT(YEAR FROM latest_date), '-0', EXTRACT(MONTH FROM end_date), '-01')::date , 

CASE WHEN COALESCE(EXTRACT(MONTH FROM end_date),0) = 0 THEN count(subscription_id) 
ELSE count(subscription_id) * (-1) END AS sub_count 

FROM cohort_cte
JOIN latest_date_cte AS ld ON 1=1
JOIN previous_year AS py ON 1=1

WHERE status = 'Cancelled'

GROUP BY EXTRACT(MONTH FROM end_date), status, latest_date )

SELECT previous_year_ending AS date, SUM(sub_count) OVER(ORDER BY previous_year_ending) AS effective_sub_count
FROM starting_subs)


-- Materialized view for Retention Cohort:

CREATE MATERIALIZED VIEW mv_retention_cohort AS (

WITH inital_sub_cte AS (
SELECT effective_sub_count AS inital_sub_count
FROM mv_sub_trend
WHERE date = (SELECT MIN(date) FROM mv_sub_trend)), 

final_sub_cte AS (
SELECT effective_sub_count AS final_sub_count
FROM mv_sub_trend
WHERE date = (SELECT MAX(date) FROM mv_sub_trend))

SELECT *, final_sub_count::FLOAT/inital_sub_count AS cohort_retention
FROM inital_sub_cte AS i 
JOIN final_sub_cte AS f ON 1=1)


-- Materialized view for Device Type Dimension:

CREATE MATERIALIZED VIEW dim_device AS (

SELECT DISTINCT device_type 
FROM fact_events )


-- Materialized view for total active users for Adoption Rate:

CREATE MATERIALIZED VIEW mv_feature_usage_active_users AS (

WITH active_accounts AS (
SELECT DISTINCT account_sk , product_sk 
FROM fact_subscriptions 
WHERE status = 'Active' AND (end_date IS NULL OR end_date >= CURRENT_DATE))

SELECT * FROM active_accounts)


-- Materialized view for Feature Adopted Users for Adoption Rate:

CREATE MATERIALIZED VIEW mv_feature_usage_adopted_users AS (

WITH active_accounts AS (
SELECT DISTINCT account_sk , product_sk 
FROM fact_subscriptions 
WHERE status = 'Active' AND (end_date IS NULL OR end_date >= CURRENT_DATE)),

feature_users_cte AS (
SELECT DISTINCT fe.account_sk, fe.product_sk, fe.feature_sk
FROM fact_events AS fe 
JOIN active_accounts AS aa ON aa.account_sk = fe.account_sk AND aa.product_sk = fe.product_sk 
WHERE fe.feature_sk IS NOT NULL)

SELECT product_sk, feature_sk, fu.account_sk AS feature_users
FROM feature_users_cte AS fu) 


-- Materialized view for Event Depth:

CREATE MATERIALIZED VIEW mv_event_depth AS (

SELECT product_sk, industry, segment, hq_country, count(event_sk)::FLOAT AS events_count, count(DISTINCT contact_sk) AS user_count
FROM fact_events AS fe 
JOIN dim_accounts AS da USING (account_sk)
JOIN dim_contacts AS dc USING (contact_sk)
WHERE event_type NOT IN ('login', 'page_view', 'logout') AND event_timestamp BETWEEN '2024-12-01' AND '2024-12-31'
GROUP BY product_sk, industry, segment, hq_country)


-- Materialized view for Feature Adoption:

CREATE MATERIALIZED VIEW mv_adoption_rates AS (

WITH active_accounts AS (
SELECT DISTINCT account_sk , product_sk 
FROM fact_subscriptions 
WHERE status = 'Active' AND (end_date IS NULL OR end_date >= CURRENT_DATE)),

feature_users_cte AS (
SELECT DISTINCT fe.account_sk, fe.product_sk, fe.feature_sk
FROM fact_events AS fe 
JOIN active_accounts AS aa ON aa.account_sk = fe.account_sk AND aa.product_sk = fe.product_sk 
WHERE fe.feature_sk IS NOT NULL), 

effective_users_per_feature AS (
SELECT product_sk, feature_sk, count(DISTINCT fu.account_sk) AS feature_users
FROM feature_users_cte AS fu 
GROUP BY product_sk, feature_sk), 

effective_total_users AS (
SELECT product_sk, count(DISTINCT account_sk) AS users_per_product
FROM active_accounts
GROUP BY product_sk)

SELECT product_sk, feature_sk, feature_users, users_per_product, feature_users::FLOAT/users_per_product AS feature_adoption
FROM effective_users_per_feature AS eu 
JOIN effective_total_users AS et USING (product_sk))


-- Materialized view for Adoption Rate across products, features, and device type:

CREATE MATERIALIZED VIEW mv_adoption_devices AS (

WITH feature_users AS (
SELECT product_sk, device_type, feature_sk, count(DISTINCT contact_sk) AS feature_employees_count
FROM fact_events 
WHERE event_type ='feature_use' AND feature_sk IS NOT NULL 
GROUP BY product_sk, device_type, feature_sk ), 

total_users AS (
SELECT product_sk, device_type, count(DISTINCT contact_sk) AS total_employees_count
FROM fact_events 
GROUP BY product_sk, device_type )

SELECT DISTINCT product_sk, fu.device_type, feature_sk, feature_employees_count, total_employees_count, feature_employees_count::FLOAT/NULLIF(tu.total_employees_count, 0)::FLOAT AS adoption_rate
FROM feature_users AS fu
JOIN total_users AS tu USING (product_sk, device_type))



-- Materialized view for Usage Frequency (Dec 1, 2024 -> Dec 31, 2024):

CREATE MATERIALIZED VIEW mv_login_frequency AS (


WITH latest_timestamp_cte AS (
SELECT MAX(event_timestamp) AS latest_timestamp
FROM fact_events), 

previous_year_timestamp AS (
SELECT CONCAT(EXTRACT(YEAR FROM latest_timestamp) - 1, '-12-01')::date AS previous_year_beginning, 
CONCAT(EXTRACT(YEAR FROM latest_timestamp), '-01-01')::date AS previous_year_ending
FROM latest_timestamp_cte)


SELECT product_sk, segment, count(event_type)::FLOAT/NULLIF(count(DISTINCT contact_sk),0) AS login_frequency
FROM fact_events AS fe 
JOIN dim_accounts AS da USING (account_sk)
JOIN previous_year_timestamp AS py ON 1=1
WHERE event_type = 'login' AND fe.event_timestamp >= previous_year_beginning AND 
fe.event_timestamp < previous_year_ending
GROUP BY product_sk, segment)



-- Materialized view for product users by country and segment:

CREATE MATERIALIZED VIEW mv_product_users_by_country AS (
SELECT product_sk, country, segment, COUNT(DISTINCT contact_sk) AS user_count
FROM fact_events 
JOIN dim_accounts AS da USING (account_sk)
GROUP BY product_sk, country, segment )





-- Materialized view for specific page view rate among products:

CREATE MATERIALIZED VIEW mv_specific_page_usage AS (

WITH latest_date_cte AS (
SELECT MAX(start_date) AS latest_date 
FROM fact_subscriptions),

previous_year AS (
SELECT concat(EXTRACT(YEAR FROM latest_date) -1, '-12-01')::DATE AS previous_year_last_month_beginning, 

concat(EXTRACT(YEAR FROM latest_date), '-01-01')::DATE AS current_year 

FROM latest_date_cte), 

main_page_views AS (
SELECT *, metadata->>'page' AS specific_page
FROM fact_events 
JOIN previous_year AS py ON 1=1 
WHERE event_type = 'page_view'AND event_timestamp >= previous_year_last_month_beginning AND event_timestamp < current_year),

total_page_views_cte  AS (
SELECT product_sk, COUNT(event_type) AS total_page_views
FROM main_page_views AS mp
GROUP BY product_sk),

specific_page_views_cte AS (
SELECT product_sk, specific_page, count(event_type) AS specific_page_views
FROM  main_page_views AS mp
GROUP BY specific_page, product_sk)

SELECT *
FROM total_page_views_cte AS tp 
JOIN specific_page_views_cte AS sp USING (product_sk))



-- Materialized view for Previous year (same month as latest) window and mrr:

CREATE MATERIALIZED VIEW mv_previous_window_mrr_snapshot AS (

WITH latest_sub_date AS (
SELECT MAX(start_date) AS latest_date
FROM fact_subscriptions), 

previous_year_window AS (
SELECT latest_date - INTERVAL '1 YEAR' AS previous_date
FROM latest_sub_date), 

concerned_previous_window AS (
SELECT MIN(date) AS concerned_previous_date_start, MAX(date) AS concerned_previous_date_end
FROM previous_year_window AS p
JOIN dim_date AS d ON EXTRACT(YEAR FROM d.date) = EXTRACT(YEAR FROM p.previous_date) AND 
EXTRACT(MONTH FROM d.date) = EXTRACT(MONTH FROM p.previous_date)), 


mrr_snapshot AS (
SELECT account_sk, product_sk, industry, segment, hq_country, net_mrr_usd
FROM fact_subscriptions AS fs 
JOIN concerned_previous_window AS CP ON 1=1
JOIN dim_accounts AS da USING (account_sk)
WHERE start_date <= concerned_previous_date_start AND
status = 'Active' AND (end_date IS NULL OR end_date >= concerned_previous_date_end))

SELECT * 
FROM mrr_snapshot)


-- Materialized View for Average Contract Value (ACV):

CREATE MATERIALIZED VIEW mv_acv AS (

WITH latest_date_cte AS (
SELECT MAX(start_date) AS latest_date
FROM fact_subscriptions),

latest_year_start_cte AS (
SELECT CONCAT(EXTRACT(YEAR FROM latest_date), '-01-01')::date AS latest_year_starting 
FROM latest_date_cte)

SELECT product_sk, industry, segment, hq_country, SUM(net_mrr_usd) * 12 AS total_acv
FROM fact_subscriptions AS fs
JOIN dim_accounts AS da USING (account_sk)
JOIN latest_date_cte AS ld ON 1=1
JOIN latest_year_start_cte AS ly ON 1=1
WHERE fs.status = 'Active' AND start_date BETWEEN latest_year_starting AND latest_date

GROUP BY account_sk, product_sk, industry, segment, hq_country)


-- Materialized view for Average Revenue Per Account (ARPA):

CREATE MATERIALIZED VIEW mv_arpa AS (

WITH latest_date_cte AS (
SELECT MAX(start_date) AS latest_date
FROM fact_subscriptions),

latest_year_start_cte AS (
SELECT CONCAT(EXTRACT(YEAR FROM latest_date), '-01-01')::date AS latest_year_starting 
FROM latest_date_cte)

SELECT product_sk, industry, segment, hq_country, 
(SUM(net_mrr_usd) * 12)/count(DISTINCT account_sk) AS annual_arpa
FROM fact_subscriptions AS fs
JOIN dim_accounts AS da USING (account_sk)
JOIN latest_date_cte AS ld ON 1=1
JOIN latest_year_start_cte AS ly ON 1=1
WHERE fs.status = 'Active' AND start_date BETWEEN latest_year_starting AND latest_date

GROUP BY product_sk, industry, segment, hq_country)



--Materialized view for Churn Revenue:

CREATE MATERIALIZED VIEW mv_churn_revenue AS (

WITH latest_date_cte AS (
SELECT MAX(start_date) AS latest_date
FROM fact_subscriptions),

latest_year_start_cte AS (
SELECT CONCAT(EXTRACT(YEAR FROM latest_date), '-01-01')::date AS latest_year_starting 
FROM latest_date_cte)

SELECT product_sk, industry, segment, hq_country, (SUM(net_mrr_usd) * 12) AS churn_revenue
FROM fact_subscriptions  AS fs
JOIN dim_accounts AS da USING (account_sk)
JOIN latest_year_start_cte AS ly ON 1=1
JOIN latest_date_cte AS ld ON 1=1
WHERE fs.status = 'Cancelled' AND fs.end_date BETWEEN latest_year_starting AND latest_date
GROUP BY product_sk, industry, segment, hq_country)



-- Materialized view for product adoption across device type

CREATE MATERIALIZED VIEW mv_adoption_products AS (

WITH feature_users AS (
SELECT product_sk, device_type, segment, count(DISTINCT contact_sk) AS product_employees_count
FROM fact_events AS fe 
JOIN dim_accounts AS da USING (account_sk)
WHERE event_type ='feature_use' AND feature_sk IS NOT NULL 
GROUP BY product_sk, device_type,segment), 

total_users AS (
SELECT product_sk, device_type, segment, count(DISTINCT contact_sk) AS total_employees_count
FROM fact_events 
JOIN dim_accounts AS da USING (account_sk)
GROUP BY product_sk, device_type, segment )

SELECT DISTINCT product_sk, segment, fu.device_type, product_employees_count, total_employees_count, product_employees_count::FLOAT/NULLIF(tu.total_employees_count, 0)::FLOAT AS adoption_rate
FROM feature_users AS fu
JOIN total_users AS tu USING (product_sk, device_type, segment))