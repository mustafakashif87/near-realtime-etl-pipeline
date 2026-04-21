-- Queries_DW
use project;

-- Q1. Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
select product_ID, product_category, month, day_type, total_revenue
from (
    select p.product_ID, p.product_category, t.month, case when t.Is_Weekday = 1 then 'Weekday' else 'Weekend' end as day_type, sum(s.total_sales_amount) as total_revenue,
        row_number() over (
            partition by t.month, case when t.Is_Weekday = 1 then 'Weekday' else 'Weekend' end 
            order by sum(s.total_sales_amount) desc
        ) as rn
    from sales_fact s
    join product_dim p on s.product_ID = p.product_ID
    join time_dim t on s.date_ID = t.DateID
    where t.year = 2020
    group by p.product_ID, p.product_category, t.month, day_type
) ranked
where rn <= 5
order by month, day_type, total_revenue desc;

-- Q2. Customer Demographics by Purchase Amount with City Category Breakdown
select c.gender, c.age, c.city_category, sum(s.total_sales_amount) as total_purchase
from sales_fact s
join customer_dim c on s.customer_ID = c.customer_ID
group by c.gender, c.age, c.city_category
order by c.city_category, total_purchase desc;

-- Q3. Product Category Sales by Occupation
select c.occupation, p.product_category, sum(s.total_sales_amount) as total_sales
from sales_fact s
join customer_dim c on s.customer_ID = c.customer_ID
join product_dim p on s.product_ID = p.product_ID
group by c.occupation, p.product_category
order by c.occupation, total_sales desc;

-- Q4. Total Purchases by Gender and Age Group with Quarterly Trend
select c.gender, c.age, t.quarter, sum(s.total_sales_amount) as total_purchase
from sales_fact s
join customer_dim c on s.customer_ID = c.customer_ID
join time_dim t on s.date_ID = t.DateID
where t.year = 2020
group by c.gender, c.age, t.quarter
order by t.quarter, c.gender, total_purchase desc;

-- Q5. Top Occupations by Product Category Sales
select product_category, occupation, total_sales
from (
    select p.product_category, c.occupation, sum(s.total_sales_amount) as total_sales,
        row_number() over (
            partition by p.product_category 
            order by sum(s.total_sales_amount) desc
        ) as rn
    from sales_fact s
    join customer_dim c on s.customer_ID = c.customer_ID
    join product_dim p on s.product_ID = p.product_ID
    group by p.product_category, c.occupation
) ranked
where rn <= 5
order by product_category, total_sales desc;

-- Q6. City Category Performance by Marital Status with Monthly Breakdown (asuming thge current date is december 31 2020)
select c.city_category, c.marital_status, t.month, sum(s.total_sales_amount) as total_purchase
from sales_fact s
join customer_dim c on s.customer_ID = c.customer_ID
join time_dim t on s.date_ID = t.DateID
where t.full_date >= date_sub('2020-12-31', interval 6 month)
group by c.city_category, c.marital_status, t.month
order by t.month, c.city_category, total_purchase desc;

-- Q7. Average Purchase Amount by Stay Duration and Gender
select c.stay_in_current_city_years, c.gender, avg(s.total_sales_amount) as avg_purchase
from sales_fact s
join customer_dim c on s.customer_ID = c.customer_ID
group by c.stay_in_current_city_years, c.gender
order by c.stay_in_current_city_years, c.gender;

-- Q8. Top 5 Revenue-Generating Cities by Product Category
select product_category, city_category, total_revenue
from (
    select p.product_category, c.city_category, sum(s.total_sales_amount) as total_revenue,
        row_number() over (
            partition by p.product_category
            order by sum(s.total_sales_amount) desc
        ) as rn
    from sales_fact s
    join customer_dim c on s.customer_ID = c.customer_ID
    join product_dim p on s.product_ID = p.product_ID
    group by p.product_category, c.city_category
) ranked
where rn <= 5
order by product_category, total_revenue desc;

-- Q9. Monthly Sales Growth by Product Category
select product_category, month, total_sales,
       round(((total_sales - prev_month_sales)/prev_month_sales)*100, 2) as mom_growth_percent
from (
    select p.product_category, t.month, sum(s.total_sales_amount) as total_sales,
        lag(sum(s.total_sales_amount)) over (
            partition by p.product_category
            order by t.month
        ) as prev_month_sales
    from sales_fact s
    join product_dim p on s.product_ID = p.product_ID
    join time_dim t on s.date_ID = t.DateID
    where t.year = 2020
    group by p.product_category, t.month
) monthly_sales
order by product_category, month;

-- Q10. Weekend vs. Weekday Sales by Age Group
select c.age, day_type, sum(s.total_sales_amount) as total_sales
from (
    select s.*, case when t.Is_Weekday = 1 then 'Weekday' else 'Weekend' end as day_type
    from sales_fact s
    join time_dim t on s.date_ID = t.DateID
    where t.year = 2020
) s
join customer_dim c on s.customer_ID = c.customer_ID
group by c.age, day_type
order by c.age, day_type, total_sales desc;

-- Q11. Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
select product_ID, product_category, month, day_type, total_revenue
from (
    select p.product_ID, p.product_category, t.month, case when t.Is_Weekday = 1 then 'Weekday' else 'Weekend' end as day_type, 
        sum(s.total_sales_amount) as total_revenue,
        row_number() over (
            partition by t.month, case when t.Is_Weekday = 1 then 'Weekday' else 'Weekend' end
            order by sum(s.total_sales_amount) desc
        ) as rn
    from sales_fact s
    join product_dim p on s.product_ID = p.product_ID
    join time_dim t on s.date_ID = t.DateID
    where t.year = 2020
    group by p.product_ID, p.product_category, t.month, day_type
) ranked
where rn <= 5
order by month, day_type, total_revenue desc;

-- Q12. Trend Analysis of Store Revenue Growth Rate Quarterly for 2017
select storeID, quarter, total_sales,
       round(((total_sales - prev_quarter_sales)/prev_quarter_sales)*100, 2) as growth_rate_percent
from (
    select s.storeID, t.quarter, sum(sf.total_sales_amount) as total_sales,
        lag(sum(sf.total_sales_amount)) over (
            partition by s.storeID
            order by t.quarter
        ) as prev_quarter_sales
    from sales_fact sf
    join store_dim s on sf.store_ID = s.storeID
    join time_dim t on sf.date_ID = t.DateID
    where t.year = 2017
    group by s.storeID, t.quarter
) quarterly_sales
order by storeID, quarter;

-- Q13. Detailed Supplier Sales Contribution by Store and Product Name
select s.storeID, sp.supplier_name, p.product_category, sum(sf.total_sales_amount) as total_sales
from sales_fact sf
join store_dim s on sf.store_ID = s.storeID
join supplier_dim sp on sf.supplier_ID = sp.supplierID
join product_dim p on sf.product_ID = p.product_ID
group by s.storeID, sp.supplier_name, p.product_category
order by s.storeID, sp.supplier_name, total_sales desc;

-- Q14. Seasonal Analysis of Product Sales Using Dynamic Drill-Down
select p.product_ID, p.product_category, t.season, sum(s.total_sales_amount) as total_sales
from sales_fact s
join product_dim p on s.product_ID = p.product_ID
join time_dim t on s.date_ID = t.DateID
group by p.product_ID, p.product_category, t.season
order by p.product_ID, t.season;

-- Q15. Store-Wise and Supplier-Wise Monthly Revenue Volatility
select storeID, supplier_name, month, total_sales,
       round(((total_sales - prev_month_sales)/prev_month_sales)*100, 2) as revenue_volatility_percent
from (
    select s.storeID, sp.supplier_name, t.month, sum(sf.total_sales_amount) as total_sales,
        lag(sum(sf.total_sales_amount)) over (
            partition by s.storeID, sp.supplier_name
            order by t.month
        ) as prev_month_sales
    from sales_fact sf
    join store_dim s on sf.store_ID = s.storeID
    join supplier_dim sp on sf.supplier_ID = sp.supplierID
    join time_dim t on sf.date_ID = t.DateID
    where t.year = 2020
    group by s.storeID, sp.supplier_name, t.month
) monthly_sales
order by storeID, supplier_name, month;

-- Q16 Optimized: Top 5 Products Purchased Together (Product Affinity Analysis)

-- Q17. Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
select s.storeID, sp.supplier_name, p.product_ID, p.product_category, sum(sf.total_sales_amount) as total_revenue
from sales_fact sf
join store_dim s on sf.store_ID = s.storeID
join supplier_dim sp on sf.supplier_ID = sp.supplierID
join product_dim p on sf.product_ID = p.product_ID
join time_dim t on sf.date_ID = t.DateID
where t.year = 2020
group by s.storeID, sp.supplier_name, p.product_ID, p.product_category with rollup
order by s.storeID, sp.supplier_name, p.product_ID;

-- Q18. Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2
select p.product_ID, p.product_category,
       sum(case when t.Half_Year = 'H1' then sf.total_sales_amount else 0 end) as revenue_H1,
       sum(case when t.Half_Year = 'H2' then sf.total_sales_amount else 0 end) as revenue_H2,
       sum(sf.total_sales_amount) as revenue_year,
       sum(case when t.Half_Year = 'H1' then sf.quantity else 0 end) as quantity_H1,
       sum(case when t.Half_Year = 'H2' then sf.quantity else 0 end) as quantity_H2,
       sum(sf.quantity) as quantity_year
from sales_fact sf
join product_dim p on sf.product_ID = p.product_ID
join time_dim t on sf.date_ID = t.DateID
where t.year = 2020
group by p.product_ID, p.product_category
order by p.product_ID;

-- Q19. Identify High Revenue Spikes in Product Sales and Highlight Outliers
select p.product_ID, p.product_category, t.Full_Date, sum(sf.total_sales_amount) as daily_sales,
       avg(sum(sf.total_sales_amount)) over (partition by p.product_ID) as daily_avg,
       case when sum(sf.total_sales_amount) > 2 * avg(sum(sf.total_sales_amount)) over (partition by p.product_ID) then 'Spike'else 'Normal' end as spike_flag
from sales_fact sf
join product_dim p on sf.product_ID = p.product_ID
join time_dim t on sf.date_ID = t.DateID
where t.year = 2020
group by p.product_ID, p.product_category, t.Full_Date
order by p.product_ID, t.Full_Date;

-- Q20. Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis
create view STORE_QUARTERLY_SALES as
select s.storeID, s.store_name, t.quarter, sum(sf.total_sales_amount) as total_sales
from sales_fact sf
join store_dim s on sf.store_ID = s.storeID
join time_dim t on sf.date_ID = t.DateID
group by s.storeID, s.store_name, t.quarter
order by s.store_name, t.quarter;

select * from STORE_QUARTERLY_SALES;
