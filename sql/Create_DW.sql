create database if not exists project;
use project;

drop table if exists sales_fact;
drop table if exists time_dim;
drop table if exists customer_dim;
drop table if exists product_dim;
drop table if exists store_dim;
drop table if exists supplier_dim;

-- customer dimension
create table customer_dim (
    customer_ID int primary key,
    gender varchar(10),
    age varchar(20),
    occupation varchar(50),
    city_category varchar(5),
    stay_in_current_city_years int,
    marital_status int
);

-- product dimension
create table product_dim (
    product_ID varchar(20) primary key,
    product_category varchar(50)
);

-- store dimension
create table store_dim (
    storeID int primary key,
    store_name varchar(50)
);

create table supplier_dim (
    supplierID int primary key,
    supplier_name varchar(50)
);

-- time dimension
create table time_dim (
    DateID int primary key,
    Full_Date date not null unique,
    Day int,
    Is_Weekday boolean,
    Month int,
    Month_Name varchar(20),
    Quarter varchar(10),
    Year int,
    Season varchar(20),
    Half_Year varchar(2)
);

-- sales fact
create table sales_fact (
    sales_ID int primary key auto_increment,
    date_ID int not null,
    customer_ID int not null,
    product_ID varchar(20) not null,
    store_ID int not null,
    supplier_ID int not null,
    order_id int,
    quantity int,
    unit_price decimal(10, 2),
    total_sales_amount decimal(10, 2),    
    foreign key (date_ID) references time_dim(DateID),
    foreign key (customer_ID) references customer_dim(customer_ID),
    foreign key (product_ID) references product_dim(product_ID),
    foreign key (store_ID) references store_dim(storeID),
    foreign key (supplier_ID) references supplier_dim(supplierID)
);
