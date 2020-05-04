/**
* You can copy, modify, distribute and perform the work, even for commercial purposes, 
* all without asking permission. 
* 
* @Author: Andrei N. CIOBANU
*/

DROP SCHEMA

IF EXISTS hr;
	CREATE SCHEMA hr COLLATE = utf8_general_ci;

USE hr;

/* *************************************************************** 
***************************CREATING TABLES************************
**************************************************************** */
CREATE TABLE regions (
	id int auto_increment,
	region_id INT (11) UNSIGNED NOT NULL,
	region_name VARCHAR(25),
	created_at DATETIME NOT NULL,
	PRIMARY KEY (id)
	);

CREATE TABLE countries (
	id int auto_increment,
	country_id CHAR(2) NOT NULL,
	country_name VARCHAR(40),
	region_id INT (11) UNSIGNED NOT NULL,
	created_at DATETIME NOT NULL,
	PRIMARY KEY (id)
);


CREATE TABLE locations (
	location_id INT (11) UNSIGNED NOT NULL AUTO_INCREMENT,
	street_address VARCHAR(40),
	postal_code VARCHAR(12),
	city VARCHAR(30) NOT NULL,
	state_province VARCHAR(25),
	country_id CHAR(2) NOT NULL,
	created_at DATETIME NOT NULL,
	PRIMARY KEY (location_id)
	);

CREATE TABLE departments (
	department_id INT (11) UNSIGNED NOT NULL AUTO_INCREMENT,
	department_name VARCHAR(30) NOT NULL,
	manager_id INT (11) UNSIGNED,
	location_id INT (11) UNSIGNED,
	created_at DATETIME NOT NULL,
	PRIMARY KEY (department_id)
	);

CREATE TABLE jobs (
	job_id int auto_increment,
	job_title VARCHAR(35) NOT NULL,
	min_salary DECIMAL(8, 0) UNSIGNED,
	max_salary DECIMAL(8, 0) UNSIGNED,
	created_at DATETIME NOT NULL,
	PRIMARY KEY (job_id)
	);

CREATE TABLE employees (
	employee_id INT (11) UNSIGNED NOT NULL auto_increment,
	first_name VARCHAR(20),
	last_name VARCHAR(25) NOT NULL,
	email VARCHAR(25) NOT NULL,
	phone_number VARCHAR(20),
	hire_date DATE NOT NULL,
	job_id VARCHAR(10) NOT NULL,
	salary DECIMAL(8, 2) NOT NULL,
	commission_pct DECIMAL(2, 2),
	manager_id INT (11) UNSIGNED,
	department_id INT (11) UNSIGNED,
	created_at DATETIME NOT NULL,
	PRIMARY KEY (employee_id)
	);

CREATE TABLE job_history (
	id int auto_increment,
	employee_id INT (11) UNSIGNED NOT NULL,
	start_date DATE NOT NULL,
	end_date DATE NOT NULL,
	job_id VARCHAR(10) NOT NULL,
	department_id INT (11) UNSIGNED NOT NULL,
	created_at DATETIME NOT NULL,
	primary key (id)
	);

COMMIT;