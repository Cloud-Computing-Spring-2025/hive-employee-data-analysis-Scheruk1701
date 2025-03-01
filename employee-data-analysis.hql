CREATE TABLE emp_temp (
    emp_id INT,
    name STRING,
    age INT,
    job_role STRING,
    salary INT,
    project STRING,
    join_date STRING,
    department STRING
) ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE;

LOAD DATA INPATH 'hdfs:///user/hive/employees.csv' INTO TABLE emp_temp;

CREATE TABLE departments_temp (
    dept_id INT,
    department_name STRING,
    location STRING
) ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE;

LOAD DATA INPATH 'hdfs:///user/hive/departments.csv' INTO TABLE departments_temp;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = non-strict;

CREATE TABLE employees_partitioned (
    emp_id INT,
    name STRING,
    age INT,
    job_role STRING,
    salary INT,
    project STRING,
    join_date STRING
) 
PARTITIONED BY (department STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS PARQUET;

-- Add multiple partitions in a single command
ALTER TABLE employees_partitioned 
ADD PARTITION (department='HR')
PARTITION (department='Engineering')
PARTITION (department='Marketing')
PARTITION (department='Finance')
PARTITION (department='Sales');

INSERT OVERWRITE TABLE employees_partitioned PARTITION (department)
SELECT emp_id, name, age, job_role, salary, project, join_date, department 
FROM emp_temp;

-- Retrieve all employees who joined after 2015
INSERT OVERWRITE DIRECTORY '/user/hive/output/after_2015'
SELECT * 
FROM employees_partitioned 
WHERE year(join_date) > 2015;

-- Find the average salary of employees in each department
INSERT OVERWRITE DIRECTORY '/user/hive/output/avg_salary'
SELECT department, AVG(salary) AS avg_salary 
FROM employees_partitioned 
GROUP BY department;

-- Identify employees working on the 'Alpha' project
INSERT OVERWRITE DIRECTORY '/user/hive/output/alpha_project'
SELECT * 
FROM employees_partitioned 
WHERE project = 'Alpha';

-- Count the number of employees in each job role
INSERT OVERWRITE DIRECTORY '/user/hive/output/job_role_count'
SELECT job_role, COUNT(*) AS count 
FROM employees_partitioned 
GROUP BY job_role;

-- Retrieve employees whose salary is above the average salary of their department
INSERT OVERWRITE DIRECTORY '/user/hive/output/salary_above_avg'
SELECT e.* 
FROM employees_partitioned e 
JOIN (SELECT department, AVG(salary) AS avg_salary FROM employees_partitioned GROUP BY department) dept_avg 
ON e.department = dept_avg.department 
WHERE e.salary > dept_avg.avg_salary;

-- Find the department with the highest number of employees
INSERT OVERWRITE DIRECTORY '/user/hive/output/highest_employee_department'
SELECT department, COUNT(*) AS num_employees 
FROM employees_partitioned 
GROUP BY department 
ORDER BY num_employees DESC 
LIMIT 1;

-- Check for employees with null values in any column and exclude them from analysis
INSERT OVERWRITE DIRECTORY '/user/hive/output/non_null_employees'
SELECT * 
FROM employees_partitioned 
WHERE emp_id IS NOT NULL 
AND name IS NOT NULL 
AND age IS NOT NULL 
AND job_role IS NOT NULL 
AND salary IS NOT NULL 
AND project IS NOT NULL 
AND join_date IS NOT NULL 
AND department IS NOT NULL;

-- Join the employees and departments tables to display employee details along with department locations
INSERT OVERWRITE DIRECTORY '/user/hive/output/employee_department_location'
SELECT e.*, d.location 
FROM employees_partitioned e 
JOIN departments_temp d 
ON e.department = d.department_name;

-- Rank employees within each department based on salary
INSERT OVERWRITE DIRECTORY '/user/hive/output/salary_rank'
SELECT emp_id, name, department, salary, 
       RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank 
FROM employees_partitioned;

-- Find the top 3 highest-paid employees in each department
INSERT OVERWRITE DIRECTORY '/user/hive/output/top3_salary'
SELECT emp_id, name, department, salary 
FROM (SELECT emp_id, name, department, salary, 
             RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank 
      FROM employees_partitioned) ranked 
WHERE rank <= 3;