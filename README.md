# Assignment #2: Employee and Department Data Analysis with Hive

## Overview
This assignment involves analyzing employees and department data using Hive. The process includes setting up Hive tables, partitioning data for optimized queries, executing analytical queries, and exporting the results.

## Implementation Approach
1. **Setup Hive Environment** - Create Hive tables for employees and departments.
2. **Load Data** - Import CSV files into Hive tables.
3. **Implement Partitioning** - Optimize data storage using partitioned tables.
4. **Execute Queries** - Perform various analytical queries on the data.
5. **Export Results** - Save query outputs for further analysis.

## Setup and Execution

### 1. **Start the Hadoop Cluster and Hive Server**
```bash
docker compose up -d
```

### 2. **Access the Hive Server Container**
```bash
docker exec -it hive-server /bin/bash
```
```bash
hive
```

### 3. **Create Hive Tables**
#### Create an employee table to store raw data:
```sql
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
```

#### Create a department table:
```sql
CREATE TABLE departments_temp (
    dept_id INT,
    department_name STRING,
    location STRING
) ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE;
```

### 4. **Load Data into Hive Tables**
Upload the CSV files to HDFS:
```bash
hdfs dfs -put employees.csv /user/hive/
hdfs dfs -put departments.csv /user/hive/
```
Load data into Hive:
```sql
LOAD DATA INPATH 'hdfs:///user/hive/employees.csv' INTO TABLE emp_temp;
LOAD DATA INPATH 'hdfs:///user/hive/departments.csv' INTO TABLE departments_temp;
```

### 5. **Implement Partitioning for Performance Optimization**
Enable dynamic partitioning:
```sql
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = non-strict;
```

Create a partitioned employee table:
```sql
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
```

Add partitions:
```sql
ALTER TABLE employees_partitioned 
ADD PARTITION (department='HR')
PARTITION (department='Engineering')
PARTITION (department='Marketing')
PARTITION (department='Finance')
PARTITION (department='Sales');
```

Insert data into the partitioned table:
```sql
INSERT OVERWRITE TABLE employees_partitioned PARTITION (department)
SELECT emp_id, name, age, job_role, salary, project, join_date, department 
FROM emp_temp;
```

### 6. **Run Hive Queries for Analysis**
#### 1) Retrieve all employees who joined after 2015
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/after_2015'
SELECT * 
FROM employees_partitioned 
WHERE year(join_date) > 2015;
```
- Returns all the fields and employees who started working from 2015

#### 2) Find the average salary of employees in each department
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/avg_salary'
SELECT department, AVG(salary) AS avg_salary 
FROM employees_partitioned 
GROUP BY department;
```
- Calculating average salary of employees by departments

#### 3) Identify employees working on the 'Alpha' project
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/alpha_project'
SELECT * 
FROM employees_partitioned 
WHERE project = 'Alpha';
```
- Extracts all employees working on project 'Alpha'.

#### 4) Count the number of employees in each job role
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/job_role_count'
SELECT job_role, COUNT(*) AS count 
FROM employees_partitioned 
GROUP BY job_role;
```
- Groups employees by job role and counts how many employees belong to each role.

#### 5) Retrieve employees whose salary is above the average salary of their department
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/salary_above_avg'
SELECT e.* 
FROM employees_partitioned e 
JOIN (SELECT department, AVG(salary) AS avg_salary FROM employees_partitioned GROUP BY department) dept_avg 
ON e.department = dept_avg.department 
WHERE e.salary > dept_avg.avg_salary;
```
- First, calculates average salary per department and then, selects employees earning above the department average salary.

#### 6) Find the department with the highest number of employees
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/highest_employee_department'
SELECT department, COUNT(*) AS num_employees 
FROM employees_partitioned 
GROUP BY department 
ORDER BY num_employees DESC 
LIMIT 1;
```
- Counts the number of employees in each department and sorting them in descending order and retrieves the top department with the highest count.

#### 7) Check for employees with null values in any column and exclude them from analysis
```sql
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
```
- Filters out any employees where any column contains NULL ensures only complete records are included in the analysis.

#### 8) Join the employees and departments tables to display employee details along with department locations
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/employee_department_location'
SELECT e.*, d.location 
FROM employees_partitioned e 
JOIN departments_temp d 
ON e.department = d.department_name;
```
- Joins employees_partitioned with departments_temp to get department locations.

#### 9) Rank employees within each department based on salary
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/salary_rank'
SELECT emp_id, name, department, salary, 
       RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank 
FROM employees_partitioned;
```
- Uses window function RANK() to assign a salary rank within each department and orders employees by highest salary first.

#### 10) Find the top 3 highest-paid employees in each department
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/top3_salary'
SELECT emp_id, name, department, salary 
FROM (SELECT emp_id, name, department, salary, 
             RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank 
      FROM employees_partitioned) ranked 
WHERE rank <= 3;
```
- Uses a subquery to assign a rank based on salary within each department and filters to return only the top 3 highest-paid employees per department.

### 7. **Copy Output from HDFS to Local Machine**
To copy the output folder from HDFS to the local file system inside the Hive container:
```bash
hdfs dfs -get /user/hive/output /tmp/output
```
Exit the container:
```bash
exit
```
Now, check the Codespaces workspace path:
```bash
pwd
```
Copy the output files from the container to the Codespace workspace:
```bash
docker cp hive-server:/tmp/output /workspaces/hive-employee-data-analysis-Scheruk1701/
```

### 8. **Commit Changes to GitHub**
```bash
git add .
git commit -m "hql queries and outputs"
git push origin master
```

## Challenges Faced & Solutions

### 1. **Path Errors in Hive**
- **Issue:** Error while loading CSV due to incorrect path format (`java.net.URISyntaxException`).
- **Solution:** Used `hdfs:///` for HDFS paths to avoid issues.

### 2. **Dynamic Partitioning Strict Mode Error**
- **Issue:** `FAILED: SemanticException [Error 10096]: Dynamic partition strict mode requires at least one static partition column.`
- **Solution:** Disabled strict mode using:
```sql
SET hive.exec.dynamic.partition.mode = nonstrict;
```

### 3. **File Exists Error When Copying Output**
- **Issue:** `get: File exists` error while copying files from HDFS.
- **Solution:** Removed the existing output directory before copying:
```bash
rm -rf /tmp/output
hdfs dfs -get /user/hive/output /tmp/output
```

### 4. **Output Not Visible in Codespaces**
- **Issue:** Could see output in HDFS but not in Codespaces.
- **Solution:** Used the following commands to bring results into Codespaces:
```bash
hdfs dfs -get /user/hive/output /opt/hive-output/
docker cp hive-server:/opt/hive-output/ .
```

## Conclusion
This assignment successfully analyzes employees and department data using Hive, implementing partitioning for performance improvement and exporting results for further analysis. The provided commands allow easy execution and reproduction of the workflow.
