DROP DATABASE IF EXISTS employees;

CREATE DATABASE employees;

USE employees;

-- Create employees table
CREATE TABLE employees (
    emp_no INT PRIMARY KEY AUTO_INCREMENT,
    birth_date DATE,
    first_name VARCHAR(14),
    last_name VARCHAR(16),
    gender ENUM('M', 'F'),
    hire_date DATE,
    manager_emp_no INT,
    FOREIGN KEY (manager_emp_no) REFERENCES employees(emp_no)
);

-- Create departments table
CREATE TABLE departments (
    dept_no INT PRIMARY KEY AUTO_INCREMENT,
    dept_name VARCHAR(40),
    manager_emp_no INT,
    FOREIGN KEY (manager_emp_no) REFERENCES employees(emp_no),
    INDEX (dept_name)
);

-- Create projects table
CREATE TABLE projects (
    project_no INT PRIMARY KEY AUTO_INCREMENT,
    project_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    dept_no INT,
    FOREIGN KEY (dept_no) REFERENCES departments(dept_no)
);

-- Create dept_emp table
CREATE TABLE dept_emp (
    emp_no INT,
    dept_no INT,
    from_date DATE,
    to_date DATE,
    PRIMARY KEY (emp_no, dept_no),
    FOREIGN KEY (emp_no) REFERENCES employees(emp_no),
    FOREIGN KEY (dept_no) REFERENCES departments(dept_no)
);

-- Create salaries table
CREATE TABLE salaries (
    emp_no INT,
    salary INT,
    from_date DATE,
    to_date DATE,
    PRIMARY KEY (emp_no, from_date),
    FOREIGN KEY (emp_no) REFERENCES employees(emp_no)
);

-- Create titles table
CREATE TABLE titles (
    emp_no INT,
    title VARCHAR(50),
    from_date DATE,
    to_date DATE,
    PRIMARY KEY (emp_no, title, from_date),
    FOREIGN KEY (emp_no) REFERENCES employees(emp_no)
);

-- Create project_assignments table
CREATE TABLE project_assignments (
    emp_no INT,
    dept_no INT,
    project_no INT,
    assigned_date DATE,
    PRIMARY KEY (emp_no, dept_no, project_no),
    FOREIGN KEY (emp_no) REFERENCES employees(emp_no),
    FOREIGN KEY (dept_no) REFERENCES departments(dept_no),
    FOREIGN KEY (project_no) REFERENCES projects(project_no)
);

-- Insert data into employees table
INSERT INTO employees (emp_no, birth_date, first_name, last_name, gender, hire_date, manager_emp_no) VALUES
(1, '1980-01-01', 'John', 'Doe', 'M', '2000-01-01', NULL),
(2, '1985-02-02', 'Jane', 'Doe', 'F', '2005-02-02', 1),
(3, '1990-03-03', 'Jim', 'Beam', 'M', '2010-03-03', 2),
(4, '1982-04-04', 'Jill', 'Valentine', 'F', '2001-04-04', 1),
(5, '1983-05-05', 'Leon', 'Kennedy', 'M', '2002-05-05', 1),
(6, '1984-06-06', 'Claire', 'Redfield', 'F', '2003-06-06', 2),
(7, '1981-07-07', 'Chris', 'Redfield', 'M', '2004-07-07', 3),
(8, '1986-08-08', 'Albert', 'Wesker', 'M', '2006-08-08', 3),
(9, '1987-09-09', 'Ada', 'Wong', 'F', '2007-09-09', 4),
(10, '1988-10-10', 'Hunk', 'Unknown', 'M', '2008-10-10', 5),
(11, '1989-11-11', 'Rebecca', 'Chambers', 'F', '2009-11-11', 6),
(12, '1995-12-12', 'Carlos', 'Oliveira', 'M', '2015-12-12', 7),
(13, '1993-01-13', 'Ashley', 'Graham', 'F', '2013-01-13', 8),
(14, '1987-02-14', 'Piers', 'Nivans', 'M', '2007-02-14', 9),
(15, '1991-03-15', 'Sherry', 'Birkin', 'F', '2011-03-15', 10),
(16, '1988-04-16', 'Carlos', 'Mendez', 'M', '2008-04-16', 6),
(17, '1984-05-17', 'Rebecca', 'Wong', 'F', '2004-05-17', 7),
(18, '1982-06-18', 'Sheva', 'Alomar', 'F', '2002-06-18', 8),
(19, '1990-07-19', 'Billy', 'Coen', 'M', '2010-07-19', 9),
(20, '1986-08-20', 'Josh', 'Stone', 'M', '2006-08-20', 10);

-- Insert data into departments table
INSERT INTO departments (dept_no, dept_name, manager_emp_no) VALUES
(1, 'Sales', 1),
(2, 'Engineering', 2),
(3, 'Finance', 3),
(4, 'Marketing', 4),
(5, 'Human Resources', 5),
(6, 'Research and Development', 11),
(7, 'Customer Service', 12),
(8, 'Quality Assurance', 13);

-- Insert data into projects table
INSERT INTO projects (project_no, project_name, start_date, end_date, dept_no) VALUES
(1, 'Project A', '2021-01-01', '2021-12-31', 1),
(2, 'Project B', '2021-06-01', '2022-05-31', 2),
(3, 'Project C', '2021-03-01', '2021-09-30', 3),
(4, 'Project D', '2021-05-01', '2021-11-30', 4),
(5, 'Project E', '2022-01-01', '2022-12-31', 5),
(6, 'Project F', '2022-06-01', '2023-05-31', 6),
(7, 'Project G', '2022-03-01', '2022-09-30', 7),
(8, 'Project H', '2022-05-01', '2022-11-30', 8),
(9, 'Project I', '2022-07-01', '2023-06-30', 1),
(10, 'Project J', '2022-08-01', '2023-07-31', 2),
(11, 'Project K', '2023-01-01', '2023-12-31', 1),
(12, 'Project L', '2023-06-01', '2024-05-31', 2),
(13, 'Project M', '2023-03-01', '2023-09-30', 3),
(14, 'Project N', '2023-05-01', '2023-11-30', 4),
(15, 'Project O', '2024-01-01', '2024-12-31', 5);

-- Insert data into dept_emp table
INSERT INTO dept_emp (emp_no, dept_no, from_date, to_date) VALUES
(1, 1, '2000-01-01', '2021-01-01'),
(2, 1, '2005-02-02', '2021-01-01'),
(3, 2, '2010-03-03', '2022-01-01'),
(4, 3, '2001-04-04', '2022-01-01'),
(5, 4, '2002-05-05', '2022-01-01'),
(6, 1, '2003-06-06', '2022-01-01'),
(7, 2, '2004-07-07', '2022-01-01'),
(8, 3, '2006-08-08', '2022-01-01'),
(9, 4, '2007-09-09', '2022-01-01'),
(10, 1, '2008-10-10', '2022-01-01'),
(11, 6, '2009-11-11', '2022-01-01'),
(12, 7, '2015-12-12', '2022-01-01'),
(13, 8, '2013-01-13', '2022-01-01'),
(14, 5, '2007-02-14', '2022-01-01'),
(15, 6, '2011-03-15', '2022-01-01'),
(16, 7, '2008-04-16', '2022-01-01'),
(17, 8, '2004-05-17', '2022-01-01'),
(18, 5, '2002-06-18', '2022-01-01'),
(19, 6, '2010-07-19', '2022-01-01'),
(20, 7, '2006-08-20', '2022-01-01');

-- Insert data into salaries table
INSERT INTO salaries (emp_no, salary, from_date, to_date) VALUES
(1, 50000, '2000-01-01', '2021-01-01'),
(2, 60000, '2005-02-02', '2021-01-01'),
(3, 70000, '2010-03-03', '2022-01-01'),
(4, 55000, '2001-04-04', '2022-01-01'),
(5, 56000, '2002-05-05', '2022-01-01'),
(6, 57000, '2003-06-06', '2022-01-01'),
(7, 58000, '2004-07-07', '2022-01-01'),
(8, 59000, '2006-08-08', '2022-01-01'),
(9, 60000, '2007-09-09', '2022-01-01'),
(10, 61000, '2008-10-10', '2022-01-01'),
(11, 62000, '2009-11-11', '2022-01-01'),
(12, 63000, '2015-12-12', '2022-01-01'),
(13, 64000, '2013-01-13', '2022-01-01'),
(14, 65000, '2007-02-14', '2022-01-01'),
(15, 66000, '2011-03-15', '2022-01-01'),
(16, 67000, '2008-04-16', '2022-01-01'),
(17, 68000, '2004-05-17', '2022-01-01'),
(18, 69000, '2002-06-18', '2022-01-01'),
(19, 70000, '2010-07-19', '2022-01-01'),
(20, 71000, '2006-08-20', '2022-01-01');

-- Insert data into titles table
INSERT INTO titles (emp_no, title, from_date, to_date) VALUES
(1, 'Manager', '2000-01-01', '2021-01-01'),
(2, 'Senior Developer', '2005-02-02', '2021-01-01'),
(3, 'Developer', '2010-03-03', '2022-01-01'),
(4, 'Finance Manager', '2001-04-04', '2022-01-01'),
(5, 'Marketing Specialist', '2002-05-05', '2022-01-01'),
(6, 'Sales Representative', '2003-06-06', '2022-01-01'),
(7, 'Engineering Lead', '2004-07-07', '2022-01-01'),
(8, 'Financial Analyst', '2006-08-08', '2022-01-01'),
(9, 'Marketing Analyst', '2007-09-09', '2022-01-01'),
(10, 'Product Manager', '2008-10-10', '2022-01-01'),
(11, 'Researcher', '2009-11-11', '2022-01-01'),
(12, 'Customer Support Specialist', '2015-12-12', '2022-01-01'),
(13, 'QA Analyst', '2013-01-13', '2022-01-01'),
(14, 'HR Manager', '2007-02-14', '2022-01-01'),
(15, 'Research Assistant', '2011-03-15', '2022-01-01'),
(16, 'Customer Service Lead', '2008-04-16', '2022-01-01'),
(17, 'Quality Engineer', '2004-05-17', '2022-01-01'),
(18, 'HR Specialist', '2002-06-18', '2022-01-01'),
(19, 'R&D Manager', '2010-07-19', '2022-01-01'),
(20, 'Customer Service Supervisor', '2006-08-20', '2022-01-01');

-- Insert data into project_assignments table
INSERT INTO project_assignments (emp_no, dept_no, project_no, assigned_date) VALUES
(1, 1, 1, '2021-01-01'),
(2, 1, 1, '2021-01-01'),
(3, 2, 2, '2021-06-01'),
(4, 3, 3, '2021-03-01'),
(5, 4, 4, '2021-05-01'),
(6, 1, 1, '2021-01-01'),
(7, 2, 2, '2021-06-01'),
(8, 3, 3, '2021-03-01'),
(9, 4, 4, '2021-05-01'),
(10, 1, 1, '2021-01-01'),
(11, 6, 6, '2009-11-11'),
(12, 7, 7, '2015-12-12'),
(13, 8, 8, '2013-01-13'),
(14, 5, 5, '2007-02-14'),
(15, 6, 6, '2011-03-15'),
(16, 7, 7, '2008-04-16'),
(17, 8, 8, '2004-05-17'),
(18, 5, 5, '2002-06-18'),
(19, 6, 6, '2010-07-19'),
(20, 7, 7, '2006-08-20'),
(1, 2, 2, '2022-01-01'),
(2, 3, 3, '2022-01-01'),
(3, 4, 4, '2022-01-01'),
(4, 5, 5, '2022-01-01'),
(5, 6, 6, '2022-01-01'),
(6, 7, 7, '2022-01-01'),
(7, 8, 8, '2022-01-01'),
(8, 1, 9, '2022-01-01'),
(9, 2, 10, '2022-01-01'),
(10, 3, 1, '2022-01-01'),
(11, 4, 2, '2022-01-01'),
(12, 5, 3, '2022-01-01'),
(13, 6, 4, '2022-01-01'),
(14, 7, 5, '2022-01-01'),
(15, 8, 6, '2022-01-01'),
(16, 1, 7, '2022-01-01'),
(17, 2, 8, '2022-01-01'),
(18, 3, 9, '2022-01-01'),
(19, 4, 10, '2022-01-01'),
(20, 5, 1, '2022-01-01'),
(1, 1, 11, '2023-01-01'),
(2, 1, 11, '2023-01-01'),
(3, 2, 12, '2023-06-01'),
(4, 3, 13, '2023-03-01'),
(5, 4, 14, '2023-05-01'),
(6, 1, 11, '2023-01-01'),
(7, 2, 12, '2023-06-01'),
(8, 3, 13, '2023-03-01'),
(9, 4, 14, '2023-05-01'),
(10, 1, 11, '2023-01-01'),
(11, 6, 15, '2024-01-01'),
(12, 7, 15, '2024-01-01'),
(13, 8, 15, '2024-01-01'),
(14, 5, 15, '2024-01-01'),
(15, 6, 15, '2024-01-01'),
(16, 7, 15, '2024-01-01'),
(17, 8, 15, '2024-01-01'),
(18, 5, 15, '2024-01-01'),
(19, 6, 15, '2024-01-01'),
(20, 7, 15, '2024-01-01');
