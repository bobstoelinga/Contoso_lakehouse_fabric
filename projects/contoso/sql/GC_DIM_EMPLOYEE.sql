SELECT employee_hk, employee_key, first_name, last_name, job_title, office_city, hire_date, valid_from AS last_changed_at FROM {gold}.historical.dim_employee_hist WHERE is_current AND NOT is_deleted
