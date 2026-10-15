SELECT holiday_date, holiday_name, local_name, country_code, global_holiday, holiday_types, version_valid_from AS last_changed_at FROM {gold}.historical.dim_nager_holiday_hist WHERE is_current
