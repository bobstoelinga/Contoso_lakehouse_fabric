SELECT currency_code, rate_date, rate_to_eur, source_series, version_valid_from AS last_changed_at FROM {gold}.historical.dim_ecb_exchange_rate_hist WHERE is_current
